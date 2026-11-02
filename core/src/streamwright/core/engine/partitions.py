"""Stream partition and incremental-window planning."""

import collections
import datetime
import itertools

from streamwright.core.config.inputs import as_date, parse_duration
from streamwright.core.engine import rules, sql
from streamwright.core.engine.errors import SourceError
from streamwright.core.engine.responses import lookup, same_value
from streamwright.core.engine.values import Distinct, partition_key, partition_value
from streamwright.core.runtime.templates import render, to_text


def _days(text, what, stream, minimum):
    duration = parse_duration(text)
    if duration.total_seconds() % 86400 or duration.days < minimum:
        raise SourceError("stream %r: `%s: %s` - use whole days (at least %dd)" % (stream, what, text, minimum))
    return duration.days


def _values(values):
    """A partition item's rendered `values` as a list (text is comma-separated values)."""
    if isinstance(values, str):
        return [value.strip() for value in values.split(",") if value.strip()]
    if not isinstance(values, list):
        return [] if values is None else [values]
    return values


def _batches(values, size):
    """
    `batch_size` partitions: the distinct values (a missing one gives no partition), in the order they were first
    seen, as lists of up to `size` values (the last one smaller); no values give none.
    """
    distinct = Distinct()
    for value in values:
        if value is not None:
            distinct.add(value)
    return [distinct.values[start:start + size] for start in range(0, len(distinct.values), size)]


def _partition_names(stream):
    """The names in a stream's partitions (each of them has all of these)."""
    return [name for item in stream.get("partitions") or [] for name in sql.partition_fields(item)[1]]


class PartitionPlanner:
    def __init__(self, runner):
        self.runner = runner

    def parent(self, item):
        return self.runner.stream_names.get(item["from_stream"].lower(), item["from_stream"])

    def stream_partitions(self, stream):
        items = stream.get("partitions") or []
        if not items:
            return [{}]
        choices = []
        for item in items:
            if "from_stream" in item:
                fields, names = sql.partition_fields(item)
                parent = self.parent(item)
                rows = (self.runner.collected.get(parent) or {}).get(fields)
                if rows is None:
                    raise SourceError("stream %r: its `from_stream` parent %r has not run" % (stream["name"], parent))
                choices.append([tuple(zip(names, row, strict=True)) for row in rows.values])
            else:
                values = _values(render(item["values"], self.runner._scopes()))
                choices.append([((item["name"], value),) for value in values])
        return [dict(pair for pairs in combination for pair in pairs)
                for combination in itertools.product(*choices)]

    def windows(self, stream, partition):
        incremental = stream.get("incremental")
        if not incremental:
            return [None]
        name = stream["name"]
        start = as_date(render(incremental["start"], self.runner._scopes()), self.runner.today)
        size = _days(incremental["window"], "window", name, 1) if incremental.get("window") else None
        lookback = _days(incremental["lookback"], "lookback", name, 0) if incremental.get("lookback") else 0
        cursor = self.runner.bookmarks.get(name, partition)
        begin = start
        if cursor:
            resume = as_date(cursor) + datetime.timedelta(days=1)
            begin = max(min(start, resume), resume - datetime.timedelta(days=lookback))
        windows = []
        while begin <= self.runner.today:
            end = self.runner.today if size is None else min(begin + datetime.timedelta(days=size - 1),
                                                               self.runner.today)
            windows.append((begin, end))
            begin = end + datetime.timedelta(days=1)
        return windows

    def check_batch_sizes(self, stream, request, items, stream_names, request_names):
        for item in items:
            if "batch_size" not in item:
                continue
            size = item.get("batch_size")
            what = "stream %r: request %r: `batch_size`" % (stream["name"], request["name"])
            if not rules.valid_batch_size(size):
                raise SourceError("%s must be a whole number of at least 1, got %r" % (what, size))
            if "from" not in item:
                continue
            fields, names = sql.partition_fields(item)
            own = [field for field, name in zip(fields, names, strict=True) if name not in stream_names]
            if "fields" in item and len(own) != 1:
                raise SourceError("%s on `{from, fields}` needs exactly one field that is not a stream partition (the "
                                  "one batched), got %s" % (what, ", ".join(map(str, names))))
            missing = [name for name in stream_names if name not in names]
            if own and missing and str(item["from"]).lower() not in request_names:
                raise SourceError("%s on step %r would list other stream partitions' values: use `{from: %s, fields: "
                                  "[%s], batch_size: %d}`" % (what, item["from"], item["from"],
                                                              ", ".join(map(str, stream_names + own)), size))

    def request_partitions(self, stream, request):
        """Return a resolver of request partitions for each stream partition."""
        items = request.get("partitions") or []
        if not items:
            return lambda partition: [partition]
        stream_names = _partition_names(stream)
        request_names = {item["name"].lower(): item["name"] for item in stream.get("requests") or []}
        step_names = {step["name"].lower(): step["name"]
                      for step in (stream.get("transform") or {}).get("steps") or []}
        self.check_batch_sizes(stream, request, items, stream_names, request_names)
        sandbox = self.runner._workspace()
        sources = [_request_source(item, stream_names, request_names, step_names, sandbox) for item in items]

        def resolve(partition):
            choices = [_request_choices(item, source, partition, self.runner._scopes)
                       for item, source in zip(items, sources, strict=True)]
            return [dict(partition, **dict(pair for pairs in combination for pair in pairs))
                    for combination in itertools.product(*choices)]
        return resolve


def _request_source(item, stream_names, request_names, step_names, sandbox):
    if "values" in item:
        return "values", item
    fields, names = sql.partition_fields(item)
    shared = [position for position, name in enumerate(names) if name in stream_names]
    own = [position for position, name in enumerate(names) if name not in stream_names]
    index = collections.defaultdict(Distinct)
    source = item["from"].lower()
    if source in request_names:
        _index_raw_rows(sandbox.raw_rows(request_names[source]), fields, names, shared=shared, own=own,
                        stream_names=stream_names, index=index)

        def key(partition):
            return partition_key(sql.as_json(partition))
    else:
        for row in sandbox.distinct_rows(step_names.get(source, item["from"]), fields):
            values = tuple(partition_value(value) for value in row)
            if all(value is not None for value in values):
                index[tuple(to_text(values[position]) for position in shared)].add(
                    tuple(values[position] for position in own))

        def key(partition):
            return tuple(to_text(partition[names[position]]) for position in shared)
    return "from", names, own, index, key


def _index_raw_rows(rows, fields, names, *, shared, own, stream_names, index):
    for read_under, record in rows:
        row = tuple(lookup(record, field) for field in fields)
        if any(value is None for value in row):
            continue
        base = {name: read_under.get(name) for name in stream_names}
        if all(same_value(row[position], base[names[position]]) for position in shared):
            index[partition_key(base)].add(tuple(row[position] for position in own))


def _request_choices(item, source, partition, scopes):
    batch = item.get("batch_size")
    if source[0] == "values":
        values = _values(render(item["values"], scopes(partition)))
        if item["name"] in partition:
            return [()] if any(same_value(value, partition[item["name"]]) for value in values) else []
        if batch:
            return [((item["name"], chunk),) for chunk in _batches(values, batch)]
        return [((item["name"], value),) for value in values]
    _, names, own, index, key = source
    rows = index.get(key(partition))
    rows = rows.values if rows is not None else []
    if batch and own:
        return [((names[own[0]], chunk),) for chunk in _batches([row[0] for row in rows], batch)]
    return [tuple((names[position], value) for position, value in zip(own, row, strict=True)) for row in rows]
