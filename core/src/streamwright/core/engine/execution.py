"""Page-mode and run-mode source stream executors."""

from streamwright.core.engine import rules, sql
from streamwright.core.engine.errors import SourceError
from streamwright.core.engine.queries import QueryError
from streamwright.core.engine.readers import read_pages
from streamwright.core.net import downloads
from streamwright.core.net.http import HttpError
from streamwright.core.runtime import components
from streamwright.core.runtime.connector_types import ConnectorError
from streamwright.core.runtime.templates import TemplateError, json_numbers

# errors that `on_partition_error: skip` can skip
_PARTITION_ERRORS = (HttpError, ConnectorError, QueryError, SourceError, TemplateError, downloads.DownloadError,
                     sql.SqlError)


def _named_errors(pages, what):
    """The pages of a request, with the errors a partition can skip named after it: "request 'x': ..."."""
    try:
        yield from pages
    except _PARTITION_ERRORS as exc:
        raise SourceError("%s: %s" % (what, exc)) from exc


def _where(stream, request, partition, window=None):
    """
    What a read is for, in log lines and the run's metrics: its stream, request, partition (DECIMAL values as JSON
    numbers, as logs write them) and window.
    """
    where = {"stream": stream["name"], "request": request["name"], "partition": json_numbers(partition)}
    if window is not None:
        where["window"] = {"start": window[0], "end": window[1]}
    return where


class PageModeExecutor:
    def __init__(self, runner):
        self.runner = runner

    def execute(self, stream, plan, partitions, stats):
        runner = self.runner
        request = plan.requests[0][1]
        client, context = runner._request_setup(stream)
        sandbox = runner._workspace()
        steps = [(step["name"], plan.queries[step["name"]], runner._parameters(plan, step["name"]),
                  plan.conform.get(step["name"])) for _, step in plan.steps]
        exports = [(export, step, plan.columns[step]) for export, _, step in plan.exports]
        for export, spec, step in plan.exports:
            runner.writer.write_schema(export, plan.columns[step], spec.get("primary_key") or [], stats)
        wanted = runner.collected.get(stream["name"], {}) if len(exports) == 1 else {}
        sandbox.create_raw(request["name"])
        try:
            for partition in partitions:
                try:
                    for window in runner.partitions_planner.windows(stream, partition):
                        scope = {"partition": partition, "config": runner.config, "window": window,
                                 "today": runner.today}
                        where = _where(stream, request, partition, window)
                        read = runner.metrics.read(where)
                        pages = read_pages(runner, stream, request, client, context,
                                           scopes=runner._scopes(partition, window), where=where, read=read)
                        for page in _named_errors(pages, "request %r" % request["name"]):
                            sandbox.run_page(request["name"], page, scope, steps)
                            for export, step, columns in exports:
                                runner.writer.write_records(export, sandbox.table_records(step, columns), wanted, stats)
                        if window is not None:
                            stats.windows += 1
                            runner.bookmarks.advance(stream, partition, window)
                        read.done(request=False)
                except _PARTITION_ERRORS as exc:
                    runner._skip_partition(stream, partition, exc, stats)
        finally:
            for table in [request["name"]] + [step[0] for step in steps]:
                sandbox.drop_table(table)
        if stream["name"] in runner.incomplete:
            runner.writer.mark_partial(plan)


class RunModeExecutor:
    def __init__(self, runner):
        self.runner = runner

    def execute(self, stream, plan, partitions, stats):
        runner = self.runner
        name = stream["name"]
        client, context = runner._request_setup(stream)
        sandbox = runner._workspace()
        planner = runner.partitions_planner
        partitions = [{"partition": partition, "failed": False, "windows": planner.windows(stream, partition)}
                      for partition in partitions]
        steps = [step["name"] for _, step in plan.steps]
        tables, ran = [], {"steps": False, "stale": False}
        try:
            self._run_operations(stream, plan, partitions, client=client, context=context, tables=tables, ran=ran,
                                 stats=stats)
            while tables:
                sandbox.drop_table(tables.pop())
            self._export_records(name, plan, stats)
        finally:
            for table in tables + steps:
                sandbox.drop_table(table)
        if name in runner.incomplete:
            runner.writer.mark_partial(plan)
        self._save_bookmarks(stream, partitions, stats)

    def _run_operations(self, stream, plan, partitions, *, client, context, tables, ran, stats):
        for kind, item in plan.order:
            if kind == "request":
                self.run_request(stream, item, partitions, client=client, context=context, tables=tables, ran=ran,
                                 stats=stats)
            else:
                self.run_step(stream["name"], plan, item["name"])
                ran["steps"] = True
        if ran["stale"]:
            for _, step in plan.steps:
                self.run_step(stream["name"], plan, step["name"])

    def _export_records(self, name, plan, stats):
        runner = self.runner
        sandbox = runner._workspace()
        outputs = []
        for export, spec, step in plan.exports:
            keys = spec.get("primary_key") or []
            try:
                problem, columns = sandbox.key_problem(step, keys) if keys else None, sandbox.table_columns(step)
            except sql.SqlError as exc:
                problem = str(exc)
            if problem:
                raise SourceError(runner.redact("stream %r: export %r: %s" % (name, export, problem)))
            outputs.append((export, step, columns, keys))
        wanted = runner.collected.get(name, {}) if len(outputs) == 1 else {}
        for export, step, columns, keys in outputs:
            runner.writer.write_schema(export, columns, keys, stats)
            runner.writer.write_records(export, sandbox.table_records(step, columns, keys), wanted, stats)

    def _save_bookmarks(self, stream, partitions, stats):
        if stream.get("incremental"):
            for item in partitions:
                for window in [] if item["failed"] else item["windows"]:
                    stats.windows += 1
                    self.runner.bookmarks.advance(stream, item["partition"], window, write=False)
            self.runner.bookmarks.flush()

    def run_request(self, stream, request, partitions, *, client, context, tables, ran, stats):
        runner = self.runner
        sandbox = runner._workspace()
        table = request["name"]
        windowed = rules.uses_window(components.request_body(request))
        sandbox.create_raw(table)
        tables.append(table)
        try:
            resolve = runner.partitions_planner.request_partitions(stream, request)
        except sql.SqlError as exc:
            message = "stream %r: request %r: partitions: %s" % (stream["name"], table, exc)
            raise SourceError(runner.redact(message)) from exc
        for item in partitions:
            if item["failed"]:
                continue
            partition = item["partition"]
            windows = item["windows"] if windowed else [None]
            span = (windows[0][0], windows[-1][1]) if windows and windows[0] is not None else None
            read = runner.metrics.read(_where(stream, request, partition, span))
            try:
                for request_partition in resolve(partition):
                    for window in windows:
                        scope = {"partition": request_partition, "config": runner.config, "window": window,
                                 "today": runner.today}
                        for page in read_pages(runner, stream, request, client, context,
                                               scopes=runner._scopes(request_partition, window),
                                               where=_where(stream, request, request_partition, window), read=read):
                            sandbox.add_raw(table, page, scope)
                if windows:
                    read.done()
            except _PARTITION_ERRORS as exc:
                runner._skip_partition(stream, partition, "request %r: %s" % (table, exc), stats)
                item["failed"] = True
                for other in tables:
                    sandbox.delete_partition(other, partition)
                ran["stale"] = ran["stale"] or ran["steps"]

    def run_step(self, stream_name, plan, step):
        runner = self.runner
        try:
            runner._workspace().materialize(step, plan.queries[step], runner._parameters(plan, step),
                                           plan.conform.get(step))
        except sql.SqlError as exc:
            raise SourceError(runner.redact("stream %r: step %r: %s" % (stream_name, step, exc))) from exc
