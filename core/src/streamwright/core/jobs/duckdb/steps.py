#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

"""
DuckDBJob steps: one classification shared by the checker, the plan and the executor, and one handler per kind.

  sql      no connection: `sql` over earlier steps                                 a TEMP VIEW or TABLE
  read     a connection and `read:`: the adapter compiles the read                 a TEMP VIEW or TABLE
  call     a connection and a connector call (`service`/`method`, or `request`)    a TEMP TABLE of the records
  source   `source:` a StreamWright source folder and its `stream`                 a TEMP TABLE of the records

Call and source steps run through the source engine (`SourceRunner.collect`), keep their bookmarks in the batch
manifest's `carry` (so incremental streams resume) and fail on a partial load. Their records land in DuckDB in
batches as JSON. With `sql`, the step is that query over the table `records` (one JSON column `record`), which also
gives an empty result its columns; without it, the records' top-level keys become columns. A message read
(`service: message`) carries its discovered column structure in `carry`, so a batch that polls no messages keeps the
same columns (zero rows) instead of collapsing to the lone `record` column.
"""

import itertools
import json
import os
from types import MappingProxyType

from streamwright.core.config.loader import load_source
from streamwright.core.engine.runner import SourceRunner
from streamwright.core.engine.sql import plan_source
from streamwright.core.jobs.calls import ephemeral_source, partition_scopes, record_value, source_inputs
from streamwright.core.jobs.refs import decode_value, encode_value
from streamwright.core.sql.quote import quote_ident, quote_literal

__all__ = [
    "CALL",
    "READ",
    "RECORDS_TABLE",
    "SOURCE",
    "SQL",
    "STEP_KEYS",
    "StepError",
    "build_step",
    "classify_step",
]

SQL, READ, CALL, SOURCE = "sql", "read", "call", "source"
_COMMON = ("name", "description")
STEP_KEYS = MappingProxyType({
    SQL: (*_COMMON, "sql", "materialize"),
    READ: (*_COMMON, "connection", "read", "materialize"),
    CALL: (*_COMMON, "connection", "service", "method", "arguments", "request", "paginator", "retry", "rate_limit",
           "partitions", "sql"),
    SOURCE: (*_COMMON, "source", "stream", "config", "partitions", "sql"),
})
RECORDS_TABLE = "records"
_INSERT_BATCH = 5000


class StepError(Exception):
    """A step cannot be built."""


def classify_step(step, connections):
    """(kind, None) or (None, why): the one place that decides what a step is."""
    if "source" in step:
        return SOURCE, None
    if "connection" not in step:
        if "sql" in step:
            return SQL, None
        return None, "a step needs `sql`, a `connection` (with `read` or a connector call) or a `source`"
    if "read" in step:
        return READ, None
    if any(key in step for key in ("service", "method", "request")):
        return CALL, None
    return None, "a step with a connection needs `read:`, or a connector call (`service`/`method`, or `request`)"


def _create(con, name, sql, materialize):
    kind = "TABLE" if materialize == "table" else "VIEW"
    con.execute("CREATE OR REPLACE TEMP %s %s AS %s" % (kind, quote_ident(name), sql))


def _build_sql(run, step, kind):
    _create(run.session.con, step["name"], step["sql"], step.get("materialize", "view"))


def _build_read(run, step, kind):
    _create(run.session.con, step["name"], run.compile_read(step), step.get("materialize", "view"))


def _build_records(run, step, kind):
    _load_records(run, step, _collect(run, step, kind), _load_batch(step))


_HANDLERS = MappingProxyType({SQL: _build_sql, READ: _build_read, CALL: _build_records, SOURCE: _build_records})


def _load_batch(step):
    """The bounded batch size records are loaded in (`page_size`), or the default."""
    size = step.get("page_size")
    return int(size) if size else _INSERT_BATCH


def build_step(run, step):
    """Builds one step in the run's session (`run`: the executor's per-batch state)."""
    kind, problem = classify_step(step, run.connections)
    if kind is None:
        raise StepError("step %s: %s" % (step["name"], problem))
    _HANDLERS[kind](run, step, kind)


# --- call and source steps ---------------------------------------------------------------------------------

def _scopes(run, step):
    """Partition values, crossed (calls.partition_scopes); pinned so a replay calls with the same values."""
    def values(index, part):
        key = "partition:%s:%d" % (step["name"], index)
        if key not in run.pinned:
            rows = run.session.con.execute("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL ORDER BY 1" % (
                quote_ident(part["field"]), quote_ident(part["from"]), quote_ident(part["field"]))).fetchall()
            run.pinned[key] = [encode_value(row[0]) for row in rows]
        return [decode_value(value) for value in run.pinned[key]]

    return partition_scopes(step, values)


def _source_path(run, step):
    """A step's source folder: absolute, or relative to the manifest's folder only."""
    path = step["source"]
    if not os.path.isabs(path):
        if run.base_dir is None:
            raise StepError("step %s: a relative source needs the job's folder (load the job from a file)" %
                            step["name"])
        path = os.path.join(run.base_dir, path)
    if not os.path.exists(path):
        raise StepError("step %s: source %r does not exist (relative paths start at the job's folder)" % (
            step["name"], step["source"]))
    return path


def _collect(run, step, kind):
    carry_key = "bookmarks:%s" % step["name"]
    state = run.carry.get(carry_key)
    run.record_columns = None
    if kind == SOURCE:
        source, stream = load_source(_source_path(run, step)), step.get("stream")
        source_plan = plan_source(source)
        planned = source_plan.streams.get(stream)
        if planned is not None and len(planned.exports) == 1:
            run.record_columns = planned.columns.get(planned.exports[0][2])
    else:
        source, stream, source_plan = None, step["name"], None
    for scope in _scopes(run, step):
        if kind == SOURCE:
            config, secrets = source_inputs(source, dict(step.get("config") or {}, **scope), run.secrets)
            runner = SourceRunner(source, config, secrets, allowed_connectors=run.allowed_connectors, plan=source_plan)
            collected = runner.collect(stream, state=state)
            for _, record in collected:
                yield record
        else:
            connection = run.declared_connections[step["connection"]]
            runner = SourceRunner(ephemeral_source(step, connection, scope), run.config, run.secrets,
                                  allowed_connectors=run.allowed_connectors)
            collected = runner.collect(stream, state=state)
            for _, record in collected:
                yield record_value(record)
        if collected.partial:
            raise StepError("step %s: stream %s was loaded partially" % (step["name"], stream))
        state = collected.state
    run.carry[carry_key] = state


def _load_records(run, step, records, batch_size=_INSERT_BATCH):
    con, name = run.session.con, step["name"]
    raw = quote_ident("__records_%s" % name)
    con.execute("CREATE OR REPLACE TEMP TABLE %s (record JSON)" % raw)
    try:
        iterator = iter(records)
        while chunk := list(itertools.islice(iterator, batch_size)):
            con.execute("INSERT INTO %s SELECT unnest(CAST(? AS JSON[]))" % raw,
                        [[json.dumps(record, default=str) for record in chunk]])
        if step.get("sql"):
            con.execute("CREATE OR REPLACE TEMP VIEW %s AS SELECT record FROM %s" % (quote_ident(RECORDS_TABLE), raw))
            try:
                _create(con, name, step["sql"], "table")
            finally:
                con.execute("DROP VIEW IF EXISTS %s" % quote_ident(RECORDS_TABLE))
        else:
            structure = con.execute("SELECT json_group_structure(record) FROM %s" % raw).fetchall()[0][0]
            message_key = "record_structure:%s" % name
            if structure and str(structure).startswith("{"):
                if step.get("service") == "message":
                    run.carry[message_key] = str(structure)
                _create(con, name, "SELECT unnest(json_transform(record, %s)) FROM %s" % (
                    quote_literal(str(structure)), raw), "table")
            elif not structure and step.get("service") == "message" and run.carry.get(message_key):
                # an empty message read keeps the columns of the established schema (zero rows), so steps that
                # reference the message columns do not break on a batch that happened to poll nothing
                _create(con, name, "SELECT unnest(json_transform(record, %s)) FROM %s" % (
                    quote_literal(run.carry[message_key]), raw), "table")
            elif getattr(run, "record_columns", None) and not structure:
                projections = ", ".join("CAST(NULL AS %s) AS %s" % (kind, quote_ident(column))
                                        for column, kind in run.record_columns)
                _create(con, name, "SELECT %s FROM %s WHERE FALSE" % (projections, raw), "table")
            else:
                _create(con, name, "SELECT record FROM %s" % raw, "table")
    finally:
        con.execute("DROP TABLE %s" % raw)
