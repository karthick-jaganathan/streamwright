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

"""The DuckDBJob rule tables and checks (the source of its JSON schema too)."""

import re
from types import MappingProxyType

from streamwright.core.jobs import ingest
from streamwright.core.jobs.apply.log import OUTPUT_LOG_FIELD
from streamwright.core.jobs.apply.rules import check_apply, is_apply_output
from streamwright.core.jobs.calls import call_problems, connection_problems, partition_dependencies, partition_problems
from streamwright.core.jobs.check.findings import connections_of
from streamwright.core.jobs.check.rules import NAME_PATTERN, RESERVED_NAMES, JobRules, input_field, install_hint
from streamwright.core.jobs.duckdb.steps import (
    CALL,
    CONSUME,
    READ,
    RECORDS_TABLE,
    SOURCE,
    SQL,
    STEP_KEYS,
    message_kind,
    step_kind,
)
from streamwright.core.jobs.fields import Field
from streamwright.core.jobs.grammar import (
    APPLY_FIELD,
    ASYNC_JOB_FIELD,
    CONNECTION_GROUPS,
    OPERATION_FIELD,
    POLICY_FIELD,
    public_key,
)
from streamwright.core.jobs.locking import STATE_FIELDS
from streamwright.core.jobs.log_labels import LOG_FIELD
from streamwright.core.jobs.lookups.rules import FAN_OUT_FIELD, LOOKUPS_FIELD
from streamwright.core.jobs.payload import INPUT_FIELD, declared_input
from streamwright.core.jobs.plan import item_dependencies
from streamwright.core.jobs.policy import OUTPUT_POLICY_KEYS, STEP_POLICY_KEYS
from streamwright.core.jobs.step_log import STEP_LOG_FIELD
from streamwright.core.jobs.write_rules import MODES
from streamwright.core.runtime import components

__all__ = ["KIND", "DuckDBRules"]

KIND = "DuckDBJob"
_TEXT = Field("string")
FILE_NAME_HELP = (
    "file outputs: the file's name inside `path` (then a folder). Placeholders: {today} and {now}/{batch_ts}, the "
    "batch's UTC date and time, with offsets and strftime formats ({today-1d:%Y%m%d}, {now+2h:%Y%m%d%H%M%S}; "
    "offsets +/-N with s m h d w); {batch_id}; {seq}, the split's 5-digit number; and the split_by columns ({col}). "
    "No '/', and at least one placeholder"
)


def _named(**fields):
    return Field("object", fields=fields)


_COLUMN_GROUP = Field("object", fields={
    "name": Field("string", required=True), "type": _TEXT, "glob": _TEXT, "regex": _TEXT, "strip_prefix": _TEXT,
    "keep_nulls": Field("boolean")})
_STEP_FIELDS = MappingProxyType({
    "name": Field("string", required=True, pattern=NAME_PATTERN), "description": _TEXT,
    "sql": Field("string", description="DuckDB SQL over earlier steps; in a call or source step, over the table "
                                       "`records`, whose one column `record` (JSON) holds each record: extract "
                                       "scalars with ->> (record->>'$.campaign.id'), which returns VARCHAR, not JSON"),
    "materialize": Field("string", enum=("view", "table")),
    "connection": _TEXT,
    "path": Field("string", description="a file, glob or {{ step.column }} (relative to the connection's roots)"),
    "table": _TEXT, "query": _TEXT,
    "options": Field("object", description="the read function's named parameters"),
    "schema_mode": Field("string", enum=("by_name", "strict")),
    "row_index": Field(("boolean", "string", "object")),
    "columns": Field(("array", "object"), description="header-to-canonical rules (typed), optionally keyed by path"),
    "column_groups": Field("array", items=_COLUMN_GROUP),
    "path_pattern": Field("string", description="regex with named groups extracted from each file path"),
    "vocabulary": Field("array", items=_named(name=Field("string", required=True),
                                              type=Field("string", required=True))),
    "on_unmapped_column": Field("string", enum=ingest.UNMAPPED_COLUMN_POLICIES),
    "operation": OPERATION_FIELD, "policy": POLICY_FIELD, "async_job": ASYNC_JOB_FIELD,
    "partitions": Field("array", items=_named(**{"from": Field("string", required=True),
                                                  "field": Field("string", required=True), "name": _TEXT}),
                        description="call steps: one call per distinct value of `field` in the step `from` (crossed "
                                    "when several), read as {{ partition.NAME }} in the call's arguments (`name` "
                                    "defaults to `field`). The values are not columns of `records`: request the key "
                                    "in the call and extract it from `record`"),
    "source": Field("string", description="a StreamWright source folder (relative to the job's folder)"),
    "stream": _TEXT, "config": Field("object"),
    "log": STEP_LOG_FIELD,
})
_OUTPUT_FIELDS = MappingProxyType({
    "name": Field("string", required=True, pattern=NAME_PATTERN), "description": _TEXT,
    "connection": Field("string", required=True), "step": _TEXT, "sql": _TEXT,
    "mode": Field("string", enum=MODES, description="writes: append | overwrite | merge (files: overwrite | error); "
                                                   "required, except on a publish output"),
    "operation": OPERATION_FIELD,
    "table": _TEXT, "path": _TEXT, "format": _TEXT, "primary_key": Field("array", items=_TEXT),
    "condition": _TEXT, "target_predicate": _TEXT, "replace_where": _TEXT, "whole_table": Field("boolean"),
    "when_matched": Field("array", items=Field("object")), "when_not_matched": Field("array", items=Field("object")),
    "when_not_matched_by_source": Field("array", items=Field("object")),
    "schema_evolution": Field("string", enum=("strict", "add_columns")),
    "partition_by": Field("array", items=_TEXT, description="file outputs: Hive folders (col=value/) under `path`"),
    "split_by": Field("array", items=_TEXT,
                      description="file outputs: one file per distinct combination of these columns; file_name and "
                                  "layout may use {col} (with a format, {col:%Y%m%d}, for a date or timestamp column)"),
    "file_name": Field("string", description=FILE_NAME_HELP),
    "layout": Field("string", description="the subfolders of file_name's file under `path`; the placeholders of "
                                          "file_name, and '/', e.g. {today:%Y/%m} or client={client_id}"),
    "options": Field("object"),
    "apply": APPLY_FIELD,
    "policy": POLICY_FIELD,
    "log": OUTPUT_LOG_FIELD,
})
_RUNTIME = _named(
    threads=Field("integer", minimum=1), memory_limit=_TEXT, temp_directory=_TEXT,
    timeout_seconds=Field("number", minimum=1, description="a batch is stopped (and fails) after this many seconds"),
    state=_named(connection=_TEXT, sqlite=Field("string", description="a SQLite file for the bookkeeping state"),
                 schema=Field("string", pattern=NAME_PATTERN), ttl_seconds=Field("integer", minimum=1),
                 **STATE_FIELDS),
    fan_out=FAN_OUT_FIELD,
    log=LOG_FIELD,
    limits=_named(reference_values=Field("integer", minimum=1), read_groups=Field("integer", minimum=1),
                  events=Field("integer", minimum=1,
                               description="the most events one publish output may produce per batch (10000)"),
                  apply_rows=Field("integer", minimum=1,
                                   description="the most rows one apply output may send per batch (100000)")),
)


# the outbox relay publishes after the commit (at least once, a stable dedupe key): no write policy applies
_PUBLISH_KEYS = frozenset(("name", "description", "connection", "service", "method", "arguments", "sql", "step"))


class DuckDBRules(JobRules):
    kind = KIND
    reserved_names = RESERVED_NAMES | {RECORDS_TABLE}
    builtin_references = frozenset(["batch.id", "batch.ts"])
    step_reference_keys = frozenset(["path", "target_predicate", "replace_where"])
    source_reference_keys = frozenset(["request", "arguments", "paginator", "async_job", "config"])
    step_kinds = STEP_KEYS

    def __init__(self, adapters, connectors=None):
        """`adapters`: {provider: DuckDBAdapter}; `connectors`: loadable source connector names (call steps)."""
        self.adapters = dict(adapters)
        self.connectors = set(connectors if connectors is not None else components.available())

    # --- tables ---------------------------------------------------------------------------------------------

    def manifest_field(self):
        step_fields = dict(_STEP_FIELDS)
        reads = sorted({r for adapter in self.adapters.values() for r in adapter.reads})
        step_fields["read"] = Field("string", enum=tuple(reads))
        for adapter in self.adapters.values():
            for keys in adapter.reads.values():
                for key in keys:
                    step_fields.setdefault(key, Field())
        inputs = Field("object", values=input_field())
        connection = Field("object", fields={"provider": Field("string", required=True), **CONNECTION_GROUPS},
                           open=True,
                           discriminator="provider",
                           variants={p: dict(a.connection_fields) for p, a in self.adapters.items()})
        return Field("object", fields={
            "kind": Field("string", required=True, enum=(KIND,)),
            "version": Field(("integer", "string")),
            "name": Field("string", required=True, pattern=NAME_PATTERN),
            "description": _TEXT,
            "spec": _named(config=inputs, secrets=inputs, lookups=LOOKUPS_FIELD, input=INPUT_FIELD),
            "connections": Field("object", required=True, min_items=1, values=connection),
            "steps": Field("array", required=True, min_items=1, items=Field("object", fields=step_fields)),
            "outputs": Field("array", required=True, min_items=1, items=Field("object", fields=_OUTPUT_FIELDS)),
            "runtime": _RUNTIME,
        })

    # --- hooks ----------------------------------------------------------------------------------------------

    def _adapter(self, connections, step):
        conn = connections.get(step.get("connection"))
        return None if conn is None else self.adapters.get(conn["provider"])

    def classify_step(self, step, connections):
        return step_kind(step, connections, self.adapters)

    def step_keys(self, kind, step, connections):
        keys = STEP_KEYS[kind]
        adapter = self._adapter(connections, step)
        if kind == READ and adapter is not None:
            keys = keys + tuple(adapter.reads.get(step.get("read"), ()))
        return keys

    def step_policy_keys(self, kind):
        """Which execution-policy keys the DuckDB engine honours per step kind (contract v1.2 §6a)."""
        if kind == CALL:
            return STEP_POLICY_KEYS                                 # call steps throttle and page the connector
        if kind == SOURCE:
            return ("retry", "page_size", "on_error", "timeout")   # the source folder owns its own paging/throttle
        if kind == SQL:
            return ("on_error", "timeout")
        if kind == CONSUME:
            return ()                     # the runner polls before the batch; the step only shapes `input`
        return ("retry", "on_error", "timeout")                    # read: bulk I/O, no client-side paging or throttle

    def output_policy_keys(self):
        return OUTPUT_POLICY_KEYS

    def check_step(self, kind, step, connections):
        found = []
        if kind == READ:
            found.extend(self._check_read(step, connections))
        elif kind == CALL:
            provider = connections[step["connection"]]["provider"]
            if provider not in self.connectors:
                found.append(("connector-not-installed", "provider %r has no installed connector for calls" % provider,
                              False))
            if provider in self.connectors:
                found.extend(call_problems(step, connections[step["connection"]]))
        elif kind == CONSUME:
            found.extend(self._adapter(connections, step).check_message(step))
        elif kind == SOURCE and "stream" not in step:
            found.append(("missing-key", "a source step needs `stream`", False))
        found.extend(partition_problems(step))
        return found

    def _check_read(self, step, connections):
        adapter = self._adapter(connections, step)
        if adapter is None:
            provider = connections[step["connection"]]["provider"]
            return [("no-adapter", "provider %r has no DuckDB adapter: %s" % (provider, install_hint(provider)), False)]
        if step["read"] not in adapter.reads:
            return [("bad-value", "%s connections read %s, not %r" % (
                adapter.provider, ", ".join(sorted(adapter.reads)) or "nothing", step["read"]), False)]
        found = list(adapter.check_read(step))
        try:
            ingest.normalize_row_index(step.get("row_index"))
            if step.get("columns") or step.get("vocabulary"):
                ingest.check_vocabulary(step.get("columns"), step.get("vocabulary"))
        except ingest.IngestError as exc:
            found.append(("bad-value", str(exc), False))
        if step.get("path_pattern"):
            try:
                re.compile(step["path_pattern"])
            except re.error as exc:
                found.append(("bad-value", "path_pattern: %s" % exc, False))
        return found

    def check_connection(self, name, connection):
        adapter = self.adapters.get(connection["provider"])
        if adapter is not None:
            return adapter.validate(name, connection)
        if connection["provider"] not in self.connectors:
            return [("connector-not-installed", "provider %r has neither a DuckDB adapter nor a connector "
                     "installed: %s" % (connection["provider"], install_hint(connection["provider"])), False)]
        return connection_problems(connection)

    def write_target(self, connection):
        adapter = self.adapters.get(connection["provider"])
        return adapter.write_target(connection) if adapter is not None else None

    def check_apply(self, output, connection):
        if connection["provider"] in self.adapters or not is_apply_output(output, connection, self.connectors):
            return None
        return check_apply(output, connection)

    def check_publish(self, output, connection):
        adapter = self.adapters.get(connection["provider"])
        kind = message_kind(output, {output.get("connection"): connection}, self.adapters)
        if kind is None:
            if "mode" not in output:
                return [("missing-key", "a write output needs `mode` (append, overwrite, merge; files: overwrite, "
                                        "error)", False)]
            return None if "service" not in output and "method" not in output else [(
                "bad-value", "%s has no messaging operation %s.%s (a write output takes no operation)" % (
                    connection["provider"], output.get("service"), output.get("method")), False)]
        found = []
        if kind != "publish":
            found.append(("wrong-role", "%s.%s consumes messages: it is a step, not an output" % (
                output.get("service"), output.get("method")), False))
        if ("sql" in output) == ("step" in output):
            found.append(("bad-value", "a publish output needs `sql` or `step` (its rows are the events)", False))
        extra = sorted(set(output) - _PUBLISH_KEYS)
        if extra:
            found.append(("unsupported-key", "a publish output takes no %s" % ", ".join(
                public_key(key) for key in extra), False))
        return found + list(adapter.check_message(output))

    def check_engine(self, job):
        consumes = [index for index, step in enumerate(job.get("steps") or ())
                    if message_kind(step, connections_of(job), self.adapters) == "consume"]
        found = [("bad-value", ("steps", index), "a job consumes from one broker step: %s already does" % (
            job["steps"][consumes[0]]["name"]), False) for index in consumes[1:]]
        if consumes and declared_input(job) is not None:
            found.append(("bad-value", ("spec", "input"), "a job takes an input payload (spec.input) or consumes a "
                                                          "broker, not both", False))
        return found

    def secret_keys(self, connection):
        adapter = self.adapters.get(connection["provider"])
        if adapter is not None:
            return adapter.secret_keys
        if connection["provider"] not in self.connectors:
            return ()
        description = components.describe(connection["provider"])   # a call step's source connector
        return description["secret_keys"] if description is not None else ()

    def check_state(self, state, connections):
        if ("connection" in state) == ("sqlite" in state):
            return [("bad-value", "runtime.state needs either `connection` or `sqlite`", False)]
        if "connection" in state:
            conn = connections.get(state["connection"])
            if conn is None:
                return [("unknown-connection", "unknown connection %r" % (state["connection"],), False)]
            adapter = self.adapters.get(conn["provider"])
            if adapter is None or not adapter.holds_state:
                return [("bad-value", "connection %r (provider %s) cannot hold bookkeeping state" % (
                    state["connection"], conn["provider"]), False)]
        elif "schema" in state:
            return [("unsupported-key", "a SQLite state file has no schema (its tables are prefixed)", False)]
        return []

    def dependencies_of(self, item):
        found = item_dependencies(item)
        found.extend(partition_dependencies(item))
        return found
