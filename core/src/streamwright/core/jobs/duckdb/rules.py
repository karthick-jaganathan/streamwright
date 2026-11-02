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
from streamwright.core.jobs.calls import partition_dependencies, partition_problems
from streamwright.core.jobs.check.rules import NAME_PATTERN, RESERVED_NAMES, JobRules, input_field
from streamwright.core.jobs.duckdb.steps import CALL, READ, RECORDS_TABLE, SOURCE, SQL, STEP_KEYS, classify_step
from streamwright.core.jobs.fields import Field
from streamwright.core.jobs.plan import item_dependencies
from streamwright.core.jobs.policy import OUTPUT_POLICY_KEYS, STEP_POLICY_KEYS
from streamwright.core.jobs.write_rules import MODES
from streamwright.core.runtime import components

__all__ = ["KIND", "DuckDBRules"]

KIND = "DuckDBJob"
_TEXT = Field("string")
# the execution-policy vocabulary (contract v1.2 §6a), accepted structurally on steps and outputs
_POLICY_FIELDS = MappingProxyType({
    "retry": Field("object"), "rate_limit": Field("object"), "paginator": Field("object"),
    "page_size": Field("integer", minimum=1), "on_error": _TEXT,
    "timeout": Field(("number", "string"), description="per-call timeout, seconds or a duration like `30s`"),
})


def _named(**fields):
    return Field("object", fields=fields)


_COLUMN_GROUP = Field("object", fields={
    "name": Field("string", required=True), "type": _TEXT, "glob": _TEXT, "regex": _TEXT, "strip_prefix": _TEXT,
    "keep_nulls": Field("boolean")})
_STEP_FIELDS = MappingProxyType({
    "name": Field("string", required=True, pattern=NAME_PATTERN), "description": _TEXT,
    "sql": Field("string", description="DuckDB SQL over earlier steps (in call/source steps: over `records`)"),
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
    "service": _TEXT, "method": _TEXT, "arguments": Field("object"),
    "request": _named(path=_TEXT, method=_TEXT, params=Field("object"), headers=Field("object"), json=Field(),
                      records=Field("object")),
    "paginator": Field("object"), "retry": Field("object"), "rate_limit": Field("object"),
    "page_size": _POLICY_FIELDS["page_size"], "on_error": _TEXT, "timeout": _POLICY_FIELDS["timeout"],
    "partitions": Field("array", items=_named(**{"from": Field("string", required=True),
                                                  "field": Field("string", required=True), "name": _TEXT})),
    "source": Field("string", description="a StreamWright source folder (relative to the job's folder)"),
    "stream": _TEXT, "config": Field("object"),
})
_OUTPUT_FIELDS = MappingProxyType({
    "name": Field("string", required=True, pattern=NAME_PATTERN), "description": _TEXT,
    "connection": Field("string", required=True), "step": _TEXT, "sql": _TEXT,
    "mode": Field("string", required=True, enum=MODES),
    "table": _TEXT, "path": _TEXT, "format": _TEXT, "primary_key": Field("array", items=_TEXT),
    "condition": _TEXT, "target_predicate": _TEXT, "replace_where": _TEXT, "whole_table": Field("boolean"),
    "when_matched": Field("array", items=Field("object")), "when_not_matched": Field("array", items=Field("object")),
    "when_not_matched_by_source": Field("array", items=Field("object")),
    "schema_evolution": Field("string", enum=("strict", "add_columns")),
    "partition_by": Field("array", items=_TEXT), "split_by": Field("array", items=_TEXT),
    "file_name": _TEXT, "layout": _TEXT, "options": Field("object"),
    **_POLICY_FIELDS,
})
_RUNTIME = _named(
    threads=Field("integer", minimum=1), memory_limit=_TEXT, temp_directory=_TEXT,
    timeout_seconds=Field("number", minimum=1, description="the run is stopped (and the batch fails) after this"),
    state=_named(connection=_TEXT, sqlite=Field("string", description="a SQLite file for the bookkeeping state"),
                 schema=Field("string", pattern=NAME_PATTERN), ttl_seconds=Field("integer", minimum=1),
                 run_key=Field("string", description="a batch with this key runs once")),
    limits=_named(reference_values=Field("integer", minimum=1), read_groups=Field("integer", minimum=1)),
)


class DuckDBRules(JobRules):
    kind = KIND
    reserved_names = RESERVED_NAMES | {RECORDS_TABLE}
    builtin_references = frozenset(["batch.id", "batch.ts"])
    step_reference_keys = frozenset(["path", "target_predicate", "replace_where"])
    source_reference_keys = frozenset(["request", "arguments", "paginator", "config"])
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
        connection = Field("object", fields={"provider": Field("string", required=True)}, open=True,
                           discriminator="provider",
                           variants={p: dict(a.connection_fields) for p, a in self.adapters.items()})
        return Field("object", fields={
            "kind": Field("string", required=True, enum=(KIND,)),
            "version": Field(("integer", "string")),
            "name": Field("string", required=True, pattern=NAME_PATTERN),
            "description": _TEXT,
            "spec": _named(config=inputs, secrets=inputs),
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
        return classify_step(step, connections)

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
            if "request" in step and ("service" in step or "method" in step):
                found.append(("bad-value", "a call is `request` (HTTP) or `service`/`method` (SDK), not both", False))
        elif kind == SOURCE and "stream" not in step:
            found.append(("missing-key", "a source step needs `stream`", False))
        found.extend(partition_problems(step))
        return found

    def _check_read(self, step, connections):
        adapter = self._adapter(connections, step)
        if adapter is None:
            provider = connections[step["connection"]]["provider"]
            return [("no-adapter", "provider %r has no DuckDB adapter (pip install streamwright-%s)" % (
                provider, provider), False)]
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
                     "installed (pip install streamwright-%s)" % (connection["provider"], connection["provider"]),
                     False)]
        return []

    def write_target(self, connection):
        adapter = self.adapters.get(connection["provider"])
        return adapter.write_target(connection) if adapter is not None else None

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
