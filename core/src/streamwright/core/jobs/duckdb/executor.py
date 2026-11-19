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
The DuckDB executor: builds a job's steps in a Session and writes its outputs through their adapters. It implements
`streamwright.core.jobs.types.Executor`, so the shared Runner gives it leases, receipts, replay and fencing.

Per batch: the batch's broker messages are the table `input` (a consume step shapes them), the steps are built, the
write outputs run (each recorded with its receipt), then `_commits` (output, table_name, version, rows, batch_id) and
`_file_errors` (step, path, reason) describe them, and the publish and apply outputs' rows become the batch's events,
which the runner commits to the outbox with the batch and relays after the messages are acknowledged.
"""

import json
import logging

from streamwright.core.jobs.apply.rules import DEFAULT_APPLY_ROWS
from streamwright.core.jobs.duckdb.adapter import ReadContext, WriteContext
from streamwright.core.jobs.duckdb.steps import MESSAGES_TABLE, build_step, message_kind
from streamwright.core.jobs.payload import declared_input, load_input
from streamwright.core.jobs.policy import SKIPPED, ExecutionPolicy, classify_transient, guard
from streamwright.core.jobs.refs import RefError, decode_value, encode_value
from streamwright.core.jobs.step_log import StepLog, log_step_rows
from streamwright.core.jobs.types import ExecutionResult, WriteReceipt, written_files
from streamwright.core.jobs.write_rules import OutputSpec, check_output
from streamwright.core.runtime.logs.context import log_context
from streamwright.core.runtime.logs.lines import fields
from streamwright.core.sql.quote import quote_ident

__all__ = ["DuckDBExecutor", "ExecutionError"]

LOG = logging.getLogger("streamwright.duckdb")


class ExecutionError(Exception):
    """A step or output cannot run as declared."""


def _retrying(surface, name):
    """A `guard` on_retry callback that logs a retry without the (possibly sensitive) error value."""
    def log(exc, delay, number):
        LOG.warning("%s %s: attempt %d failed (%s), retrying in %.1fs", surface, name, number,
                    type(exc).__name__, delay)
    return log


def _tolerated(run, surface, name):
    """A `guard` on_tolerated callback: record the skip/warn and log it (the error type only)."""
    def note(exc, mode):
        run.tolerated.append((name, mode))
        (LOG.warning if mode == "warn" else LOG.error)("%s %s %s on %s: continuing", surface, name, mode,
                                                       type(exc).__name__)
    return note


class _BatchRun:
    """What the steps of one batch share: the session, pinned values, bookkeeping lists."""

    def __init__(self, executor, context, manifest):
        self.session, self.connections, self.adapters = executor.session, executor.connections, executor.adapters
        self.declared_connections, self.config = executor.declared_connections, executor.config
        self.secrets, self.allowed_connectors, self.base_dir = (executor.secrets, executor.allowed_connectors,
                                                                executor.base_dir)
        self.limits, self.batch = executor.limits, context
        self.pinned = manifest.setdefault("pinned", {})
        self.carry = manifest.setdefault("carry", {})
        self.previous_signatures = executor.previous_signatures
        self.file_errors, self.schema_reports, self.schema_signatures = [], [], []
        self.tolerated = []                          # (step or output name, "skip"|"warn") an on_error policy swallowed

    def values(self, step, column):
        """The distinct values of step.column, collected once per batch and pinned (runtime.limits.reference_values)."""
        key = "%s.%s" % (step, column)
        cap = int(self.limits.get("reference_values", 10000))
        if key not in self.pinned:
            rows = self.session.con.execute("SELECT DISTINCT %s FROM %s ORDER BY 1 LIMIT %d" % (
                quote_ident(column), quote_ident(step), cap + 1)).fetchall()
            if len(rows) > cap:
                raise RefError("{{ %s }} has more than %d distinct values (runtime.limits.reference_values)" % (
                    key, cap))
            self.pinned[key] = [encode_value(row[0]) for row in rows]
        return [decode_value(value) for value in self.pinned[key]]

    def compile_read(self, step):
        connection = self.connections[step["connection"]]
        context = ReadContext(self.session, step["connection"], connection, step, batch=self.batch, pinned=self.pinned,
                              limits=self.limits, values=self.values, previous_signatures=self.previous_signatures)
        sql = self.adapters[connection["provider"]].compile_read(context, step)
        self.file_errors.extend(context.file_errors)
        self.schema_reports.extend(context.schema_reports)
        self.schema_signatures.extend(context.schema_signatures)
        return sql


class DuckDBExecutor:
    def __init__(self, job, plan, session, adapters, *, secrets=None, allowed_connectors=None, base_dir=None,
                 previous_signatures=None, declared_connections=None, config=None, apply_outputs=(), payload=None,
                 redact=None, log_row_values=True):
        """
        `job`: resolved and checked; `adapters`: {provider: DuckDBAdapter} for its connections; `apply_outputs`: the
        names of its outputs that apply connector operations (their rows go to the outbox, like publish outputs);
        `payload`: this run's input payload (streamwright.core.jobs.payload), for a batch that did not keep its rows.
        `declared_connections` (as written, secret references kept) and `config` (the job's values) are what call
        steps hand to the source engine, which renders a connection like a source's auth block. `redact` scrubs the
        lines of a step's `log:`; `log_row_values` False (streamwright serve) logs only their number of rows.
        """
        self.job, self.plan, self.session, self.adapters = job, plan, session, adapters
        self.connections = job.get("connections") or {}
        self.declared_connections = dict(declared_connections or {})
        self.config = dict(config or {})
        self.secrets, self.allowed_connectors, self.base_dir = secrets or {}, allowed_connectors, base_dir
        self.limits = (job.get("runtime") or {}).get("limits") or {}
        self.previous_signatures = previous_signatures or {}
        self.apply_outputs = frozenset(apply_outputs)
        self.payload = payload
        self.redact, self.log_row_values = redact, log_row_values

    def execute(self, context, manifest, done, hooks):
        self.session.start_clock()            # runtime.timeout_seconds bounds each batch
        try:
            return self._execute(context, manifest, done, hooks)
        finally:
            self.session.stop_clock()

    def _publishes(self, item):
        """Whether an output's rows go to the outbox (a publish or an apply output) instead of being written."""
        return item["name"] in self.apply_outputs or message_kind(item, self.connections, self.adapters) == "publish"

    def _messages(self, manifest):
        con = self.session.con
        declared = declared_input(self.job)
        if declared is not None:              # the payload is the batch's input (a job has a payload or a consumer)
            load_input(con, MESSAGES_TABLE, declared, manifest, self.payload)
            return
        con.execute("CREATE OR REPLACE TEMP TABLE %s (message_id VARCHAR, key VARCHAR, body VARCHAR, "
                    "headers MAP(VARCHAR, VARCHAR), timestamp VARCHAR)" % quote_ident(MESSAGES_TABLE))
        rows = [(m["message_id"], m.get("key"), m["body"], list((m.get("headers") or {}).keys()),
                 [str(value) for value in (m.get("headers") or {}).values()], m.get("timestamp"))
                for m in manifest.get("messages") or ()]
        if rows:
            con.executemany("INSERT INTO %s VALUES (?, ?, ?, MAP(?::VARCHAR[], ?::VARCHAR[]), ?)"
                            % quote_ident(MESSAGES_TABLE), rows)

    def _runtime_tables(self, run, receipts):
        con = self.session.con
        con.execute("CREATE OR REPLACE TEMP TABLE _commits (output VARCHAR, table_name VARCHAR, version BIGINT, "
                    "rows BIGINT, batch_id VARCHAR)")
        rows = [(name, receipt.get("destination"), receipt.get("version"), receipt.get("rows"), run.batch.batch_id)
                for name, receipt in sorted(receipts.items())]
        if rows:
            con.executemany("INSERT INTO _commits VALUES (?, ?, ?, ?, ?)", rows)
        con.execute("CREATE OR REPLACE TEMP TABLE _file_errors (step VARCHAR, path VARCHAR, reason VARCHAR)")
        if run.file_errors:
            con.executemany("INSERT INTO _file_errors VALUES (?, ?, ?)", [tuple(e) for e in run.file_errors])

    def _events(self, item):
        """The rows of a publish or apply output (its `sql`, or its `step`), as JSON-ready events."""
        applies = item["name"] in self.apply_outputs
        limit = "apply_rows" if applies else "events"
        cap = int(self.limits.get(limit, DEFAULT_APPLY_ROWS if applies else 10000))
        sql = item.get("sql") or "SELECT * FROM %s" % quote_ident(item["step"])
        cursor = self.session.con.execute("SELECT * FROM (%s) LIMIT %d" % (sql, cap + 1))
        names = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        if len(rows) > cap:
            raise ExecutionError("%s output %s has more than %d rows (runtime.limits.%s)" % (
                "apply" if applies else "publish", item["name"], cap, limit))
        return [json.loads(json.dumps(dict(zip(names, row, strict=True)), default=str)) for row in rows]

    def _execute(self, context, manifest, done, hooks):
        run = _BatchRun(self, context, manifest)
        self._messages(manifest)
        for name in self.plan.order:
            self.session.check_clock()
            with log_context(step=name):
                self._build(run, self.plan.by_name[name].item)
        for step, path, reason in run.file_errors:
            LOG.warning("step %s rejected %s: %s", step, path, reason)
        hooks.save_manifest(manifest)
        receipts = dict(done)
        publishes = [node for node in self.plan.outputs if self._publishes(node.item)]
        for node in (node for node in self.plan.outputs if not self._publishes(node.item)):
            with log_context(output=node.name):
                if node.name in done:
                    LOG.info("output %s already has a receipt for batch %s: skipped", node.name, context.batch_id)
                    continue
                self.session.check_clock()
                receipt = self._write(run, node.item, hooks)
                hooks.record_receipt(receipt, manifest)
                receipts[node.name] = receipt.to_dict()
                if receipt.already_committed:
                    LOG.info("output %s: already committed in an earlier attempt", node.name)
                else:
                    LOG.info("output %s: %s rows to %s", node.name, receipt.rows, receipt.destination,
                             extra=fields(event="output_end", export=node.name, records=receipt.rows))
                    for path in written_files(receipt):
                        LOG.info("output %s: wrote %s", node.name, path,
                                 extra=fields(event="output_file", export=node.name, path=path))
        self._runtime_tables(run, receipts)
        events = {}
        for node in publishes:
            with log_context(output=node.name):
                events[node.name] = rows = self._events(node.item)
                LOG.info("output %s: %d %s to the outbox", node.name, len(rows),
                         "operations" if node.name in self.apply_outputs else "events",
                         extra=fields(event="output_end", export=node.name, records=len(rows)))
        return ExecutionResult(events=events, schema_signatures=tuple(run.schema_signatures),
                               schema_reports=tuple(run.schema_reports))

    def _build(self, run, step):
        """Build one step under its execution policy (retry transient reads, on_error, timeout)."""
        policy = ExecutionPolicy.of(step)
        outcome = guard(policy, lambda: build_step(run, step), classify=classify_transient(policy),
                        on_retry=_retrying("step", step["name"]),
                        on_tolerated=_tolerated(run, "step", step["name"]), on_timeout=self.session.con.interrupt)
        if outcome is SKIPPED:
            self.session.con.execute(
                "CREATE OR REPLACE TEMP VIEW %s AS SELECT NULL AS record WHERE FALSE" % quote_ident(step["name"]))
        else:
            self._log_rows(run, step)

    def _log_rows(self, run, step):
        """A step's `log:`: its first rows (max_rows + 1), counted only when there are more or values are not logged."""
        log = StepLog.of(step)
        if log is None:
            return
        con, relation = self.session.con, quote_ident(step["name"])
        columns = [column[0] for column in con.execute("SELECT * FROM %s LIMIT 0" % relation).description]
        log_step_rows(log, columns, lambda n: con.execute("SELECT * FROM %s LIMIT %d" % (relation, n)).fetchall(),
                      lambda: con.execute("SELECT count(*) FROM %s" % relation).fetchone()[0],
                      row_values=self.log_row_values, redact=self.redact,
                      constants={"batch.id": run.batch.batch_id, "batch.ts": run.batch.batch_ts})

    def _write(self, run, item, hooks):
        spec = OutputSpec.from_mapping(item)
        connection = self.connections[spec.connection]
        adapter = self.adapters[connection["provider"]]
        target = adapter.write_target(connection)
        problems = [p for p in check_output(spec, target) if not p.warning] if target else []
        if target is None or problems:
            raise ExecutionError("output %s cannot be written: %s" % (
                spec.name, "; ".join(p.message for p in problems) or "its connection is not writable"))
        source = quote_ident(spec.step) if spec.step else quote_ident("__output_%s" % spec.name)

        def write():
            if spec.sql:
                self.session.con.execute("CREATE OR REPLACE TEMP VIEW %s AS %s" % (source, spec.sql))
            hooks.fence()
            return adapter.write(WriteContext(self.session, spec.connection, connection, spec, source=source,
                                              batch=run.batch, values=run.values, limits=self.limits))

        policy = ExecutionPolicy.of(item)
        receipt = guard(policy, write, classify=classify_transient(policy),
                        on_retry=_retrying("output", spec.name),
                        on_tolerated=_tolerated(run, "output", spec.name), on_timeout=self.session.con.interrupt)
        if receipt is SKIPPED:
            return WriteReceipt(spec.name, "skipped (on_error: %s)" % policy.on_error, 0)
        return receipt
