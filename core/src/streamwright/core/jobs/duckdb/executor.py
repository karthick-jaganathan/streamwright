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
"""

import logging

from streamwright.core.jobs.duckdb.adapter import ReadContext, WriteContext
from streamwright.core.jobs.duckdb.steps import build_step
from streamwright.core.jobs.policy import SKIPPED, ExecutionPolicy, classify_transient, guard
from streamwright.core.jobs.refs import RefError, decode_value, encode_value
from streamwright.core.jobs.types import ExecutionResult, WriteReceipt, written_files
from streamwright.core.jobs.write_rules import OutputSpec, check_output
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
                 previous_signatures=None, declared_connections=None, config=None):
        """
        `job`: resolved and checked; `adapters`: {provider: DuckDBAdapter} for its connections.
        `declared_connections` (as written, secret references kept) and `config` (the job's values) are what call
        steps hand to the source engine, which renders a connection like a source's auth block.
        """
        self.job, self.plan, self.session, self.adapters = job, plan, session, adapters
        self.connections = job.get("connections") or {}
        self.declared_connections = dict(declared_connections or {})
        self.config = dict(config or {})
        self.secrets, self.allowed_connectors, self.base_dir = secrets or {}, allowed_connectors, base_dir
        self.limits = (job.get("runtime") or {}).get("limits") or {}
        self.previous_signatures = previous_signatures or {}
        self.built = []

    def execute(self, context, manifest, done, hooks):
        run = _BatchRun(self, context, manifest)
        for name in self.plan.order:
            self.session.check_clock()
            self._build(run, self.plan.by_name[name].item)
            self.built.append(name)
        for step, path, reason in run.file_errors:
            LOG.warning("step %s rejected %s: %s", step, path, reason)
        hooks.save_manifest(manifest)
        for node in self.plan.outputs:
            if node.name in done:
                LOG.info("output %s already has a receipt for batch %s: skipped", node.name, context.batch_id)
                continue
            self.session.check_clock()
            receipt = self._write(run, node.item, hooks)
            hooks.record_receipt(receipt, manifest)
            if receipt.already_committed:
                LOG.info("output %s: already committed in an earlier attempt", node.name)
            else:
                LOG.info("output %s: %s rows to %s", node.name, receipt.rows, receipt.destination,
                         extra=fields(event="output_end", export=node.name, records=receipt.rows))
                for path in written_files(receipt):
                    LOG.info("output %s: wrote %s", node.name, path,
                             extra=fields(event="output_file", export=node.name, path=path))
        return ExecutionResult(schema_signatures=tuple(run.schema_signatures),
                               schema_reports=tuple(run.schema_reports))

    def _build(self, run, step):
        """Build one step under its execution policy (retry transient reads, on_error, timeout)."""
        policy = ExecutionPolicy.of(step)
        outcome = guard(policy, lambda: build_step(run, step), classify=classify_transient(policy),
                        on_retry=_retrying("step", step["name"]),
                        on_tolerated=_tolerated(run, "step", step["name"]))
        if outcome is SKIPPED:
            self.session.con.execute(
                "CREATE OR REPLACE TEMP VIEW %s AS SELECT NULL AS record WHERE FALSE" % quote_ident(step["name"]))

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
                        on_tolerated=_tolerated(run, "output", spec.name))
        if receipt is SKIPPED:
            return WriteReceipt(spec.name, "skipped (on_error: %s)" % policy.on_error, 0)
        return receipt
