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
Delivering apply outputs' rows (honest at-least-once, plan §2.5). The executor's rows are committed to the outbox
(sw_events) with the batch, with the output's rendered arguments pinned in the destination. Then, per batch and output:

  1. rows whose action, destructive action or NULLs the output does not allow fail without a call;
  2. each chunk is marked `in_flight` (write-ahead) before its call, and its rows' outcomes are stored after it;
  3. a chunk left `in_flight` (the run stopped mid-call) has an unknown outcome: an idempotent operation sends it
     again; otherwise `on_unknown: hold` stops for the operator (`state resend` / `state skip`), `reconcile` asks the
     connector what exists;
  4. a call the vendor rejects as a whole fails its rows; a transient failure that outlasts the retries leaves them in
     flight (unknown). A non-idempotent operation's call is sent once, never retried in the run (a call that timed out
     may have been applied): its transient failure leaves the rows in flight for step 3 to decide.

The connector context (its retry policy and the output's rate limit) is made once per output and run, so the rate
limit holds across the batches a run relays.

Rows are never logged; outcomes are kept in the outbox (`state show`).
"""

import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, NamedTuple

from streamwright.core.config.inputs import parse_duration
from streamwright.core.jobs.apply.errors import ApplyError, RowsRejectedError
from streamwright.core.jobs.apply.http_send import HttpApplySession
from streamwright.core.jobs.apply.log import OUTPUT_LOG, OutputLog, log_outcomes
from streamwright.core.jobs.apply.rules import REQUEST, apply_operation, is_apply_output
from streamwright.core.jobs.grammar import CONNECTION_HTTP_KEYS, CONNECTION_POLICY_KEYS
from streamwright.core.jobs.types import ERROR_TEXT_LIMIT
from streamwright.core.net.policies import RateLimiter
from streamwright.core.runtime import source_connectors
from streamwright.core.runtime.apply import (
    ACTION_COLUMN,
    APPLIED,
    APPLY_CAPABILITIES,
    FAILED,
    ApplySession,
    PartialApplyError,
    ReconcilingSession,
    RowResult,
)
from streamwright.core.runtime.components import declared_auth
from streamwright.core.runtime.connector_types import ConnectorContext, ConnectorError
from streamwright.core.runtime.contract_validation import check_arguments, resolve_arguments, resolve_values
from streamwright.core.runtime.logs.lines import fields, milliseconds, thousands
from streamwright.core.runtime.templates import render

__all__ = ["Applier", "ApplyCall", "ApplyError", "apply_calls"]


@dataclass(frozen=True)
class ApplyCall:
    """An apply output: its connection (as declared), connector operation and policies."""
    output: str
    connection_name: str
    connection: Mapping[str, Any]
    definition: Any
    operation: Any
    chunk_size: int
    on_row_error: str = "fail"
    nulls: str = "error"
    on_unknown: str = "hold"
    allow_remove: bool = False
    retry: Mapping[str, Any] | None = None
    rate_limit: Mapping[str, Any] | None = None
    log: OutputLog | None = None                  # its `log:`: the results of the rows a run sends
    request: bool = False                         # its operation is a request (`http.send` of a REST API connector)

    def destination(self, item):
        """The outbox destination of a batch's rows: the operation and its rendered arguments (or request), pinned."""
        arguments = item.get(REQUEST if self.request else "arguments")
        return json.dumps({"operation": self.operation.name, "arguments": arguments or {}}, sort_keys=True,
                          default=str)


def apply_calls(job, declared_connections, connectors):
    """{output name: ApplyCall} of a job's apply outputs (`declared_connections`: as written, secret refs kept)."""
    found = {}
    for item in job.get("outputs") or ():
        connection = (job.get("connections") or {}).get(item.get("connection"))
        if not is_apply_output(item, connection, connectors):
            continue
        definition, operation = apply_operation(item, connection)
        if operation is None:
            raise ApplyError("output %s: %s has no apply operation %s.%s" % (
                item["name"], connection["provider"], item.get("service"), item.get("method")))
        request = REQUEST in item                     # a REST connection's policy is the default of its requests
        found[item["name"]] = ApplyCall(
            item["name"], item["connection"], declared_connections[item["connection"]], definition, operation,
            int(item.get("chunk_size") or operation.max_chunk), item.get("on_row_error", "fail"),
            item.get("nulls", "error"), item.get("on_unknown", "hold"), bool(item.get("allow_remove", False)),
            item.get("retry") or (connection.get("retry") if request else None),
            item.get("rate_limit") or (connection.get("rate_limit") if request else None), OutputLog.of(item),
            request)
    return found


def _row_problem(call, payload):
    """(prepared payload, None) or (None, (error code, message)) per the output's action and NULL policies."""
    operation = call.operation
    row = dict(payload)
    action = row.get(ACTION_COLUMN)
    action = operation.actions[0] if action is None else action
    if action not in operation.actions:
        return None, ("INVALID_ACTION", "op %r is not an action of %s (%s)" % (
            action, operation.name, ", ".join(operation.actions)))
    if action in operation.destructive_actions and not call.allow_remove:
        return None, ("REMOVE_NOT_ALLOWED", "op %s needs allow_remove: true on output %s" % (action, call.output))
    row[ACTION_COLUMN] = action
    empty = sorted(key for key, value in row.items() if value is None)
    if empty and call.nulls == "error":
        return None, ("NULL_VALUE", "NULL in %s (nulls: error; skip_field leaves a NULL field out, clear clears it)"
                      % ", ".join(empty))
    if call.nulls == "skip_field":
        row = {key: value for key, value in row.items() if value is not None}
    return row, None


class _Target(NamedTuple):
    """Where one batch's outcomes are recorded: the state store, the run's lease, the batch."""
    state: Any
    lease: Any
    batch_id: str


def _sequence(row):
    return row["event_sequence"]


def _sequence_of(outcome):
    return outcome[0]


def _log_chunk(output, number, chunks, outcomes, seconds):
    """A chunk's call: `output NAME: call 2/5: 2,000 row(s), 1,998 applied, 2 failed, 1.4 s`."""
    applied = sum(1 for outcome in outcomes if outcome[1] == APPLIED)
    OUTPUT_LOG.info("output %s: call %d/%d: %s row(s), %s applied, %s failed, %.1f s", output, number, chunks,
                    thousands(len(outcomes)), thousands(applied), thousands(len(outcomes) - applied), seconds,
                    extra=fields(event="apply_call", records=len(outcomes),
                                 duration_ms=milliseconds(seconds)))


class Applier:
    """Applies a run's apply outputs through their connectors' sessions (one per connection, opened when needed)."""

    def __init__(self, calls, *, config, secrets, redact, log_row_values=True):
        """`log_row_values` False (streamwright serve): an output's `log:` logs only its number of rows."""
        self.calls = dict(calls)
        self.config, self.secrets, self.redact = dict(config or {}), dict(secrets or {}), redact
        self.log_row_values = log_row_values
        self._sessions = {}                                   # connection -> its open session
        self._contexts = {}                                   # output -> its ConnectorContext
        self.counts = {}                                      # output -> {"applied": n, "failed": n}

    def destination(self, output, item):
        return self.calls[output].destination(item)

    def close(self):
        for session in self._sessions.values():
            if callable(getattr(session, "close", None)):
                session.close()
        self._sessions.clear()
        self._contexts.clear()

    def _context(self, call):
        """The output's context, made once per run; a non-idempotent operation gets one attempt (no retries)."""
        if call.output not in self._contexts:
            limit = call.rate_limit
            limiter = (RateLimiter(limit["requests"], parse_duration(str(limit["per"])).total_seconds())
                       if limit else None)
            retry = call.retry if call.operation.idempotent else dict(call.retry or {}, max_attempts=1)
            self._contexts[call.output] = ConnectorContext(call.definition.name, self.redact, retry=retry,
                                                           rate_limiter=limiter,
                                                           default_codes=call.definition.retry_codes)
        return self._contexts[call.output]

    def _session(self, call):
        """(session, context) of the call's connection (rendered, checked, opened once per run) and output."""
        if call.connection_name in self._sessions:
            return self._sessions[call.connection_name], self._context(call)
        defaults = (*CONNECTION_HTTP_KEYS, *CONNECTION_POLICY_KEYS) if call.request else ()
        definition = call.definition
        source = {"auth": {key: value for key, value in call.connection.items() if key not in defaults}}
        auth = render(declared_auth(source), {"config": self.config, "secrets": self.secrets})
        problems = (source_connectors.check_declared_auth(definition, declared_auth(source))
                    + source_connectors.check_resolved_auth(definition, auth))
        if problems:
            raise ApplyError(self.redact("output %s: %s" % (call.output, "; ".join(problems))))
        context = self._context(call)
        if call.request:
            http = render({key: call.connection[key] for key in CONNECTION_HTTP_KEYS if key in call.connection},
                          {"config": self.config, "secrets": self.secrets})
            for value in resolve_values(definition, auth).secret_values:
                context.secret(value)
            self._sessions[call.connection_name] = HttpApplySession(auth, http, self.redact)
            return self._sessions[call.connection_name], context
        try:
            session = source_connectors.open_session(definition, auth, context)
        except ConnectorError as exc:
            raise ApplyError(self.redact("%s: cannot connect: %s" % (definition.name, exc))) from None
        if not isinstance(session, ApplySession):
            raise ApplyError("connector %s does not apply operations (its session has no apply())" % definition.name)
        self._sessions[call.connection_name] = session
        return session, context

    # --- one batch's rows of one output ---------------------------------------------------------------------

    def deliver(self, state, lease, batch_id, output, *, rows, log):
        """Applies the pending and in-flight rows of (batch, output); raises ApplyError when the policy says stop."""
        call = self.calls[output]
        target = _Target(state, lease, batch_id)
        arguments = self._arguments(call, json.loads(rows[0]["destination"]).get("arguments") or {})
        unknown = [row for row in rows if row["status"] == "in_flight"]
        pending = [row for row in rows if row["status"] != "in_flight"]
        if unknown and not call.operation.idempotent:
            pending += self._unknown(call, target, arguments=arguments, rows=unknown)
        else:
            pending += unknown
        ready, failed = [], []
        for row in sorted(pending, key=_sequence):
            payload, problem = _row_problem(call, row["payload"] or {})
            if problem is None:
                ready.append((row["event_sequence"], payload))
            else:
                failed.append((row["event_sequence"], FAILED, None, *problem))
        if failed:
            state.record_event_outcomes(lease, batch_id, output, failed)
        counts, results, started = {APPLIED: 0, FAILED: len(failed)}, list(failed), time.monotonic()
        chunks = range(0, len(ready), call.chunk_size)
        if ready:
            session, context = self._session(call)
            for number, start in enumerate(chunks, start=1):
                began = time.monotonic()
                outcomes = self._chunk(call, target, session=session, context=context, arguments=arguments,
                                       chunk=ready[start:start + call.chunk_size])
                results.extend(outcomes)
                for outcome in outcomes:
                    counts[outcome[1]] += 1
                _log_chunk(output, number, len(chunks), outcomes, time.monotonic() - began)
        total = self.counts.setdefault(output, {APPLIED: 0, FAILED: 0})
        for key, value in counts.items():
            total[key] += value
        log("applied", job_id=batch_id, output=output, rows=counts[APPLIED], failed=counts[FAILED], calls=len(chunks),
            duration_ms=milliseconds(time.monotonic() - started))
        if call.log is not None:
            sent = {row["event_sequence"]: row["payload"] for row in pending}
            log_outcomes(call.log, [(sent.get(outcome[0]), outcome) for outcome in sorted(results, key=_sequence_of)],
                         row_values=self.log_row_values, redact=self.redact, constants={"batch.id": batch_id})
        if counts[FAILED] and call.on_row_error == "fail":
            raise RowsRejectedError("output %s of batch %s: %d row(s) were rejected (on_row_error: fail); `state show "
                                    "JOB %s` lists them" % (output, batch_id, counts[FAILED], batch_id), batch_id)

    def _arguments(self, call, pinned):
        """The pinned, rendered arguments as their declared types, checked like a read's (a text "false" fails)."""
        arguments = resolve_arguments(call.operation, pinned)
        problems = ["%s: %s" % (issue.path, issue.message) if issue.path else issue.message
                    for issue in check_arguments(call.definition, call.operation, arguments, APPLY_CAPABILITIES,
                                                 rendered=True) if issue.severity == "error"]
        if problems:
            raise ApplyError(self.redact("output %s: %s" % (call.output, "; ".join(problems))))
        return arguments

    def _chunk(self, call, target, *, session, context, arguments, chunk):
        state, lease, batch_id = target
        sequences = [number for number, _ in chunk]
        state.mark_events_in_flight(lease, batch_id, call.output, sequences)       # write-ahead
        try:
            results = session.apply(call.operation, arguments, [payload for _, payload in chunk], context)
        except PartialApplyError as exc:
            self._record_partial(call, target, sequences, exc)
            raise ApplyError(self._cut_short(call, batch_id, len(chunk) - len(exc.results), exc)) from None
        except ConnectorError as exc:
            if exc.retryable or context.retryable(exc):
                raise ApplyError(self._cut_short(call, batch_id, len(chunk), exc)) from None
            text = self.redact(str(exc))[:ERROR_TEXT_LIMIT]
            outcomes = [(number, FAILED, None, None if exc.code is None else str(exc.code), text)
                        for number in sequences]
        except Exception as exc:
            raise ApplyError(self._cut_short(call, batch_id, len(chunk), exc)) from None
        else:
            if len(results) != len(chunk) or not all(isinstance(item, RowResult) for item in results):
                raise ApplyError("connector %s returned %d results for %d rows: the rows stay in flight" % (
                    call.definition.name, len(results), len(chunk)))
            outcomes = [self._outcome(number, item) for number, item in zip(sequences, results, strict=True)]
        state.record_event_outcomes(lease, batch_id, call.output, outcomes)
        return outcomes

    def _record_partial(self, call, target, sequences, exc):
        """The outcomes of a cut-short chunk's rows sent before the cut (the rest stay in flight)."""
        state, lease, batch_id = target
        if len(exc.results) >= len(sequences) or not all(isinstance(item, RowResult) for item in exc.results):
            raise ApplyError("connector %s returned %d partial results for %d rows: the rows stay in flight" % (
                call.definition.name, len(exc.results), len(sequences)))
        done = [self._outcome(number, item) for number, item in zip(sequences, exc.results, strict=False)]
        if done:
            state.record_event_outcomes(lease, batch_id, call.output, done)

    def _outcome(self, number, item):
        message = None if item.error_message is None else self.redact(item.error_message)[:ERROR_TEXT_LIMIT]
        result = None if item.result is None else json.loads(self.redact(json.dumps(item.result, default=str)))
        return number, item.status, result, item.error_code, message

    def _cut_short(self, call, batch_id, count, exc):
        return self.redact("output %s of batch %s: a call of %d row(s) did not complete (%s: %s); the rows stay in "
                           "flight and the next run %s" % (
                               call.output, batch_id, count, type(exc).__name__, exc,
                               "sends them again" if call.operation.idempotent else
                               "holds them (on_unknown: %s)" % call.on_unknown))[:ERROR_TEXT_LIMIT]

    def _unknown(self, call, target, *, arguments, rows):
        """In-flight rows of a non-idempotent operation: held, or reconciled (returns those to send again)."""
        state, lease, batch_id = target
        if call.on_unknown != "reconcile":
            raise ApplyError("output %s of batch %s: %d row(s) were being applied when a run stopped and %s is not "
                             "idempotent, so their outcome is unknown: check them in the vendor, then `state resend "
                             "%s` (send them again) or `state skip %s` (mark them failed)" % (
                                 call.output, batch_id, len(rows), call.operation.name, batch_id, batch_id))
        session, context = self._session(call)
        if not isinstance(session, ReconcilingSession):
            raise ApplyError("output %s: on_unknown: reconcile, but connector %s cannot reconcile; use on_unknown: "
                             "hold" % (call.output, call.definition.name))
        prepared = [(row, _row_problem(call, row["payload"] or {})[0] or dict(row["payload"] or {}))
                    for row in sorted(rows, key=_sequence)]
        found = session.reconcile(call.operation, arguments, [payload for _, payload in prepared], context)
        if len(found) != len(prepared):
            raise ApplyError("connector %s reconciled %d of %d rows" % (call.definition.name, len(found),
                                                                        len(prepared)))
        settled = [self._outcome(row["event_sequence"], item) for (row, _), item in zip(prepared, found, strict=True)
                   if item is not None]
        if settled:
            state.record_event_outcomes(lease, batch_id, call.output, settled)
        return [row for (row, _), item in zip(prepared, found, strict=True) if item is None]
