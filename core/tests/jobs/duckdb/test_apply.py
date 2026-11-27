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
"""Apply outputs: rows applied as a connector's operations, chunked, written ahead, recovered (plan §2)."""

import json
import logging
import sqlite3
from types import MappingProxyType

import pytest

from streamwright.core.cli import main as cli_main
from streamwright.core.jobs.apply import delivery
from streamwright.core.jobs.duckdb.engine import run_job
from streamwright.core.jobs.duckdb.plugin import JOB_ENGINE
from streamwright.core.jobs.duckdb.results import describe_output
from streamwright.core.jobs.state import open_sqlite
from streamwright.core.runtime import components
from streamwright.core.runtime.logs.context import stamp
from streamwright.core.sdk.apply import RowResult
from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, OperationKind
from streamwright.core.sdk.engines.source import ConnectorError


class Ledger:
    """The fake vendor: what each call received, and how the next calls behave."""

    def __init__(self):
        self.calls = []                 # (operation, arguments, rows)
        self.crash_on_call = None       # the call number that raises before returning (the run stops mid-call)
        self.reject_call = None         # the call number the vendor rejects as a whole
        self.transient_calls = 0        # the first calls time out (transient, outcome unknown)
        self.accept_all = False         # the vendor no longer rejects the rows marked `reject`

    def apply(self, operation, arguments, rows):
        self.calls.append((operation.name, dict(arguments), [dict(row) for row in rows]))
        if self.crash_on_call == len(self.calls):
            raise RuntimeError("connection reset")
        if self.reject_call == len(self.calls):
            raise ConnectorError("ledger: PERMISSION_DENIED", code="PERMISSION_DENIED")
        if len(self.calls) <= self.transient_calls:
            raise ConnectorError("ledger: DEADLINE_EXCEEDED", code="DEADLINE_EXCEEDED", retryable=True, retry_after=0)
        return [RowResult.failed("REJECTED", "rejected %s" % row["key"]) if row.get("reject") and not self.accept_all
                else RowResult.applied({"id": "e-%s" % row["key"]}) for row in rows]


class LedgerSession:
    def __init__(self, ledger):
        self.ledger = ledger

    def read(self, operation, arguments, context):
        return iter(())

    def apply(self, operation, arguments, rows, context):
        return context.call(self.ledger.apply, operation, arguments, rows)

    def close(self):
        pass


class LedgerAdapter:
    ledger = None

    def open(self, connection, context):
        assert connection["token"] == "tok-1"
        return LedgerSession(LedgerAdapter.ledger)


LEDGER_API = ConnectorDefinition(
    name="ledger_api", title="Ledger API", family="test",
    connection=(Field("token", "string", required=True, secret=True),),
    operations=(
        Operation(service="entries", method="mutate", kind=OperationKind.APPLY, idempotent=False, max_chunk=2,
                  actions=("create", "update", "remove"), destructive_actions=("remove",),
                  arguments=(Field("account", "string", required=True),)),
        Operation(service="conversions", method="upload", kind=OperationKind.APPLY, max_chunk=3,
                  actions=("upload",)),
        Operation(service="entries", method="list"),
    ),
    adapters={"source": __name__ + ":LedgerAdapter"})


@pytest.fixture
def ledger():
    LedgerAdapter.ledger = Ledger()
    components.register(LEDGER_API)
    yield LedgerAdapter.ledger
    components.unregister("ledger_api")
    LedgerAdapter.ledger = None


def _job(tmp_path, rows_sql, *, call=None, apply=None, **output):
    return {
        "kind": "DuckDBJob", "name": "ledger_sync",
        "spec": {"config": {"account": {"type": "string", "default": "acc-1"}},
                 "secrets": {"token": {"type": "string", "required": True}}},
        "connections": {"ledger": {"provider": "ledger_api", "token": "{{ secrets.token }}"}},
        "steps": [{"name": "entries", "sql": rows_sql}],
        "outputs": [{"name": "push", "connection": "ledger", "step": "entries",
                     "operation": {"service": "entries", "method": "mutate",
                                   "arguments": {"account": "{{ config.account }}"},
                                   **(call or {})},
                     **({"apply": apply} if apply else {}), **output}],
        "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}},
    }


FIVE = "SELECT k AS key, 'n' || k AS name FROM range(5) t(k) ORDER BY k"


def _state(tmp_path, action, *args):
    """`streamwright state ACTION --url ... ARGS` on the job's state database."""
    return cli_main(["state", action, "--url", "sqlite:///%s" % (tmp_path / "state.db"), *args])


def _outbox(tmp_path):
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        return [(row[0], row[1], row[2], json.loads(row[3]) if row[3] else None, row[4]) for row in con.execute(
            "SELECT sequence, status, attempts, result, error_code FROM sw_events ORDER BY job_id, sequence")]


def _run(job, **options):
    return run_job(job, secrets={"token": "tok-1"}, **options)


def _codes(job):
    return {(issue.code, issue.path) for issue in JOB_ENGINE.validate_job(job).issues}


def test_an_apply_output_validates_its_operation_keys_and_policies(tmp_path, ledger):
    assert JOB_ENGINE.validate_job(_job(tmp_path, FIVE, apply={"chunk_size": 2, "on_row_error": "continue"})).valid
    assert ("bad-value", "outputs[0]") in _codes(_job(tmp_path, FIVE, call={"method": "list"}))  # a read, not an apply
    assert ("bad-value", "outputs[0]") in _codes(_job(tmp_path, FIVE, apply={"chunk_size": 3}))  # over max_chunk
    assert ("unsupported-key", "outputs[0]") in _codes(_job(tmp_path, FIVE, mode="append"))
    assert ("bad-value", "outputs[0].apply.nulls") in _codes(_job(tmp_path, FIVE, apply={"nulls": "ignore"}))
    assert ("bad-call", "outputs[0]") in _codes(_job(tmp_path, FIVE, call={"arguments": {}}))  # account is required
    assert ("unsupported-argument", "outputs[0]") in _codes(_job(tmp_path, FIVE, policy={"timeout": "5s"}))
    flat = _job(tmp_path, FIVE)
    flat["outputs"][0]["chunk_size"] = 2                                      # the flat forms are not the grammar
    assert ("unknown-key", "outputs[0].chunk_size") in _codes(flat)
    both = _job(tmp_path, FIVE)
    both["outputs"][0]["sql"] = "SELECT 1"
    assert ("bad-value", "outputs[0]") in _codes(both)


def test_rows_are_applied_in_chunks_with_pinned_arguments_and_their_results_are_kept(tmp_path, ledger):
    result = _run(_job(tmp_path, FIVE), config={"account": "acc-9"})
    assert result.exit_code == 0, result.error
    assert [len(rows) for _, _, rows in ledger.calls] == [2, 2, 1]
    assert {arguments["account"] for _, arguments, _ in ledger.calls} == {"acc-9"}
    assert ledger.calls[0][2][0] == {"key": 0, "name": "n0", "op": "create"}
    assert [(status, attempts, result) for _, status, attempts, result, _ in _outbox(tmp_path)] == [
        ("applied", 1, {"id": "e-%d" % key}) for key in range(5)]
    assert result.outputs == {"push": 5}


def test_rejected_rows_and_row_checks_follow_on_row_error_and_nulls(tmp_path, ledger):
    sql = ("SELECT * FROM (VALUES (1, 'a', 'create', false), (2, NULL, 'create', false), (3, 'c', 'remove', false), "
           "(4, 'd', 'create', true), (5, 'e', 'drop', false)) t(key, name, op, reject)")
    failed = _run(_job(tmp_path, sql))
    assert failed.exit_code == 1 and "4 row(s) were rejected" in failed.error
    codes = {sequence: code for sequence, _, _, _, code in _outbox(tmp_path)}
    assert codes == {0: None, 1: "NULL_VALUE", 2: "REMOVE_NOT_ALLOWED", 3: "REJECTED", 4: "INVALID_ACTION"}
    assert [row["key"] for _, _, rows in ledger.calls for row in rows] == [1, 4]          # checked rows never sent

    ledger.calls.clear()
    lenient = _job(tmp_path / "b", sql, apply={"on_row_error": "continue", "nulls": "skip_field", "allow_remove": True})
    (tmp_path / "b").mkdir()
    assert _run(lenient).exit_code == 0
    sent = {row["key"]: row for _, _, rows in ledger.calls for row in rows}
    assert "name" not in sent[2] and sent[3]["op"] == "remove"


def test_rejected_rows_under_on_row_error_fail_fail_the_batch_for_the_failed_job_policy(tmp_path, ledger):
    job = _job(tmp_path, "SELECT 1 AS key, 'a' AS name, true AS reject")
    job["runtime"]["state"]["failed_job_policy"] = "block"
    assert _run(job).exit_code == 1
    assert [b["status"] for b in open_sqlite(str(tmp_path / "state.db")).list_batches("ledger_sync")] == ["failed"]
    blocked = _run(job)
    assert blocked.exit_code == 1 and "row(s) were rejected" in blocked.error
    assert len(ledger.calls) == 1


REJECTING = "SELECT k AS key, 'n' || k AS name, k IN (1, 3) AS reject FROM range(5) t(k) ORDER BY k"


def test_retrying_a_batch_of_rejected_rows_sends_only_its_failed_rows_again(tmp_path, ledger):
    job = _job(tmp_path, REJECTING)
    job["runtime"]["state"].update(failed_job_policy="retry", max_job_attempts=3)
    assert _run(job, job_key="sync-1").exit_code == 1
    again = _run(job, job_key="sync-1")                                    # attempt 2: rejected again, fails again
    assert again.exit_code == 1 and "2 row(s) were rejected" in again.error
    assert [(s, n) for _, s, n, _, _ in _outbox(tmp_path)] == [
        ("applied", 1), ("failed", 2), ("applied", 1), ("failed", 2), ("applied", 1)]
    ledger.accept_all = True
    assert _run(job, job_key="sync-1").exit_code == 0                      # attempt 3: applied
    assert [[row["key"] for row in rows] for _, _, rows in ledger.calls] == [[0, 1], [2, 3], [4], [1, 3], [1, 3]]
    assert [(s, n, code) for _, s, n, _, code in _outbox(tmp_path)] == [
        ("applied", 1, None), ("applied", 3, None), ("applied", 1, None), ("applied", 3, None), ("applied", 1, None)]
    assert [b["status"] for b in open_sqlite(str(tmp_path / "state.db")).list_batches("ledger_sync")] == ["completed"]


def test_abandoning_a_batch_of_rejected_rows_sends_nothing_again(tmp_path, ledger):
    job = _job(tmp_path, REJECTING)
    assert _run(job, job_key="sync-1").exit_code == 1
    ledger.accept_all = True
    assert _run(job, job_key="sync-1").exit_code == 0                      # failed_job_policy abandon (the default)
    assert len(ledger.calls) == 3
    assert [s for _, s, *_ in _outbox(tmp_path)] == ["applied", "failed", "applied", "failed", "applied"]
    assert [b["status"] for b in open_sqlite(str(tmp_path / "state.db")).list_batches("ledger_sync")] == ["abandoned"]


def test_a_call_the_vendor_rejects_fails_its_rows_without_holding_them(tmp_path, ledger):
    ledger.reject_call = 2
    result = _run(_job(tmp_path, FIVE, apply={"on_row_error": "continue"}))
    assert result.exit_code == 0, result.error
    assert [(status, code) for _, status, _, _, code in _outbox(tmp_path)] == [
        ("applied", None), ("applied", None), ("failed", "PERMISSION_DENIED"), ("failed", "PERMISSION_DENIED"),
        ("applied", None)]
    assert (result.outputs, result.rejected) == ({"push": 3}, {"push": 2})   # what a --summary and serve report
    assert result.apply_outputs == {"push"}
    assert describe_output("push", 3, result.rejected, result.apply_outputs) == "push 3 applied, 2 rejected"
    assert describe_output("table", 7, {}, result.apply_outputs) == "table 7 rows"


def test_state_list_shows_an_apply_output_s_rows_in_the_outbox_not_as_written(tmp_path, ledger, capsys):
    ledger.reject_call = 2
    assert _run(_job(tmp_path, FIVE, apply={"on_row_error": "continue"})).exit_code == 0
    capsys.readouterr()
    assert _state(tmp_path, "list") == 0
    header, row = capsys.readouterr().out.splitlines()
    assert "WRITTEN" in header and "OUTBOX" in header and "ROWS" not in header
    assert " 0        3 applied, 2 failed" in row          # nothing written to a table; the rows' outcomes
    assert _state(tmp_path, "list", "--json") == 0
    assert json.loads(capsys.readouterr().out)["outbox"] == {"applied": 3, "failed": 2}


def test_a_cut_short_call_is_held_for_a_non_idempotent_operation_then_resent_or_skipped(tmp_path, ledger, capsys):
    ledger.crash_on_call = 2
    job = _job(tmp_path, FIVE)
    first = _run(job, job_key="sync-1")                    # a job key: this work runs once; later runs recover it
    assert first.exit_code == 1 and "did not complete" in first.error
    assert [status for _, status, *_ in _outbox(tmp_path)] == ["applied", "applied", "in_flight", "in_flight",
                                                              "pending"]
    held = _run(job, job_key="sync-1")
    assert held.exit_code == 1 and "outcome is unknown" in held.error
    assert "`streamwright state resend --url URL ledger_sync-00000001`" in held.error
    assert len(ledger.calls) == 2                                          # nothing sent while held

    batch = open_sqlite(str(tmp_path / "state.db")).list_batches("ledger_sync")[0]["batch_id"]
    assert _state(tmp_path, "show", batch) == 0
    shown = json.loads(capsys.readouterr().out)
    assert [row["status"] for row in shown["outbox"]] == ["applied", "applied", "in_flight", "in_flight", "pending"]
    assert _state(tmp_path, "resend", batch) == 0
    assert "2 held row(s) will be sent again" in capsys.readouterr().out
    resumed = _run(job, job_key="sync-1")
    assert resumed.exit_code == 0, resumed.error
    assert [[row["key"] for row in rows] for _, _, rows in ledger.calls[2:]] == [[2, 3], [4]]
    assert _run(job, job_key="sync-1").exit_code == 0 and len(ledger.calls) == 4         # done: nothing re-applied
    assert [status for _, status, *_ in _outbox(tmp_path)] == ["applied"] * 5


def test_a_cut_short_call_of_an_idempotent_operation_is_sent_again(tmp_path, ledger):
    ledger.crash_on_call = 1
    job = _job(tmp_path, FIVE)
    job["outputs"][0]["operation"] = {"service": "conversions", "method": "upload", "arguments": {}}
    job["steps"][0]["sql"] = "SELECT key, name FROM (%s)" % FIVE
    assert _run(job, job_key="upload-1").exit_code == 1
    assert _run(job, job_key="upload-1").exit_code == 0
    assert [[row["key"] for row in rows] for _, _, rows in ledger.calls] == [[0, 1, 2], [0, 1, 2], [3, 4]]
    assert [(status, attempts) for _, status, attempts, _, _ in _outbox(tmp_path)] == [
        ("applied", 2)] * 3 + [("applied", 1)] * 2


def test_a_non_idempotent_call_is_never_retried_in_the_run(tmp_path, ledger):
    ledger.transient_calls = 5
    job = _job(tmp_path, FIVE)
    retrying = dict(job, outputs=[dict(job["outputs"][0], policy={"retry": {"max_attempts": 4}})])
    report = JOB_ENGINE.validate_job(retrying)
    assert report.valid and any("is not idempotent, so a call is sent once" in i.message for i in report.issues)
    result = _run(job)
    assert result.exit_code == 1 and "did not complete" in result.error
    assert len(ledger.calls) == 1
    assert [status for _, status, *_ in _outbox(tmp_path)][:2] == ["in_flight", "in_flight"]


def test_the_rate_limit_of_an_apply_output_holds_across_the_batches_a_run_relays(tmp_path, ledger, monkeypatch):
    limiters = []

    class Limiter(delivery.RateLimiter):
        def wait(self):
            limiters.append(self)

    monkeypatch.setattr(delivery, "RateLimiter", Limiter)
    ledger.crash_on_call = 1
    job = _job(tmp_path, FIVE, policy={"rate_limit": {"requests": 100, "per": "1s"}})
    job["outputs"][0]["operation"] = {"service": "conversions", "method": "upload", "arguments": {}}
    job["steps"][0]["sql"] = "SELECT key, name FROM (%s)" % FIVE
    assert _run(job).exit_code == 1
    limiters.clear()
    assert _run(job).exit_code == 0                         # relays the first batch, then runs and relays a second
    assert len(limiters) == 4 and len({id(limiter) for limiter in limiters}) == 1


def test_the_apply_rows_cap_stops_a_runaway_batch_before_any_call(tmp_path, ledger):
    job = _job(tmp_path, FIVE)
    job["runtime"]["limits"] = {"apply_rows": 4}
    result = _run(job)
    assert result.exit_code == 1 and "runtime.limits.apply_rows" in result.error
    assert ledger.calls == []


class _Tags(logging.Handler):
    """The text tags of the lines logged while it is attached (stamped as the CLI's record factory does)."""

    def __init__(self):
        super().__init__(logging.INFO)
        self.tags = []

    def emit(self, record):
        stamp(record)
        self.tags.append(record.context_tag)


def test_a_run_names_itself_in_its_log_lines_and_batches(tmp_path, ledger):
    handler, logger = _Tags(), logging.getLogger("streamwright")
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        assert _run(_job(tmp_path, FIVE), request_id="sw_test-request-1").exit_code == 0
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
    assert any(tag.endswith("req=sw_test-request-1]") for tag in handler.tags)
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        assert json.loads(con.execute("SELECT metadata FROM sw_jobs").fetchone()[0])["request_id"] == \
            "sw_test-request-1"
    from_cli = _run(_job(tmp_path / "x", FIVE)) if (tmp_path / "x").mkdir() is None else None
    with sqlite3.connect(str(tmp_path / "x" / "state.db")) as con:
        minted = json.loads(con.execute("SELECT metadata FROM sw_jobs").fetchone()[0])["request_id"]
    assert from_cli.exit_code == 0 and minted.startswith("sw_") and len(minted) == 25


def test_skip_marks_held_rows_failed_and_the_next_run_carries_on(tmp_path, ledger, capsys):
    ledger.crash_on_call = 1
    job = _job(tmp_path, FIVE, apply={"on_row_error": "continue"})
    assert _run(job, job_key="sync-2").exit_code == 1
    batch = open_sqlite(str(tmp_path / "state.db")).list_batches("ledger_sync")[0]["batch_id"]
    assert _state(tmp_path, "skip", batch, "--output", "push") == 0
    assert _run(job, job_key="sync-2").exit_code == 0
    assert [(status, code) for _, status, _, _, code in _outbox(tmp_path)] == [
        ("failed", "UNKNOWN_OUTCOME")] * 2 + [("applied", None)] * 3


REJECTS = "SELECT k AS key, 'n' || k AS name, k % 2 = 1 AS reject FROM range(5) t(k) ORDER BY k"
FAILED_ROWS = MappingProxyType({
    "sql": "SELECT outcome.row AS row, key, outcome.error_code AS code, outcome.error_message AS error FROM push "
           "WHERE outcome.status = 'failed' ORDER BY outcome.row",
    "message": "row {row} (key {key}) of {{ batch.id }}: {code}: {error}", "level": "warning"})


def _output_lines(caplog):
    """The lines of an output's `log:` (not its calls' timings)."""
    return [record.getMessage() for record in caplog.records
            if record.name == "streamwright.output" and getattr(record, "event", None) != "apply_call"]


def test_an_apply_outputs_log_selects_from_its_results_once_the_rows_are_sent(tmp_path, ledger, caplog):
    caplog.set_level(logging.DEBUG, logger="streamwright.output")
    result = _run(_job(tmp_path, REJECTS, apply={"on_row_error": "continue"}, log=dict(FAILED_ROWS)))
    assert result.exit_code == 0, result.error
    batch = result.batches[0]
    assert _output_lines(caplog) == ["output push: row 2 (key 1) of %s: REJECTED: rejected 1" % batch,
                                     "output push: row 4 (key 3) of %s: REJECTED: rejected 3" % batch]
    calls = [record for record in caplog.records if getattr(record, "event", None) == "apply_call"]
    assert [record.getMessage().rsplit(",", 1)[0] for record in calls] == [
        "output push: call 1/3: 2 row(s), 1 applied, 1 failed", "output push: call 2/3: 2 row(s), 1 applied, 1 failed",
        "output push: call 3/3: 1 row(s), 1 applied, 0 failed"]                  # then the call's seconds
    assert all(record.duration_ms >= 0 for record in calls)
    # without sql, every row; a placeholder reaches into the outcome and what the vendor returned
    caplog.clear()
    (tmp_path / "b").mkdir()
    every = {"message": "{key}: {outcome.status} {outcome.result.id}", "max_rows": 3}
    assert _run(_job(tmp_path / "b", REJECTS, apply={"on_row_error": "continue"}, log=every)).exit_code == 0
    assert _output_lines(caplog) == ["output push: 0: applied e-0", "output push: 1: failed null",
                                     "output push: 2: applied e-2", "output push: ... and 2 more row(s)"]


def test_an_apply_outputs_log_runs_before_rejected_rows_fail_the_batch_and_never_fails_a_run(tmp_path, ledger, caplog):
    caplog.set_level(logging.INFO, logger="streamwright.output")
    failed = _run(_job(tmp_path, REJECTS, log=dict(FAILED_ROWS)))
    assert failed.exit_code == 1 and "2 row(s) were rejected" in failed.error
    assert len(_output_lines(caplog)) == 2
    caplog.clear()
    (tmp_path / "b").mkdir()
    reading = {"sql": "SELECT * FROM read_csv('%s')" % (tmp_path / "state.db"), "message": "{key}"}
    assert _run(_job(tmp_path / "b", REJECTS, apply={"on_row_error": "continue"}, log=reading)).exit_code == 0
    assert _output_lines(caplog)[0].startswith("output push: its log: cannot run: Permission Error")
    caplog.clear()
    (tmp_path / "c").mkdir()
    served = _run(_job(tmp_path / "c", REJECTS, apply={"on_row_error": "continue"}, log=dict(FAILED_ROWS)),
                  log_row_values=False)
    assert served.exit_code == 0
    assert _output_lines(caplog) == ["output push: 2 row(s) (row values are not logged by streamwright serve)"]


def test_only_an_apply_output_takes_a_log_and_its_sql_is_one_select(tmp_path, ledger):
    assert JOB_ENGINE.validate_job(_job(tmp_path, FIVE, log=dict(FAILED_ROWS))).valid
    deleting = dict(FAILED_ROWS, sql="DELETE FROM push")
    assert ("bad-value", "outputs[0].log") in _codes(_job(tmp_path, FIVE, log=deleting))
    assert ("bad-value", "outputs[0].log") in _codes(_job(tmp_path, FIVE, log={"message": "{"}))
    job = _job(tmp_path, FIVE)
    job["connections"]["out"] = {"provider": "files", "roots": [str(tmp_path)]}
    job["outputs"].append({"name": "copy", "connection": "out", "step": "entries", "path": "copy", "format": "csv",
                           "mode": "overwrite", "log": {"message": "{key}"}})
    assert ("unsupported-key", "outputs[1].log") in _codes(job)
