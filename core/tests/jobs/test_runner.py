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

"""Crash-window tests of the shared Runner with a SQLite store and fakes that implement the contracts (TST-01)."""

import time

import duckdb
import pytest

from streamwright.core.config.inputs import InputError
from streamwright.core.jobs.locking import FailedJobPolicy, LockPlan
from streamwright.core.jobs.runner import BrokerCall, Runner
from streamwright.core.jobs.state import LeaseHeldError, open_sqlite, open_store
from streamwright.core.jobs.types import Broker, ExecutionResult, Executor, Message, WriteReceipt


class Crash(BaseException):
    """Stands in for the process dying: nothing in the runner may handle it."""


class FakeExecutor:
    """Writes `outputs` in order; `fail_at` raises (or crashes) right before that output's write."""

    def __init__(self, outputs=("a", "b"), fail_at=None, error=None, events=None, during=None):
        self.outputs, self.fail_at, self.events, self.during = outputs, fail_at, events, during
        self.error = RuntimeError("boom") if error is None else error
        self.calls, self.written, self.pins = 0, [], []

    def execute(self, context, manifest, done, hooks):
        self.calls += 1
        manifest["pinned"].setdefault("listing", "pinned-%d" % self.calls)
        self.pins.append(manifest["pinned"]["listing"])
        hooks.save_manifest(manifest)
        if self.during:
            self.during(context)
        for name in self.outputs:
            if name in done:
                continue
            if name == self.fail_at:
                raise self.error
            hooks.fence()
            self.written.append(name)
            hooks.record_receipt(WriteReceipt(name, "t_" + name, 1), manifest)
        return ExecutionResult(events=self.events or {})


class FakeBroker(Broker):
    supports_watermarks = False

    def __init__(self, messages=(), publish_error=None, ack_error=None):
        self.queue, self.publish_error, self.ack_error = list(messages), publish_error, ack_error
        self.acked, self.published = [], []

    def poll(self, arguments, limit, max_wait_seconds, watermarks=None):
        taken, self.queue = self.queue[:limit], self.queue[limit:]
        return taken

    def ack(self, messages, arguments):
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.extend(m.message_id for m in messages)

    def publish(self, arguments, events, dedupe_key):
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((dedupe_key, list(events)))

    def destination(self, arguments):
        return arguments.get("QueueUrl")


def _message(number):
    return Message("m%d" % number, None, "{}", {}, None, receipt="r%d" % number)


@pytest.fixture
def state(tmp_path):
    store = open_sqlite(str(tmp_path / "state.db"))
    store.initialize()
    return store


def _run(state, executor, **kwargs):
    run_args = {k: kwargs.pop(k) for k in ("mode", "job_key", "max_messages") if k in kwargs}
    runner = Runner("job", state, executor, **kwargs)
    return runner, runner.run(**run_args)


def test_fakes_implement_the_contracts():
    assert isinstance(FakeExecutor(), Executor)
    FakeBroker()


def test_a_batch_writes_every_output_and_completes(state):
    executor = FakeExecutor()
    runner, code = _run(state, executor)
    assert code == 0 and executor.written == ["a", "b"]
    batch = state.get_batch("job", runner.processed[0])
    assert batch["status"] == "completed" and batch["manifest"]["pinned"] == {"listing": "pinned-1"}
    assert sorted(state.get_receipts("job", batch["batch_id"])) == ["a", "b"]


def test_a_failure_keeps_the_receipts_and_retry_writes_only_the_rest(state):
    first = FakeExecutor(fail_at="b")
    runner, code = _run(state, first)
    assert code == 1 and "boom" in runner.error and first.written == ["a"]
    batch_id = runner.processed[0]
    assert (state.get_batch("job", batch_id)["status"], state.get_batch("job", batch_id)["failed_from"]) == (
        "failed", "processing")
    blocked, code = _run(state, FakeExecutor(), failed_jobs=FailedJobPolicy("block"))
    assert code == 1 and "not retried (failed job policy block): `streamwright state retry|abandon" in blocked.error
    lease = state.acquire_lease("job", "operator", 60)
    state.retry_batch(lease, batch_id)
    state.release_lease(lease)
    second = FakeExecutor()
    _, code = _run(state, second, max_messages=0, failed_jobs=FailedJobPolicy("block"))
    assert code == 0 and second.written == ["b"]
    assert state.get_batch("job", batch_id)["status"] == "completed"
    assert state.get_batch("job", batch_id)["attempts"] == 2


def test_by_default_a_run_abandons_a_failed_batch_keeping_its_error_then_runs_new_work(state, caplog):
    runner, _ = _run(state, FakeExecutor(fail_at="b"))
    failed = runner.processed[0]
    second = FakeExecutor()
    runner, code = _run(state, second)
    assert code == 0 and runner.processed == ["job-00000002"] and second.written == ["a", "b"]
    abandoned = state.get_batch("job", failed)
    assert (abandoned["status"], abandoned["error"], abandoned["attempts"]) == ("abandoned", "RuntimeError: boom", 1)
    assert "abandoned: job_id=job-00000001, reason=failed job policy abandon, error=RuntimeError: boom" in caplog.text


def test_an_abandoned_keyed_batch_says_its_key_will_not_run_again(state, caplog):
    _run(state, FakeExecutor(fail_at="b"), job_key="day-1")
    runner, code = _run(state, FakeExecutor(), job_key="day-1")
    assert code == 0 and runner.processed == []
    assert "job_key=day-1 (a later run with this key does nothing)" in caplog.text


def test_a_run_retries_a_failed_batch_from_where_it_failed_then_runs_new_work(state, caplog):
    first = FakeExecutor(fail_at="b")
    runner, _ = _run(state, first)
    failed = runner.processed[0]
    second = FakeExecutor()
    runner, code = _run(state, second, failed_jobs=FailedJobPolicy("retry"))
    assert code == 0 and runner.processed == [failed, "job-00000002"]
    assert second.written == ["b", "a", "b"] and second.pins[0] == "pinned-1"   # the failed batch kept its pins
    assert state.get_batch("job", failed)["status"] == "completed"
    assert state.get_batch("job", failed)["attempts"] == 2
    assert "retry: job_id=job-00000001, attempt=2 of 3, resumes_from=processing, error=RuntimeError: boom" in (
        caplog.text)


def test_a_failed_batch_is_retried_up_to_max_job_attempts_then_blocks(state):
    policy = FailedJobPolicy("retry", 2)
    runner, code = _run(state, FakeExecutor(fail_at="b"), failed_jobs=policy)
    failed = runner.processed[0]
    runner, code = _run(state, FakeExecutor(fail_at="b"), failed_jobs=policy)   # attempt 2 fails too
    assert code == 1 and runner.processed == [failed]
    untouched = FakeExecutor()
    runner, code = _run(state, untouched, failed_jobs=policy)
    assert code == 1 and untouched.calls == 0 and runner.processed == []
    assert "not retried (started 2 time(s), max job attempts 2)" in runner.error
    assert state.get_batch("job", failed)["attempts"] == 2


def test_a_crash_resumes_with_the_pinned_values_saved_before_the_first_write(state):
    crashing = FakeExecutor(fail_at="b", error=Crash())
    with pytest.raises(Crash):
        _run(state, crashing)
    resumed = FakeExecutor()
    _, code = _run(state, resumed, mode="drain", max_messages=0)
    assert code == 0 and resumed.written == ["b"]
    assert resumed.pins == ["pinned-1"]                      # the replay saw the first attempt's pins


def test_a_lost_lease_fences_the_next_write(state):
    def take_over(context):
        time.sleep(1.2)
        state.acquire_lease("job", "intruder", 300)

    executor = FakeExecutor(during=take_over)
    runner, code = _run(state, executor, lease_seconds=1, heartbeat_seconds=60)
    assert code == 1 and executor.written == [] and "taken over or has expired" in runner.error
    assert state.get_lease("job")["owner_id"] == "intruder"


def test_the_heartbeat_keeps_the_lease_during_a_long_batch(state):
    blocked = []

    def try_take_over(context):
        time.sleep(1.5)
        with pytest.raises(LeaseHeldError):
            state.acquire_lease("job", "intruder", 300)
        blocked.append(True)

    executor = FakeExecutor(during=try_take_over)
    _, code = _run(state, executor, lease_seconds=1, heartbeat_seconds=0.2)
    assert code == 0 and blocked and executor.written == ["a", "b"]


def test_a_job_key_runs_once(state):
    first = FakeExecutor()
    assert _run(state, first, job_key="2026-11-01")[1] == 0
    second = FakeExecutor()
    assert _run(state, second, job_key="2026-11-01")[1] == 0
    assert second.calls == 0 and len(state.list_batches("job")) == 1
    with pytest.raises(InputError, match=r"the job key .* is 192 characters: at most 191"):
        _run(state, FakeExecutor(), job_key="k" * 192)


def test_events_are_committed_with_the_batch_then_acked_then_relayed(state):
    broker = FakeBroker([_message(1), _message(2)])
    executor = FakeExecutor(outputs=("a",), events={"notify": [{"n": 1}]})
    runner, code = _run(state, executor, consumer=BrokerCall(broker, {}),
                        publishers={"notify": BrokerCall(broker, {"QueueUrl": "https://queue/q1"})})
    assert code == 0 and broker.acked == ["m1", "m2"]
    assert broker.published == [("job:%s:notify" % runner.processed[0], [{"n": 1}])]
    assert state.list_pending_events("job") == []


def test_a_relay_failure_leaves_the_batch_acked_and_the_next_run_relays_it(state):
    broker = FakeBroker([_message(1)], publish_error=ConnectionError("broker down"))
    publishers = {"notify": BrokerCall(broker, {"QueueUrl": "q"})}
    runner, code = _run(state, FakeExecutor(outputs=(), events={"notify": [{"n": 1}]}),
                        consumer=BrokerCall(broker, {}), publishers=publishers)
    batch_id = runner.processed[0]
    assert code == 1 and "broker down" in runner.error
    assert state.get_batch("job", batch_id)["status"] == "acked" and broker.acked == ["m1"]
    broker.publish_error = None
    again = FakeExecutor()
    _, code = _run(state, again, consumer=BrokerCall(broker, {}), publishers=publishers)
    assert code == 0 and again.calls == 0 and broker.acked == ["m1"]
    assert state.get_batch("job", batch_id)["status"] == "completed" and len(broker.published) == 1


def test_an_ack_failure_retries_from_committed_without_executing_again(state):
    broker = FakeBroker([_message(1)], ack_error=TimeoutError("ack timed out"))
    first = FakeExecutor(outputs=("a",))
    runner, code = _run(state, first, consumer=BrokerCall(broker, {}))
    batch_id = runner.processed[0]
    assert code == 1 and state.get_batch("job", batch_id)["failed_from"] == "committed"
    lease = state.acquire_lease("job", "operator", 60)
    assert state.retry_batch(lease, batch_id) == "committed"
    state.release_lease(lease)
    broker.ack_error = None
    second = FakeExecutor(outputs=("a",))
    _, code = _run(state, second, consumer=BrokerCall(broker, {}), max_messages=0)
    assert code == 0 and second.calls == 0 and broker.acked == ["m1"]


def test_a_publishing_output_without_a_destination_fails_the_batch_before_commit(state):
    broker = FakeBroker([_message(1)])
    runner, code = _run(state, FakeExecutor(outputs=(), events={"notify": [{"n": 1}]}),
                        consumer=BrokerCall(broker, {}), publishers={"notify": BrokerCall(broker, {})})
    assert code == 1 and "no event destination" in runner.error and broker.acked == []
    assert state.get_batch("job", runner.processed[0])["failed_from"] == "processing"


def test_schema_signatures_are_stored_only_with_the_commit(state):
    class Signing(FakeExecutor):
        def execute(self, context, manifest, done, hooks):
            result = super().execute(context, manifest, done, hooks)
            return ExecutionResult(events=result.events, schema_signatures=[("s3://in/", "h1", "a,b")])

    _, code = _run(state, Signing(events={"unrouted": [{"id": 1}]}))
    assert code == 1 and state.get_schema_signatures("job") == {}
    lease = state.acquire_lease("job", "operator", 60)
    state.retry_batch(lease, state.list_batches("job")[0]["batch_id"])
    state.release_lease(lease)
    _, code = _run(state, Signing(), mode="drain", max_messages=0)
    assert code == 0 and state.get_schema_signatures("job") == {"s3://in/": "h1"}


def test_errors_are_redacted_before_they_are_stored(state):
    runner, code = _run(state, FakeExecutor(fail_at="a", error=RuntimeError("password=hunter22")),
                        redact=lambda text: text.replace("hunter22", "***"))
    stored = state.get_batch("job", runner.processed[0])["error"]
    assert code == 1 and "hunter22" not in stored and "***" in stored and "hunter22" not in runner.error


def test_the_carry_section_follows_the_last_completed_batch(state):
    class Carrying(FakeExecutor):
        def execute(self, context, manifest, done, hooks):
            self.seen = dict(manifest["carry"])
            manifest["carry"]["bookmark"] = self.seen.get("bookmark", 0) + 1
            return super().execute(context, manifest, done, hooks)

    first, second = Carrying(), Carrying()
    _run(state, first)
    failed_runner, _ = _run(state, Carrying(fail_at="a"))  # a failed batch carries nothing on
    lease = state.acquire_lease("job", "op", 60)
    state.record_batch(lease, failed_runner.processed[0], "abandoned", {})
    state.release_lease(lease)
    _run(state, second)
    assert first.seen == {} and second.seen == {"bookmark": 1}


def test_the_heartbeat_works_on_a_duckdb_store_shared_with_the_job(tmp_path):

    con = duckdb.connect(str(tmp_path / "job.duckdb"))
    state_con = con.cursor()
    store = open_store(state_con.cursor, "duckdb", catalog="job")
    store.initialize()

    def busy(context):
        for _ in range(15):
            con.execute("SELECT count(*) FROM range(100000)").fetchall()
            time.sleep(0.1)

    runner, code = _run(store, FakeExecutor(during=busy), lease_seconds=1, heartbeat_seconds=0.2)
    assert code == 0 and store.get_batch("job", runner.processed[0])["status"] == "completed"


def test_a_transient_conflict_on_lease_renewal_is_retried_not_a_lost_lease(tmp_path):

    con = duckdb.connect(str(tmp_path / "job.duckdb"))
    store = open_store(con.cursor().cursor, "duckdb", catalog="job")
    store.initialize()

    def hold_the_lease_row(context):         # a contending acquire_lease that rolls back: renewals conflict meanwhile
        other = con.cursor()
        other.execute("BEGIN TRANSACTION")
        other.execute("UPDATE job.streamwright.sw_leases SET expires_at = expires_at")
        time.sleep(0.7)
        other.execute("ROLLBACK")
        time.sleep(0.5)

    logged = []
    runner, code = _run(store, FakeExecutor(during=hold_the_lease_row), lease_seconds=3, heartbeat_seconds=0.2,
                        log=lambda event, **fields: logged.append(event))
    assert code == 0, runner.error
    assert store.get_batch("job", runner.processed[0])["status"] == "completed"
    assert "lease_renewal_retried" in logged


def _lock(policy="prune", scope="", items=("1", "2", "3")):
    return LockPlan(scope, policy, 0.0, None, "accounts", tuple(items), tuple("acct:%s" % i for i in items),
                    {"accounts": list(items), "client": scope})


def _prepared(executors):
    def prepare(inputs):
        executors.append((inputs, FakeExecutor()))
        return executors[-1][1]
    return prepare


def test_prune_runs_the_free_items_and_pins_them(state):
    other = state.acquire_lease("job", "w", 300, tokens=["acct:3"])
    executors = []
    runner = Runner("job", state, lock=_lock(), prepare=_prepared(executors))
    assert runner.run() == 0
    (inputs, _), = executors
    assert inputs == {"scope": "", "config": {"accounts": ["1", "2"], "client": ""}, "tokens": ["acct:1", "acct:2"]}
    batch = state.get_batch("job", runner.processed[0])
    assert batch["manifest"]["inputs"] == inputs
    assert batch["tokens"] == {"accepted": ["acct:1", "acct:2"], "skipped": ["acct:3"]}
    state.release_lease(other)


def test_a_run_whose_items_are_all_held_is_recorded_as_skipped(state):
    other = state.acquire_lease("job", "w", 300, tokens=["acct:1", "acct:2"])
    runner = Runner("job", state, lock=_lock(items=("1", "2")), prepare=_prepared([]))
    assert runner.run() == 0 and runner.skipped and runner.processed == []
    skipped, = state.list_batches("job")
    assert (skipped["status"], skipped["tokens"]["skipped"]) == ("skipped", ["acct:1", "acct:2"])
    assert state.get_lease("job", "")["held"] is False                  # the scope's lease was released
    state.release_lease(other)


def test_a_held_scope_is_skipped_under_prune_and_fails_otherwise(state):
    other = state.acquire_lease("job", "w", 300, tokens=["scope:a"])
    runner = Runner("job", state, scope="a", lock=_lock(scope="a"), prepare=_prepared([]))
    assert runner.run() == 0 and runner.skipped
    runner = Runner("job", state, scope="a", lock=_lock("fail", scope="a"), prepare=_prepared([]))
    assert runner.run() == 1 and "scope:a" in runner.error
    state.release_lease(other)


def test_the_fail_policy_stops_on_a_held_item(state):
    other = state.acquire_lease("job", "w", 300, tokens=["acct:1"])
    runner = Runner("job", state, lock=_lock("fail"), prepare=_prepared([]))
    assert runner.run() == 1 and "acct:1" in runner.error
    state.release_lease(other)


def test_scopes_keep_their_own_batches(state):
    executors = []
    failing = Runner("job", state, scope="a", lock=_lock("fail", "a", ("1",)),
                     prepare=lambda inputs: FakeExecutor(fail_at="a"))
    assert failing.run() == 1
    runner = Runner("job", state, scope="b", lock=_lock("fail", "b", ("2",)), prepare=_prepared(executors))
    assert runner.run() == 0                                           # a's failed batch does not stop b
    assert executors[0][0]["config"]["accounts"] == ["2"]
    a, b = failing.processed[0], runner.processed[0]
    assert a != b and state.get_batch("job", a)["status"] == "failed"
    assert state.get_batch("job", b)["scope"] == "b" and state.get_batch("job", b)["batch_sequence"] == 1
    assert state.get_batch("job", a)["batch_sequence"] == 1


def test_a_resumed_batch_needs_its_pinned_tokens_and_runs_with_its_pinned_inputs(state):
    crashed = Runner("job", state, lock=_lock("fail", items=("1",)),
                     prepare=lambda inputs: FakeExecutor(fail_at="a", error=KeyboardInterrupt()))
    assert crashed.run() == 130                                        # left processing, its lease released
    other = state.acquire_lease("job", "w", 300, tokens=["acct:1"])
    executors = []
    later = Runner("job", state, lock=_lock("fail", items=("2",)), prepare=_prepared(executors))
    assert later.run() == 1 and "acct:1" in later.error and executors == []
    state.release_lease(other)
    assert later.run() == 0
    resumed, new = executors
    assert resumed[0]["config"]["accounts"] == ["1"] and new[0]["config"]["accounts"] == ["2"]
    assert state.get_batch("job", crashed.processed[0])["status"] == "completed"


def test_batch_sequences_are_real_and_ids_name_the_scope(state):
    first = Runner("job", state, FakeExecutor())
    second = Runner("job", state, FakeExecutor())
    assert first.run() == 0 and second.run() == 0
    assert first.processed == ["job-00000001"] and second.processed == ["job-00000002"]
    scoped = Runner("job", state, FakeExecutor(), scope="client:x")
    assert scoped.run() == 0
    assert scoped.processed[0].startswith("job-") and scoped.processed[0].endswith("-00000001")
