"""Crash-window tests of the shared Runner with a SQLite store and fakes that implement the contracts (TST-01)."""

import time

import duckdb
import pytest

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
    run_args = {k: kwargs.pop(k) for k in ("mode", "run_key", "max_messages") if k in kwargs}
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
    blocked, code = _run(state, FakeExecutor())
    assert code == 1 and "retry or abandon" in blocked.error
    lease = state.acquire_lease("job", "operator", 60)
    state.retry_batch(lease, batch_id)
    state.release_lease(lease)
    second = FakeExecutor()
    _, code = _run(state, second, max_messages=0)
    assert code == 0 and second.written == ["b"]
    assert state.get_batch("job", batch_id)["status"] == "completed"


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


def test_a_run_key_runs_once(state):
    first = FakeExecutor()
    assert _run(state, first, run_key="2026-11-01")[1] == 0
    second = FakeExecutor()
    assert _run(state, second, run_key="2026-11-01")[1] == 0
    assert second.calls == 0 and len(state.list_batches("job")) == 1


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
    _run(state, Carrying(fail_at="a"))                    # a failed batch carries nothing on
    lease = state.acquire_lease("job", "op", 60)
    state.record_batch(lease, "job-00000002", "abandoned", {})
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
        other.execute("UPDATE job.streamwright.job_leases SET expires_at = expires_at")
        time.sleep(0.7)
        other.execute("ROLLBACK")
        time.sleep(0.5)

    logged = []
    runner, code = _run(store, FakeExecutor(during=hold_the_lease_row), lease_seconds=3, heartbeat_seconds=0.2,
                        log=lambda event, **fields: logged.append(event))
    assert code == 0, runner.error
    assert store.get_batch("job", runner.processed[0])["status"] == "completed"
    assert "lease_renewal_retried" in logged
