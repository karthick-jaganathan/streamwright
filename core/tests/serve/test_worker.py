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

"""`streamwright worker`: claims, takeovers after a worker dies, timeouts, and the run queue in the state store."""

import contextlib
import datetime
import json
import logging
import multiprocessing
import os
import signal
import threading
import time
import types
import uuid
from types import MappingProxyType

import pytest

from streamwright.core.jobs.duckdb.results import RunResult
from streamwright.core.jobs.state.requests import RateLimitedError
from streamwright.core.jobs.state.url import open_url
from streamwright.core.serve import cli as serve_cli
from streamwright.core.serve import worker as worker_module
from streamwright.core.serve.worker import Worker, execute_run, run_summary


def _enqueue(service, config, principal="crm", **extra):
    run, created = service.store.enqueue_request(job_name="client_feed", principal=principal, body_sha256="0" * 64,
                                                 inputs=config, request_id="sw_test", **extra)
    assert created
    return run["run_id"]


def _config(client="1001", day="2026-11-16"):
    return {"tenant_id": "t1", "client_id": client, "feed_date": day}


def test_a_dead_workers_run_is_taken_over_and_resumed_without_running_twice(feed):
    service = feed.service()
    feed.write_feed("t1", "1001")
    run_id = _enqueue(service, _config(), idempotency_key="order-1", scope_key="client:t1/1001")
    executed = []

    def dies_after_the_run(manifest, options, timeout, log_settings, lost=None):
        executed.append(options["job_key"])
        execute_run(manifest, options)          # the batch completes ...
        raise SystemExit                        # ... and the worker dies before recording the run

    with pytest.raises(SystemExit):
        feed.worker(service, "w1", claim_seconds=1, execute=dies_after_the_run).run_one()
    assert service.store.get_request(run_id)["status"] == "running"
    assert feed.worker(service, "w2", claim_seconds=1).run_one() is None      # w1's claim has not ended yet
    time.sleep(1.2)
    second = feed.worker(service, "w2")
    assert second.run_one() == run_id
    run = service.store.get_request(run_id)
    assert (run["status"], run["worker_id"], run["attempts"]) == ("succeeded", "w2", 2)
    assert executed == ["serve:crm:order-1"]
    job_state = open_url("sqlite:///%s" % (feed.folder / "state.db"))
    batches = job_state.list_batches("client_feed")
    assert len(batches) == 1 and batches[0]["job_key"] == "serve:crm:order-1"     # the takeover did not run it again


def test_a_served_run_logs_a_steps_number_of_rows_never_their_values(feed, caplog):
    job = feed.job()
    job["steps"][0]["log"] = {"message": "row {id}: amount {amount}"}
    feed.write_job(job)
    service = feed.service()
    feed.write_feed("t1", "1001", rows=((1, 4242), (2, 20)))
    run_id = _enqueue(service, _config())
    with caplog.at_level(logging.INFO, logger="streamwright.steps"):
        assert feed.worker(service).run_one() == run_id
    assert service.store.get_request(run_id)["status"] == "succeeded"
    assert [record.getMessage() for record in caplog.records if record.name == "streamwright.steps"] == [
        "step rows: 2 row(s) (row values are not logged by streamwright serve)"]


def test_two_callers_runs_with_the_same_idempotency_key_are_two_runs(feed):
    service = feed.service()
    feed.write_feed("t1", "1001")
    runs = [_enqueue(service, _config(), principal, idempotency_key="order-1", scope_key="client:t1/1001")
            for principal in ("crm", "ops")]
    worker = feed.worker(service)
    while worker.run_one():
        pass
    assert [service.store.get_request(run_id)["result"]["outputs"][0]["already_committed"] for run_id in runs] == [
        False, False]
    batches = open_url("sqlite:///%s" % (feed.folder / "state.db")).list_batches("client_feed")
    assert sorted(batch["job_key"] for batch in batches) == ["serve:crm:order-1", "serve:ops:order-1"]


def _endless(feed, timeout):
    """A service whose job runs for hours, stopped after `timeout`."""
    job = feed.job()
    job["steps"] = [{"name": "rows", "sql": "SELECT count(*) AS n FROM range(1000000) a, range(1000000) b"}]
    job["outputs"][0]["sql"] = "SELECT '{{ config.tenant_id }}' AS tenant, n, 0 AS total FROM rows"
    feed.write_job(job)
    return feed.service({"client_feed": {"manifest": "job.yaml", "principals": ["crm"],
                                         "set": {"tenant_id": {"from_claim": "tenant"}},
                                         "server_options": {"apply_state_migrations": True},
                                         "limits": {"timeout": timeout}}})


def test_a_run_that_outlasts_its_timeout_is_stopped(feed):
    service = _endless(feed, "2s")
    run_id = _enqueue(service, _config())
    started = time.monotonic()
    assert Worker(service.jobs, service.store, worker_id="w-timeout").run_one() == run_id      # in a child process
    run = service.store.get_request(run_id)
    assert (run["status"], run["error_code"]) == ("failed", "timeout")
    assert time.monotonic() - started < 60


def test_a_run_whose_claim_is_taken_over_is_stopped_and_left_to_the_other_worker(feed):
    service = _endless(feed, "30s")
    store = service.store
    run_id = _enqueue(service, _config())

    def take_over():
        while store.get_request(run_id)["status"] != "running":
            time.sleep(0.05)
        store._run(lambda conn: store._x(conn, "UPDATE {sw_requests} SET worker_id = 'w2' WHERE run_id = ?",
                                         (run_id,)))

    thread = threading.Thread(target=take_over)
    thread.start()
    started = time.monotonic()
    assert Worker(service.jobs, store, worker_id="w1", claim_seconds=1).run_one() == run_id
    thread.join()
    assert time.monotonic() - started < 20                       # not left to run until its timeout
    run = store.get_request(run_id)
    assert (run["status"], run["worker_id"], run["result"], run["error_code"]) == ("running", "w2", None, None)


def test_a_claim_the_store_cannot_renew_is_lost_before_it_ends(caplog):
    class Down:
        def renew_request(self, run_id, worker_id, seconds):
            raise ConnectionError("the database is down")

    renewal = worker_module._Renewal(Down(), "run_x", "w1", 1)
    renewal.start()
    assert renewal.lost.wait(3)
    renewal.join()
    assert "its claim ends before it can be renewed" in caplog.text


def test_the_runs_process_ignores_the_signals_that_stop_its_worker(feed):
    feed.write_feed("t1", "1001")
    service = feed.service()
    served = service.jobs["client_feed"]
    options = {**served.entry.server_options, "config": _config(), "input_rows": None, "request_id": "sw_sig",
               "job_key": None}
    done = threading.Event()

    def signal_the_child():                       # Ctrl-C and a service manager's stop reach the whole group
        while not done.wait(0.02):
            for child in multiprocessing.active_children():
                for signum in (signal.SIGINT, signal.SIGTERM):
                    with contextlib.suppress(ProcessLookupError, TypeError):     # ended, or not yet started
                        os.kill(child.pid, signum)

    thread = threading.Thread(target=signal_the_child)
    thread.start()
    try:
        outcome = worker_module._in_child(served.entry.manifest, options, 60)
    finally:
        done.set()
        thread.join()
    assert outcome[:3] == (True, outcome[1], None) and outcome[1]["outputs"][0]["rows"] == 1


def test_a_run_whose_job_lease_is_held_waits_in_the_queue(feed, monkeypatch):
    service = feed.service()
    run_id = _enqueue(service, _config())
    monkeypatch.setattr("streamwright.core.serve.worker._DEFER_SECONDS", 1)

    def held(manifest, options, timeout, log_settings, lost=None):
        return False, None, "lease-held", "job client_feed: the lease is held by another worker"

    assert feed.worker(service, execute=held).run_one() == run_id
    run = service.store.get_request(run_id)
    assert (run["status"], run["worker_id"], run["error_code"]) == ("queued", None, None)
    assert feed.worker(service).run_one() is None                 # not before its time
    time.sleep(1.2)
    feed.write_feed("t1", "1001")
    assert feed.worker(service).run_one() == run_id
    assert service.store.get_request(run_id)["status"] == "succeeded"


def test_a_job_the_service_no_longer_lists_fails_its_run(feed):
    service = feed.service()
    run_id = service.store.enqueue_request(job_name="retired", principal="crm", body_sha256="0" * 64, inputs={},
                                           request_id="sw_x")[0]["run_id"]
    assert feed.worker(service).run_one() == run_id
    assert service.store.get_request(run_id)["error_code"] == "unknown-job"


DATABASES = MappingProxyType({"postgres": "STREAMWRIGHT_TEST_PG_URL", "mysql": "STREAMWRIGHT_TEST_MYSQL_URL"})


@pytest.fixture(params=["sqlite", "duckdb", "postgres", "mysql"])
def queue(request, tmp_path):
    """A migrated state store of each kind (postgresql:// and mysql:// from STREAMWRIGHT_TEST_PG_URL / _MYSQL_URL)."""
    if request.param in ("sqlite", "duckdb"):
        store = open_url("%s:///%s" % (request.param, tmp_path / ("queue." + request.param)))
    else:
        url = os.environ.get(DATABASES[request.param])
        if not url:
            pytest.skip("set %s to run the %s queue tests" % (DATABASES[request.param], request.param))
        separator = "&" if "?" in url else "?"
        store = open_url(url + ("%sschema=sw_q_%s" % (separator, uuid.uuid4().hex[:10])
                                if request.param == "postgres" else ""))
    store.initialize()
    yield store
    if request.param == "mysql":                  # one database: leave it as found
        store._run(lambda conn: store._x(conn, "DELETE FROM {sw_requests}"))
    elif request.param == "postgres":             # its own schema: dropped
        store._run(lambda conn: conn.execute('DROP SCHEMA "%s" CASCADE' % store._schema))


def test_the_queue_claims_one_run_per_scope_and_respects_concurrency(queue):
    store = queue

    def enqueue(scope, **extra):
        return store.enqueue_request(job_name="j", principal="p", body_sha256="0" * 64, inputs={"a": 1},
                                     request_id="r", scope_key=scope, **extra)[0]["run_id"]

    first, same_scope, other = enqueue("s1", payload=[{"x": 1}]), enqueue("s1"), enqueue("s2")
    assert store.claim_request("w1", 30)["run_id"] == first
    assert store.claim_request("w2", 30, concurrency={"j": 5})["run_id"] == other       # s1 is busy
    assert store.claim_request("w3", 30, concurrency={"j": 5}) is None
    assert store.claim_request("w3", 30, concurrency={"j": 2}) is None
    assert not store.renew_request(first, "w2", 30) and store.renew_request(first, "w1", 30)
    assert not store.finish_request(first, "w2", succeeded=True)
    assert store.finish_request(first, "w1", succeeded=True, result={"status": "ok"})
    finished = store.get_request(first)
    assert (finished["status"], finished["payload"], finished["result"]) == ("succeeded", None, {"status": "ok"})
    assert store.claim_request("w3", 30)["run_id"] == same_scope
    assert store.count_requests(("running",), "j") == 2
    assert json.dumps(store.list_requests(job_name="j", statuses=("succeeded",)), default=str).count(first) == 1


def _hold_the_claim(store, monkeypatch):
    """Makes the next claim pause after its UPDATE (its transaction open) until the returned event is set."""
    updated, release, run = threading.Event(), threading.Event(), store._x

    def execute(conn, text, args=()):
        cursor = run(conn, text, args)
        if text.startswith("UPDATE {sw_requests} SET status = 'running'") and not updated.is_set():
            updated.set()
            release.wait(10)
        return cursor

    monkeypatch.setattr(store, "_x", execute)
    return updated, release


@pytest.mark.parametrize("scopes,concurrency", [(("s1", "s1"), None), (("s1", "s2"), {"j": 1})])
def test_claims_at_once_see_each_other(queue, monkeypatch, scopes, concurrency):
    if queue._dialect.name == "duckdb":
        pytest.skip("DuckDB: one process writes; a conflicting claim fails and the worker tries again")
    store = queue
    first, _ = (store.enqueue_request(job_name="j", principal="p", body_sha256="0" * 64, inputs={}, request_id="r",
                                      scope_key=scope)[0]["run_id"] for scope in scopes)
    updated, release = _hold_the_claim(store, monkeypatch)
    claims = {}
    threads = [threading.Thread(target=lambda name=name: claims.setdefault(
        name, store.claim_request(name, 30, concurrency=concurrency))) for name in ("w1", "w2")]
    threads[0].start()
    assert updated.wait(10)                       # w1 has claimed `first`, not yet committed
    threads[1].start()
    time.sleep(0.5)                               # w2 claims meanwhile
    release.set()
    for thread in threads:
        thread.join()
    assert claims["w1"]["run_id"] == first and claims["w2"] is None


def test_a_key_queued_at_once_by_two_requests_is_one_run(queue, monkeypatch):
    store = queue
    queued = MappingProxyType({"job_name": "j", "principal": "p", "body_sha256": "0" * 64, "inputs": {},
                               "request_id": "r", "idempotency_key": "k"})
    first, created = store.enqueue_request(**queued)
    read, hidden = store._requests, []

    def not_yet_committed(conn, where, args, suffix=""):
        if "idempotency_key" in where and not hidden:      # the other request's run is not visible yet: INSERT
            hidden.append(where)
            return []
        return read(conn, where, args, suffix)

    monkeypatch.setattr(store, "_requests", not_yet_committed)
    run, again = store.enqueue_request(**queued)          # the unique key refuses the INSERT: the run is that one
    assert hidden and created and not again and run["run_id"] == first["run_id"]
    assert store.count_requests(("queued",), "j") == 1


def test_a_run_summary_tells_the_caller_the_rows_a_vendor_rejected():
    result = RunResult(0, "push", "", ["b1"], {"upload": 3, "files": 10}, rejected={"upload": 2})
    assert run_summary(result)["outputs"] == [
        {"name": "files", "rows": 10, "already_committed": False},
        {"name": "upload", "rows": 3, "already_committed": False, "rejected": 2}]


def test_a_busy_scope_never_hides_the_runs_queued_behind_it(queue):
    store = queue

    def enqueue(scope):
        return store.enqueue_request(job_name="j", principal="p", body_sha256="0" * 64, inputs={}, request_id="r",
                                     scope_key=scope)[0]["run_id"]

    busy = [enqueue("account-1") for _ in range(30)]          # more than any window of oldest runs
    free = enqueue("account-2")
    assert store.claim_request("w1", 30)["run_id"] == busy[0]
    assert store.claim_request("w2", 30)["run_id"] == free
    assert store.claim_request("w3", 30) is None


def test_ended_runs_are_purged_in_chunks_and_queued_ones_kept(queue):
    store = queue
    runs = [store.enqueue_request(job_name="j", principal="p", body_sha256="0" * 64, inputs={}, request_id="r",
                                  scope_key="s%d" % number)[0]["run_id"] for number in range(5)]
    for number in range(3):
        claimed = store.claim_request("w", 30)
        store.finish_request(claimed["run_id"], "w", succeeded=number != 1, error_code=None)
    later = datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=1)
    assert store.purge_requests(later, dry_run=True) == 3 and store.count_requests(("succeeded", "failed")) == 3
    assert store.purge_requests(datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)) == 0
    assert store.purge_requests(later, chunk=2) == 3
    assert [run_id for run_id in runs if store.get_request(run_id)] == runs[3:]


def test_the_queue_counts_a_callers_recent_runs_and_reports_its_state(queue):
    store = queue
    before = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=5)
    for principal, scope in (("p", "a"), ("p", "b"), ("q", "c")):
        store.enqueue_request(job_name="j", principal=principal, body_sha256="0" * 64, inputs={}, request_id="r",
                              scope_key=scope)
    count, oldest = store.recent_requests("p", "j", before)
    assert count == 2 and oldest is not None
    assert store.recent_requests("p", "j", datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=1)) \
        == (0, None)
    claimed = store.claim_request("w", 30)
    store.finish_request(claimed["run_id"], "w", succeeded=True)
    store.claim_request("w", 30)
    stats = store.queue_stats(before)
    assert sorted((job, status, runs) for job, status, runs, _ in stats["active"]) == [
        ("j", "queued", 1), ("j", "running", 1)]
    assert [(job, status) for job, status, *_ in stats["finished"]] == [("j", "succeeded")]
    assert all(stats["finished"][0][2:])          # created, started and finished


def test_a_finished_run_logs_its_queue_wait_and_run_time(feed, caplog):
    caplog.set_level(logging.INFO, logger="streamwright.serve.worker")
    service = feed.service()
    feed.write_feed("t1", "1001")
    run_id = _enqueue(service, _config())
    feed.worker(service).run_one()
    record = next(record for record in caplog.records if getattr(record, "event", None) == "serve_run")
    assert record.run_id == run_id and record.status == "succeeded" and record.attempt == 1
    assert record.queue_wait_ms >= 0 and record.duration_ms >= 0
    assert "(attempt 1, queued " in record.getMessage()


def test_a_worker_with_processes_runs_that_many_workers(feed, monkeypatch):
    started = []

    class Recorder:
        def __init__(self, jobs, store, *, log_settings=None, worker_id=None):
            self.worker_id = worker_id or "default"

        def run(self, stop):
            started.append((self.worker_id, threading.current_thread() is threading.main_thread()))

    monkeypatch.setattr(serve_cli, "Worker", Recorder)
    monkeypatch.setattr(serve_cli, "_service", lambda args: (types.SimpleNamespace(jobs={}, store=None), None))
    args = types.SimpleNamespace(serve_yaml="serve.yaml", processes=3, log_level=None, log_format=None, log=None,
                                 log_config=None, log_max_chars=None)
    assert serve_cli._worker(args) == 0
    ids = sorted(worker_id for worker_id, _ in started)
    assert [worker_id.rsplit("/", 1)[1] for worker_id in ids] == ["1", "2", "3"]
    assert len({worker_id.rsplit("/", 1)[0] for worker_id in ids}) == 1       # one worker: its runs, numbered
    assert sum(1 for _, main in started if main) == 1          # one on the main thread: signals reach it
    monkeypatch.setattr(serve_cli, "_service", lambda args: (types.SimpleNamespace(jobs={}, store=None), None))
    started.clear()
    assert serve_cli._worker(types.SimpleNamespace(**dict(vars(args), processes=1))) == 0
    assert started == [("default", True)]


def test_requests_at_once_never_exceed_a_rate(queue, monkeypatch):
    if queue._dialect.name == "duckdb":
        pytest.skip("DuckDB cannot hold serve's queue (one writing process)")
    store, counted, release = queue, threading.Event(), threading.Event()
    recent = store._recent

    def slow(conn, principal, job_name, since):      # the first request holds its transaction after counting
        found = recent(conn, principal, job_name, since)
        if not counted.is_set():
            counted.set()
            release.wait(10)
        return found

    monkeypatch.setattr(store, "_recent", slow)
    since = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=1)
    outcomes = {}

    def request(name):
        try:
            outcomes[name] = store.enqueue_request(job_name="j", principal="p", body_sha256="0" * 64, inputs={},
                                                   request_id=name, scope_key=name, rate=(1, since))[1]
        except RateLimitedError:
            outcomes[name] = "rate-limited"

    first = threading.Thread(target=request, args=("a",))
    first.start()
    assert counted.wait(10)                       # a has counted 0 and holds the lock
    second = threading.Thread(target=request, args=("b",))
    second.start()
    time.sleep(0.5)                               # b waits for a's transaction
    release.set()
    first.join()
    second.join()
    assert outcomes == {"a": True, "b": "rate-limited"}
    assert store.count_requests(("queued",), "j") == 1
