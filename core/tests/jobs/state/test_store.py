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

"""State store contract suite: the same tests against SQLite, DuckDB and PostgreSQL 16 (TST-02)."""

import datetime
import logging
import os
import sqlite3
import threading

import duckdb
import pytest

from streamwright.core.jobs.state import (
    LeaseHeldError,
    MySQLDialect,
    PostgresDialect,
    SqliteDialect,
    SqlStateStore,
    StaleLeaseError,
    StateError,
    StateStore,
    open_sqlite,
    open_store,
)
from streamwright.core.jobs.state.target import describe_connection, describe_url
from streamwright.core.jobs.types import WriteReceipt

PG_DSN = os.environ.get("STREAMWRIGHT_TEST_PG_DSN")


@pytest.fixture(params=["sqlite", "duckdb", "postgres"])
def store(request, tmp_path):
    if request.param == "sqlite":
        found = open_sqlite(str(tmp_path / "state.db"))
    elif request.param == "duckdb":
        con = duckdb.connect(str(tmp_path / "state.duckdb"))
        request.addfinalizer(con.close)
        found = open_store(con.cursor, "duckdb", catalog="state")
    else:
        schema = request.getfixturevalue("pg_schema")
        psycopg = pytest.importorskip("psycopg")
        found = open_store(lambda: psycopg.connect(PG_DSN), "postgres", schema=schema)
    found.initialize()
    return found


def _batch(store, lease, batch_id, status="received", **kwargs):
    sequence = kwargs.pop("batch_sequence", None) or store.next_batch_sequence(lease, kwargs.get("scope", ""))
    store.record_batch(lease, batch_id, status, {"pinned": {}}, batch_sequence=sequence, **kwargs)
    return sequence


def test_store_implements_the_protocol_and_migrates_once(store):
    assert isinstance(store, StateStore)
    store.initialize()
    assert store.schema_version() == 1


def test_lease_is_exclusive_until_released_or_expired(store):
    lease = store.acquire_lease("job", "worker-a", 300)
    assert lease.epoch == 1 and lease.expires_at.endswith("Z")
    with pytest.raises(LeaseHeldError):
        store.acquire_lease("job", "worker-b", 300)
    renewed = store.renew_lease(lease, 600)
    assert renewed.epoch == 1 and renewed.expires_at > lease.expires_at
    store.release_lease(renewed)
    assert store.get_lease("job")["held"] is False
    taken = store.acquire_lease("job", "worker-b", 300)
    assert taken.epoch == 2


def test_an_expired_lease_is_taken_over_and_the_old_worker_is_fenced(store):
    old = store.acquire_lease("job", "worker-a", 0)          # expires at once (database clock)
    with pytest.raises(StaleLeaseError):
        store.check_lease(old)
    new = store.acquire_lease("job", "worker-b", 300)
    assert new.epoch == old.epoch + 1
    store.check_lease(new)
    for write in (lambda: store.renew_lease(old, 300),
                  lambda: store.record_batch(old, "b1", "received", {}, batch_sequence=1),
                  lambda: store.commit_batch(old, "b1", {}, {}, [("p", "h", "s")])):
        with pytest.raises(StaleLeaseError):
            write()


def test_batch_status_machine_keeps_the_status_it_failed_from(store):
    lease = store.acquire_lease("job", "w", 300)
    _batch(store, lease, "b1")
    store.record_batch(lease, "b1", "processing", {"pinned": {"x": 1}})
    with pytest.raises(StateError):
        store.record_batch(lease, "b1", "completed", {})
    store.record_batch(lease, "b1", "failed", {"pinned": {"x": 1}}, error="boom")
    failed = store.get_batch("job", "b1")
    assert (failed["status"], failed["failed_from"], failed["error"]) == ("failed", "processing", "boom")
    with pytest.raises(StateError):
        store.record_batch(lease, "b1", "processing", {})       # only retry_batch leaves failed
    assert store.retry_batch(lease, "b1") == "processing"
    resumed = store.get_batch("job", "b1")
    assert (resumed["status"], resumed["failed_from"], resumed["error"]) == ("processing", None, None)
    assert resumed["manifest"] == {"pinned": {"x": 1}}
    with pytest.raises(StateError):
        store.retry_batch(lease, "b1")


def test_batch_sequence_never_comes_back_after_a_purge(store):
    lease = store.acquire_lease("job", "w", 300)
    sequence = _batch(store, lease, "b1")
    for status in ("processing", "committed", "acked", "completed"):
        store.record_batch(lease, "b1", status, {})
    future = datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=1)
    assert store.purge_batches("job", future) == 1
    assert store.get_batch("job", "b1") is None
    assert store.next_batch_sequence(lease) == sequence + 1


def test_a_purge_deletes_batches_a_chunk_per_transaction(store):
    lease = store.acquire_lease("job", "w", 300)
    for batch_id in ("b1", "b2", "b3"):
        _batch(store, lease, batch_id, status="processing")
        store.commit_batch(lease, batch_id, {}, {"push": ("dest", [{"a": 1}])})
        for status in ("acked", "completed"):
            store.record_batch(lease, batch_id, status, {})
    future = datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=1)
    assert store.purge_batches("job", future, chunk=2) == 3
    assert [store.get_batch("job", batch_id) for batch_id in ("b1", "b2", "b3")] == [None, None, None]
    assert store.list_events("job", "b3") == []


def test_a_purge_deletes_every_outbox_row_of_its_batches(store):
    lease = store.acquire_lease("job", "w", 300)
    _batch(store, lease, "b1", status="processing")
    store.commit_batch(lease, "b1", {}, {"push": ("dest", [{"a": 1}, {"a": 2}])})
    store.record_event_outcomes(lease, "b1", "push", [(0, "applied", {"id": 1}, None, None),
                                                      (1, "failed", None, "REJECTED", "no")])
    for status in ("acked", "completed"):
        store.record_batch(lease, "b1", status, {})
    assert store.purge_batches("job", datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=1)) == 1
    assert store.list_events("job", "b1") == []


def test_retry_puts_only_the_failed_apply_rows_of_the_resent_outputs_back_to_pending(store):
    lease = store.acquire_lease("job", "w", 300)
    _batch(store, lease, "b1", status="processing")
    store.commit_batch(lease, "b1", {}, {"push": ("dest", [{"a": n} for n in range(4)]), "log": ("dest", [{"a": 9}])})
    store.mark_events_in_flight(lease, "b1", "push", [0, 1, 2])
    store.record_event_outcomes(lease, "b1", "push", [(0, "applied", {"id": 1}, None, None),
                                                      (1, "failed", None, "REJECTED", "no")])
    store.settle_in_flight_events(lease, "b1", "push", resend=False)     # row 2: skipped, UNKNOWN_OUTCOME
    store.mark_events_in_flight(lease, "b1", "push", [3])
    store.mark_events_in_flight(lease, "b1", "log", [0])
    store.record_event_outcomes(lease, "b1", "log", [(0, "failed", None, "REJECTED", "no")])
    for status in ("acked", "failed"):
        store.record_batch(lease, "b1", status, {}, error="rejected")
    assert store.retry_batch(lease, "b1", resend=["push"]) == "acked"
    assert [(row["output_name"], row["event_sequence"], row["status"], row["attempts"], row["error_code"])
            for row in store.list_events("job", "b1")] == [
        ("log", 0, "failed", 1, "REJECTED"), ("push", 0, "applied", 1, None), ("push", 1, "pending", 1, "REJECTED"),
        ("push", 2, "failed", 1, "UNKNOWN_OUTCOME"), ("push", 3, "in_flight", 1, None)]
    store.record_batch(lease, "b1", "failed", {}, error="rejected")
    store.retry_batch(lease, "b1")                                       # no outputs named: every output's
    assert [(row["output_name"], row["event_sequence"]) for row in store.list_pending_events("job")] == [
        ("log", 0), ("push", 1), ("push", 3)]


def test_receipt_and_manifest_are_saved_together(store):
    lease = store.acquire_lease("job", "w", 300)
    _batch(store, lease, "b1", "processing")
    receipt = WriteReceipt("out", "db.table", 3, version=7, files=("a.csv",))
    store.record_receipt(lease, "b1", receipt, {"pinned": {"ingest:s": [1]}})
    assert WriteReceipt.from_dict(store.get_receipts("job", "b1")["out"]) == receipt
    assert store.get_batch("job", "b1")["manifest"] == {"pinned": {"ingest:s": [1]}}


def test_commit_writes_the_outbox_in_the_same_transaction(store):
    lease = store.acquire_lease("job", "w", 300)
    _batch(store, lease, "b1", "processing")
    with pytest.raises(StateError):
        store.commit_batch(lease, "b1", {}, {"events": (None, [{"a": 1}])})
    assert store.get_batch("job", "b1")["status"] == "processing"
    store.commit_batch(lease, "b1", {"m": 1}, {"events": ("topic-a", [{"a": 1}, {"a": 2}])})
    assert store.get_batch("job", "b1")["status"] == "committed"
    pending = store.list_pending_events("job")
    assert [(e["destination"], e["event_sequence"], e["payload"]) for e in pending] == [
        ("topic-a", 0, {"a": 1}), ("topic-a", 1, {"a": 2})]
    store.mark_events_sent(lease, "b1", "events", [0, 1])
    assert store.list_pending_events("job") == []


def test_job_key_is_unique_per_job(store):
    lease = store.acquire_lease("job", "w", 300)
    _batch(store, lease, "b1", job_key="2026-11-01")
    assert store.find_batch_by_job_key("job", "2026-11-01")["batch_id"] == "b1"
    assert store.find_batch_by_job_key("job", "other") is None
    with pytest.raises(Exception, match=r"(?i)unique|duplicate"):  # the database's own constraint error
        _batch(store, lease, "b2", job_key="2026-11-01")
    assert [b["batch_id"] for b in store.list_incomplete_batches("job")] == ["b1"]


def test_schema_signatures_are_replaced_per_prefix(store):
    lease = store.acquire_lease("job", "w", 300)
    _batch(store, lease, "b1", "processing")
    _batch(store, lease, "b2", "processing")
    store.commit_batch(lease, "b1", {}, {}, [("s3://a/", "h1", "a,b"), ("s3://b/", "h2", "c")])
    store.commit_batch(lease, "b2", {}, {}, [("s3://a/", "h3", "a,b,c")])
    assert store.get_schema_signatures("job") == {"s3://a/": "h3", "s3://b/": "h2"}


# --- failure injection (SQLite underneath) -------------------------------------------------------------------

class _Flaky:
    """A DB-API connection whose commit, rollback or close fails on demand."""

    def __init__(self, conn, fail):
        self._conn, self._fail = conn, fail

    def execute(self, *args):
        return self._conn.execute(*args)

    def _call(self, name):
        if name in self._fail:
            getattr(self._conn, name)()
            raise sqlite3.OperationalError("%s failed" % name)
        getattr(self._conn, name)()

    def commit(self):
        self._call("commit")

    def rollback(self):
        self._call("rollback")

    def close(self):
        self._call("close")


def _flaky_store(tmp_path, fail):
    path = str(tmp_path / "flaky.db")
    open_sqlite(path).initialize()
    return SqlStateStore(lambda: _Flaky(sqlite3.connect(path, isolation_level=None), fail), SqliteDialect())


def test_a_failed_commit_propagates(tmp_path):
    with pytest.raises(sqlite3.OperationalError, match="commit failed"):
        _flaky_store(tmp_path, {"commit"}).acquire_lease("job", "w", 300)


def test_rollback_and_close_failures_keep_the_original_error(tmp_path):
    store = _flaky_store(tmp_path, {"rollback", "close"})
    lease = open_sqlite(str(tmp_path / "flaky.db")).acquire_lease("job", "w", 300)
    with pytest.raises(StateError, match="a new batch starts as received or processing"):
        store.record_batch(lease, "b1", "completed", {})


def test_a_close_failure_after_a_successful_commit_is_reported(tmp_path):
    with pytest.raises(sqlite3.OperationalError, match="close failed"):
        _flaky_store(tmp_path, {"close"}).acquire_lease("job", "w", 300)
    assert open_sqlite(str(tmp_path / "flaky.db")).get_lease("job")["epoch"] == 1


def test_migrations_log_the_database_they_change_and_never_its_password(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="streamwright.jobs.state.migrations")
    store = open_sqlite(str(tmp_path / "state.db"))
    store.initialize()
    lines = [record.getMessage() for record in caplog.records]
    target = "sqlite file %s" % (tmp_path / "state.db")
    assert lines[0] == "applying 1 state migration(s) to %s: 0001_initial_schema" % target
    assert lines[1].startswith("state migration 0001_initial_schema (version 1) applied to %s in " % target)
    assert lines[1].endswith("store id %s" % store.store_identity())
    assert caplog.records[1].database == target and caplog.records[1].duration_ms >= 0
    described = describe_url("postgresql://sw:s3cr3t@db.internal:5432/etl?sslpassword=hush&schema=x", "postgres",
                             schema="sw_ops")
    assert described == "postgres sw@db.internal:5432/etl, schema sw_ops"
    assert describe_connection("ops", "mysql", {"host": "h", "port": 3306, "database": "shop", "user": "app",
                                                "password": "s3cr3t"}) == "mysql connection 'ops' (app@h:3306/shop)"


def test_a_sqlite_state_file_in_a_missing_folder_is_a_clear_state_error(tmp_path):
    with pytest.raises(StateError, match="its folder %s does not exist" % (tmp_path / "missing")):
        open_sqlite(str(tmp_path / "missing" / "state.db")).initialize()


def test_an_in_memory_sqlite_store_keeps_its_tables_across_operations_and_threads():
    store = open_sqlite(":memory:")
    store.initialize()
    assert store.check_schema() and store.list_incomplete_batches("job") == []
    lease = store.acquire_lease("job", "owner", 60)
    errors = []
    thread = threading.Thread(target=lambda: errors.extend(
        [] if store.renew_lease(lease, 60) else ["renewal returned nothing"]))   # the lease heartbeat's thread
    thread.start()
    thread.join()
    assert errors == [] and store.get_lease("job")["owner_id"] == "owner"


def test_sqlite_tables_use_the_prefix_and_named_constraints(tmp_path):
    path = str(tmp_path / "named.db")
    open_sqlite(path).initialize()
    with sqlite3.connect(path) as conn:
        found = dict(conn.execute("SELECT name, sql FROM sqlite_master WHERE type = 'table'").fetchall())
    assert set(found) == {"sw_" + t for t in ("leases", "jobs", "sequences", "receipts", "events", "signatures",
                                              "requests", "migrations")}
    assert "CONSTRAINT uq_sw_jobs__name_scope_key UNIQUE (job_name, scope, job_key)" in found["sw_jobs"]


def test_postgres_uses_a_schema_and_named_constraints(pg_schema):
    psycopg = pytest.importorskip("psycopg")
    store = open_store(lambda: psycopg.connect(PG_DSN), "postgres", schema=pg_schema)
    store.initialize()
    with psycopg.connect(PG_DSN) as conn:
        names = {r[0] for r in conn.execute(
            "SELECT conname FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace WHERE n.nspname = %s",
            (pg_schema,)).fetchall()}
        types = dict(conn.execute("SELECT column_name, data_type FROM information_schema.columns WHERE "
                                  "table_schema = %s AND table_name = 'sw_jobs'", (pg_schema,)).fetchall())
    assert {"pk_sw_jobs", "uq_sw_jobs__name_scope_key", "pk_sw_receipts", "pk_sw_events", "pk_sw_leases"} <= names
    assert types["checkpoint"] in ("json", "jsonb") and types["updated_at"] == "timestamp with time zone"


def test_multi_resource_leases_disjoint_tokens_can_run_concurrently(store):
    # Job 1 acquires accounts 1-5 for client xyz
    tokens_1 = ["client:xyz/acc:%d" % i for i in range(1, 6)]
    lease_1 = store.acquire_lease("ads_sync", "worker-1", 300, tokens=tokens_1)
    assert set(lease_1.tokens) == set(tokens_1)
    assert lease_1.pruned_tokens == ()

    # Job 2 acquires accounts 6-10 for client xyz concurrently (no conflict!)
    tokens_2 = ["client:xyz/acc:%d" % i for i in range(6, 11)]
    lease_2 = store.acquire_lease("ads_sync", "worker-2", 300, tokens=tokens_2)
    assert set(lease_2.tokens) == set(tokens_2)
    assert lease_2.pruned_tokens == ()

    # Verify both leases are active
    for t in tokens_1:
        assert store.get_lease("ads_sync", token=t)["owner_id"] == "worker-1"
    for t in tokens_2:
        assert store.get_lease("ads_sync", token=t)["owner_id"] == "worker-2"

    store.release_lease(lease_1)
    store.release_lease(lease_2)


def test_multi_resource_leases_overlapping_tokens_fail_policy(store):
    tokens_1 = ["client:xyz/acc:%d" % i for i in range(1, 6)]
    lease_1 = store.acquire_lease("ads_sync", "worker-1", 300, tokens=tokens_1)

    # Job 2 tries accounts 3, 4, 6, 7 (3 and 4 are held by worker-1)
    tokens_2 = ["client:xyz/acc:%d" % i for i in (3, 4, 6, 7)]
    with pytest.raises(LeaseHeldError) as exc_info:
        store.acquire_lease("ads_sync", "worker-2", 300, tokens=tokens_2, policy="fail")
    assert "client:xyz/acc:3" in str(exc_info.value)
    assert "client:xyz/acc:4" in str(exc_info.value)
    assert "client:xyz/acc:3" in exc_info.value.conflicts
    assert "client:xyz/acc:4" in exc_info.value.conflicts

    store.release_lease(lease_1)


def test_multi_resource_leases_prune_policy_prunes_held_and_acquires_available(store):
    tokens_1 = ["client:xyz/acc:%d" % i for i in range(1, 6)]
    lease_1 = store.acquire_lease("ads_sync", "worker-1", 300, tokens=tokens_1)

    # Job 2 runs with prune: 3 and 4 held, 6 and 7 free
    tokens_2 = ["client:xyz/acc:%d" % i for i in (3, 4, 6, 7)]
    lease_2 = store.acquire_lease("ads_sync", "worker-2", 300, tokens=["scope:b"], optional=tokens_2, policy="prune")
    assert set(lease_2.tokens) == {"scope:b", "client:xyz/acc:6", "client:xyz/acc:7"}
    assert set(lease_2.pruned_tokens) == {"client:xyz/acc:3", "client:xyz/acc:4"}

    # Job 3 runs with prune when ALL tokens are held
    lease_3 = store.acquire_lease("ads_sync", "worker-3", 300, tokens=["scope:c"],
                                  optional=["client:xyz/acc:1", "client:xyz/acc:2"], policy="prune")
    assert lease_3.tokens == ("scope:c",)
    assert set(lease_3.pruned_tokens) == {"client:xyz/acc:1", "client:xyz/acc:2"}

    # a needed token held (the scope's): nothing is taken, the run is skipped
    lease_4 = store.acquire_lease("ads_sync", "worker-4", 300, tokens=["scope:b"], optional=["client:xyz/acc:9"],
                                  policy="prune")
    assert (lease_4.epoch, lease_4.tokens) == (0, ())
    assert store.get_lease("ads_sync", "client:xyz/acc:9")["held"] is False

    store.release_lease(lease_1)
    store.release_lease(lease_2)


def test_multi_resource_leases_wait_policy_waits_and_acquires_on_release(store):
    tokens_1 = ["client:xyz/acc:1", "client:xyz/acc:2"]
    lease_1 = store.acquire_lease("ads_sync", "worker-1", 1, tokens=tokens_1)

    # In another thread or after release/expiry, wait policy succeeds
    store.release_lease(lease_1)
    lease_2 = store.acquire_lease("ads_sync", "worker-2", 300, tokens=["client:xyz/acc:2", "client:xyz/acc:3"],
                                  policy="wait", timeout=1.0)
    assert set(lease_2.tokens) == {"client:xyz/acc:2", "client:xyz/acc:3"}
    store.release_lease(lease_2)



# --- scopes, sequences, metrics ------------------------------------------------------------------------------

def _complete(store, lease, batch_id, manifest=None):
    for status in ("processing", "committed", "acked", "completed"):
        store.record_batch(lease, batch_id, status, manifest or {})


def test_history_is_kept_per_scope(store):
    a = store.acquire_lease("job", "w1", 300, tokens=["scope:a"])
    b = store.acquire_lease("job", "w2", 300, tokens=["scope:b"])        # scopes run side by side
    assert _batch(store, a, "a1", scope="a", job_key="k") == 1
    assert _batch(store, b, "b1", scope="b", job_key="k") == 1           # sequences and job keys are per scope
    store.record_batch(a, "a1", "processing", {})
    store.record_batch(a, "a1", "failed", {}, error="boom")
    assert [x["batch_id"] for x in store.list_incomplete_batches("job", "a")] == ["a1"]
    assert [x["batch_id"] for x in store.list_incomplete_batches("job", "b")] == ["b1"]
    assert store.list_incomplete_batches("job") == []
    assert store.find_batch_by_job_key("job", "k", "b")["batch_id"] == "b1"
    _complete(store, b, "b1", {"carry": {"x": 1}})
    assert store.find_latest_completed_batch("job", "b")["manifest"] == {"carry": {"x": 1}}
    assert store.find_latest_completed_batch("job", "a") is None
    assert {x["batch_id"]: x["scope"] for x in store.list_batches("job")} == {"a1": "a", "b1": "b"}
    assert [x["batch_id"] for x in store.list_batches("job", scope="a")] == ["a1"]


def test_the_outbox_is_relayed_per_scope(store):
    a = store.acquire_lease("job", "w1", 300, tokens=["scope:a"])
    _batch(store, a, "a1", "processing", scope="a")
    store.commit_batch(a, "a1", {}, {"out": ("topic", [{"n": 1}])})
    assert [e["batch_id"] for e in store.list_pending_events("job", "a")] == ["a1"]
    assert store.list_pending_events("job", "b") == []


def test_a_completed_batch_records_its_rows_and_duration(store):
    lease = store.acquire_lease("job", "w", 300)
    _batch(store, lease, "b1", "processing")
    store.record_receipt(lease, "b1", WriteReceipt("one", "t1", 3))
    store.record_receipt(lease, "b1", WriteReceipt("two", "t2", 4))
    store.record_receipt(lease, "b1", WriteReceipt("two", "t2", 5))      # a rewrite replaces its receipt
    for status in ("committed", "acked"):
        store.record_batch(lease, "b1", status, {})
    assert store.get_batch("job", "b1")["finished_at"] is None
    store.record_batch(lease, "b1", "completed", {})
    batch = store.get_batch("job", "b1")
    assert batch["rows_written"] == 8
    assert batch["finished_at"] is not None and batch["duration_ms"] >= 0


def test_a_skipped_run_is_recorded_without_a_lease(store):
    store.record_skipped("job", "job-skipped-1", {"batch_ts": "t"}, scope="a",
                         tokens={"accepted": [], "skipped": ["acct:1"]})
    batch = store.get_batch("job", "job-skipped-1")
    assert (batch["status"], batch["scope"], batch["tokens"]["skipped"]) == ("skipped", "a", ["acct:1"])
    assert batch["finished_at"] is not None
    assert store.list_incomplete_batches("job", "a") == []


def test_wait_backs_off_with_jitter_until_the_timeout(tmp_path):
    path = str(tmp_path / "wait.db")
    open_sqlite(path).initialize()
    pauses = []
    store = SqlStateStore(lambda: sqlite3.connect(path, isolation_level=None), SqliteDialect(),
                          sleep=pauses.append)
    store.acquire_lease("job", "w1", 300, tokens=["t"])
    with pytest.raises(LeaseHeldError, match="wait timed out"):
        store.acquire_lease("job", "w2", 300, tokens=["t"], policy="wait", timeout=0.3)
    assert pauses and all(0 < p <= 0.3 for p in pauses)
    assert len(pauses) > 1 or pauses[0] >= 0.1


def test_postgres_and_mysql_escape_percent_signs():
    for dialect in (PostgresDialect(), MySQLDialect()):
        sql = dialect.placeholders("SELECT '%' || ? FROM t WHERE e < " + dialect.expires_in("?"))
        assert "'%%'" in sql and "?" not in sql and sql.count("%s") == 2


def test_mysql_migration_lock_needs_one():
    dialect = MySQLDialect()
    assert dialect.migration_locked((1,))
    assert not dialect.migration_locked((0,)) and not dialect.migration_locked((None,))
    assert not dialect.migration_locked(None)


# --- the store before sw_* tables -----------------------------------------------------------------------------

