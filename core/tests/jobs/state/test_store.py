"""State store contract suite: the same tests against SQLite, DuckDB and PostgreSQL 16 (TST-02)."""

import datetime
import os
import sqlite3

import duckdb
import pytest

from streamwright.core.jobs.state import (
    LeaseHeldError,
    SqliteDialect,
    SqlStateStore,
    StaleLeaseError,
    StateError,
    StateStore,
    open_sqlite,
    open_store,
)
from streamwright.core.jobs.state.migrations import LATEST_VERSION
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
    sequence = kwargs.pop("batch_sequence", None) or store.next_batch_sequence(lease.job_name)
    store.record_batch(lease, batch_id, status, {"pinned": {}}, batch_sequence=sequence, **kwargs)
    return sequence


def test_store_implements_the_protocol_and_migrates_once(store):
    assert isinstance(store, StateStore)
    store.initialize()
    assert store.schema_version() == LATEST_VERSION


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
    assert store.next_batch_sequence("job") == sequence + 1
    with pytest.raises(StateError):
        store.record_batch(lease, "b2", "received", {}, batch_sequence=sequence)


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


def test_run_key_is_unique_per_job(store):
    lease = store.acquire_lease("job", "w", 300)
    _batch(store, lease, "b1", run_key="2026-11-01")
    assert store.find_batch_by_run_key("job", "2026-11-01")["batch_id"] == "b1"
    assert store.find_batch_by_run_key("job", "other") is None
    with pytest.raises(Exception, match=r"(?i)unique|duplicate"):  # the database's own constraint error
        _batch(store, lease, "b2", run_key="2026-11-01")
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
    with pytest.raises(StateError, match="needs a batch_sequence"):
        store.record_batch(lease, "b1", "received", {})


def test_a_close_failure_after_a_successful_commit_is_reported(tmp_path):
    with pytest.raises(sqlite3.OperationalError, match="close failed"):
        _flaky_store(tmp_path, {"close"}).acquire_lease("job", "w", 300)
    assert open_sqlite(str(tmp_path / "flaky.db")).get_lease("job")["epoch"] == 1


def test_sqlite_tables_use_the_prefix_and_named_constraints(tmp_path):
    path = str(tmp_path / "named.db")
    open_sqlite(path).initialize()
    with sqlite3.connect(path) as conn:
        found = dict(conn.execute("SELECT name, sql FROM sqlite_master WHERE type = 'table'").fetchall())
    assert set(found) == {"streamwright_" + t for t in ("job_leases", "job_sequences", "batches", "output_receipts",
                                                         "outbox_events", "schema_signatures", "store_metadata",
                                                         "schema_migrations")}
    assert "CONSTRAINT uq_streamwright_batches__job_name_run_key UNIQUE" in found["streamwright_batches"]


def test_postgres_uses_a_schema_and_named_constraints(pg_schema):
    psycopg = pytest.importorskip("psycopg")
    store = open_store(lambda: psycopg.connect(PG_DSN), "postgres", schema=pg_schema)
    store.initialize()
    with psycopg.connect(PG_DSN) as conn:
        names = {r[0] for r in conn.execute(
            "SELECT conname FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace WHERE n.nspname = %s",
            (pg_schema,)).fetchall()}
        types = dict(conn.execute("SELECT column_name, data_type FROM information_schema.columns WHERE "
                                  "table_schema = %s AND table_name = 'batches'", (pg_schema,)).fetchall())
    assert {"pk_batches", "uq_batches__job_name_batch_sequence", "uq_batches__job_name_run_key",
            "pk_output_receipts", "pk_outbox_events", "pk_job_leases"} <= names
    assert types["manifest_json"] == "jsonb" and types["updated_at"] == "timestamp with time zone"
