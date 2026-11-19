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

"""Tests for StreamWright state database migrations, runner, and URL parser."""

import gc
import importlib
import sqlite3
import sys
import threading
import types
import urllib.parse

import duckdb
import pytest

from streamwright.core.jobs.state.dialects import DuckDBDialect, SqliteDialect
from streamwright.core.jobs.state.migrations import MigrationRunner
from streamwright.core.jobs.state.tables import get_tables
from streamwright.core.jobs.state.url import ENV_STATE_URL, _file_path, open_url, resolve_state_url


def test_sqlite_migration_lifecycle(tmp_path):
    db_file = str(tmp_path / "test_migration.db")
    runner = MigrationRunner(lambda: sqlite3.connect(db_file, isolation_level=None), SqliteDialect())

    # 1. Before apply
    assert runner.check() is False
    show = runner.show()
    assert [migration["version"] for migration in show] == [1]
    assert show[0]["applied"] is False
    assert show[0]["store_id"] is None

    # 2. Dry run
    plan = runner.apply(dry_run=True)
    assert [migration["version"] for migration in plan] == [1]
    assert len(plan[0]["statements"]) >= 6
    assert any("attempts INTEGER DEFAULT '1' NOT NULL" in statement for statement in plan[0]["statements"])
    assert any(statement.startswith("CREATE TABLE IF NOT EXISTS sw_requests") for statement in plan[0]["statements"])

    # 3. Apply
    applied = runner.apply()
    assert [migration["version"] for migration in applied] == [1]
    store_id = applied[0]["store_id"]
    assert store_id is not None
    assert runner.store_id() == store_id
    assert runner.schema_version() == runner.latest_version() == 1

    # 4. After apply
    assert runner.check() is True
    show_after = runner.show()
    assert show_after[0]["applied"] is True
    assert show_after[0]["store_id"] == store_id

    # 5. Idempotent re-apply
    re_applied = runner.apply()
    assert re_applied == []
    assert runner.check() is True


def test_duckdb_migration_lifecycle():
    con = duckdb.connect()
    runner = MigrationRunner(con.cursor, DuckDBDialect(), schema="test_sw")

    assert runner.check() is False
    applied = runner.apply()
    assert [migration["version"] for migration in applied] == [1]
    assert runner.check() is True

    # Verify tables in schema
    tables = [r[0] for r in con.execute("SHOW TABLES FROM test_sw").fetchall()]
    assert set(tables) == {"sw_leases", "sw_jobs", "sw_sequences", "sw_receipts", "sw_events", "sw_signatures",
                           "sw_requests", "sw_migrations"}


def test_url_resolver_sqlite(tmp_path):
    path = str(tmp_path / "url_test.db")
    resolved = resolve_state_url(f"sqlite:///{path}")
    assert resolved.scheme == "sqlite"
    assert isinstance(resolved.dialect, SqliteDialect)
    assert resolved.schema is None

    runner = resolved.make_runner()
    assert runner.check() is False
    runner.apply()
    assert runner.check() is True


def test_url_resolver_duckdb():
    resolved = resolve_state_url("duckdb:///:memory:?schema=custom_schema")
    assert resolved.scheme == "duckdb"
    assert isinstance(resolved.dialect, DuckDBDialect)
    assert resolved.schema == "custom_schema"


def test_url_resolver_env_variable(tmp_path, monkeypatch):
    path = str(tmp_path / "env_db.sqlite")
    monkeypatch.setenv(ENV_STATE_URL, f"sqlite:///{path}")

    store = open_url()
    store.initialize()
    assert store.schema_version() == 1
    assert store.check_schema() is True
    assert store.store_identity() is not None


def test_url_resolver_missing_url(monkeypatch):
    monkeypatch.delenv(ENV_STATE_URL, raising=False)
    with pytest.raises(ValueError, match="no state database URL provided"):
        resolve_state_url()


def test_url_resolver_unsupported_scheme():
    with pytest.raises(ValueError, match="unsupported state database URL scheme"):
        resolve_state_url("oracle://user:pass@host/db")


def test_table_and_column_comments():
    """Verify that all state tables and their columns have generalized comments."""
    initial_schema = importlib.import_module("streamwright.core.jobs.state.migrations.0001_initial_schema")

    tables = get_tables()
    assert len(tables) == 8

    for name, table in tables.items():
        assert table.comment, f"Table {name} must have a comment"
        assert len(table.comment.strip()) > 10
        for col in table.columns:
            assert col.comment, f"Column {name}.{col.name} must have a comment"
            assert len(col.comment.strip()) > 5

    # Check postgres / duckdb comment statements generation
    statements = initial_schema.get_statements("postgres")
    table_comment_stmts = [s for s in statements if s.startswith("COMMENT ON TABLE")]
    column_comment_stmts = [s for s in statements if s.startswith("COMMENT ON COLUMN")]
    assert len(table_comment_stmts) == 8
    assert len(column_comment_stmts) > 30


def test_url_paths_follow_the_slash_convention(tmp_path, monkeypatch):


    def path(url):
        return _file_path(urllib.parse.urlparse(url))

    assert path("sqlite:///state.db") == "state.db"
    assert path("sqlite:////var/lib/state.db") == "/var/lib/state.db"
    assert path("sqlite:///:memory:") == path("sqlite://") == ":memory:"
    assert path("duckdb:///dir/state.duckdb") == "dir/state.duckdb"
    monkeypatch.chdir(tmp_path)
    open_url("sqlite:///relative.db").initialize()
    assert (tmp_path / "relative.db").exists()


def test_an_in_memory_url_keeps_its_database():
    store = open_url("sqlite:///:memory:")
    store.initialize()
    gc.collect()
    assert store.check_schema() is True
    other = open_url("sqlite:///:memory:")
    assert other.check_schema() is False                              # every resolved URL is its own database


def test_an_in_memory_url_store_serves_concurrent_threads():
    store = open_url("sqlite:///:memory:")
    store.initialize()
    lease = store.acquire_lease("job", "owner", 60)
    errors = []

    def renew():                                                       # the lease heartbeat's thread
        try:
            for _ in range(200):
                store.renew_lease(lease, 60)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=renew)
    thread.start()
    for _ in range(200):
        store.list_incomplete_batches("job")
        store.check_lease(lease)
    thread.join()
    assert errors == []


def test_postgres_urls_hand_the_driver_its_own_parameters(monkeypatch):

    seen = []
    monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(connect=lambda url, **kw: seen.append((url, kw))))
    resolved = resolve_state_url("postgres://u:p@h:5432/db?schema=sw&sslmode=require&catalog=x")
    resolved.connect()
    assert resolved.schema == "sw"
    # no server-side prepared statements: PgBouncer's transaction pooling breaks them
    assert seen == [("postgresql://u:p@h:5432/db?sslmode=require", {"prepare_threshold": None})]


def test_mysql_urls_decode_credentials_and_use_their_database(monkeypatch):

    seen = []
    monkeypatch.setitem(sys.modules, "pymysql", types.SimpleNamespace(connect=lambda **kw: seen.append(kw)))
    resolved = resolve_state_url("mysql://us%40er:p%3Ass@h/state_db")
    assert hasattr(resolved.connect(), "execute")                     # PyMySQL executes on cursors only
    assert resolved.schema == "state_db"
    assert seen[0]["user"] == "us@er" and seen[0]["password"] == "p:ss" and seen[0]["database"] == "state_db"
    assert resolve_state_url("mysql://u@h/state_db?schema=other").schema == "other"
