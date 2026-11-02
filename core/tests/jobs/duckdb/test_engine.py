"""DuckDBJob runs through the shared Runner: state in the job's own catalog, replay, run keys, limits, redaction."""

import logging
import os
import sqlite3
from types import MappingProxyType

import duckdb
import pytest
import yaml

from streamwright.core.jobs.duckdb import loading
from streamwright.core.jobs.duckdb.adapter import BUILT_IN_ADAPTERS, ProviderAdapter
from streamwright.core.jobs.duckdb.engine import JobInvalidError, run_job
from streamwright.core.jobs.fields import Field
from streamwright.core.jobs.state import open_sqlite
from streamwright.core.sql.quote import sql_string

EXAMPLE = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "examples", "jobs", "files", "orders.yaml")
SECRET = "pa'ss-SECRET-123"


def _overlay(tmp_path, **runtime):
    path = tmp_path / "overlay.yaml"
    path.write_text(yaml.safe_dump({"connections": {"analytics_warehouse": {"path": str(tmp_path / "wh.duckdb")},
                                                    "lake_export": {"roots": [str(tmp_path / "lake")]}},
                                    "runtime": dict({"state": {"connection": "analytics_warehouse"}}, **runtime)}))
    return str(path)


def test_the_example_runs_twice_with_its_state_in_its_own_warehouse(tmp_path):
    for expected in ("files_orders-00000001", "files_orders-00000002"):
        result = run_job(EXAMPLE, overlays=[_overlay(tmp_path)], config={"min_amount_usd": "20"})
        assert result.exit_code == 0, result.error
        assert result.batches == [expected] and result.outputs == {"warehouse_fact_orders": 5,
                                                                   "country_lake_export": 5}
    with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as con:
        assert con.execute("SELECT count(*) FROM main.fact_orders").fetchall() == [(5,)]
        assert con.execute("SELECT status FROM streamwright.batches ORDER BY batch_sequence").fetchall() == [
            ("completed",), ("completed",)]


def _two_outputs(tmp_path, fail_second):
    second = "SELECT * FROM missing_table" if fail_second else "SELECT 2 AS id"
    return {"kind": "DuckDBJob", "name": "replay",
            "connections": {"wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")}},
            "steps": [{"name": "data", "sql": "SELECT 1 AS id"}],
            "outputs": [{"name": "first", "connection": "wh", "step": "data", "table": "a", "mode": "append"},
                        {"name": "second", "connection": "wh", "sql": second, "table": "b", "mode": "overwrite"}],
            "runtime": {"state": {"sqlite": str(tmp_path / "state.db")}}}


def test_a_retry_skips_outputs_that_have_a_receipt(tmp_path):
    with pytest.raises(JobInvalidError):                       # an unknown step is rejected before anything runs
        run_job(dict(_two_outputs(tmp_path, True), steps=[{"name": "data", "sql": "SELECT 1 AS id"}],
                     outputs=[{"name": "first", "connection": "wh", "step": "nope", "table": "a", "mode": "append"}]))
    failed = run_job(_two_outputs(tmp_path, True))
    assert failed.exit_code == 1 and "missing_table" in failed.error
    store = open_sqlite(str(tmp_path / "state.db"))
    lease = store.acquire_lease("replay", "operator", 60)
    assert store.retry_batch(lease, failed.batches[0]) == "processing"
    store.release_lease(lease)
    resumed = run_job(_two_outputs(tmp_path, False), resume_only=True)
    assert resumed.exit_code == 0 and resumed.outputs == {"first": 1, "second": 1}
    with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as con:
        assert con.execute("SELECT count(*) FROM a").fetchall() == [(1,)]         # appended once, not twice


def test_a_run_key_runs_once(tmp_path):
    job = _two_outputs(tmp_path, False)
    job["runtime"]["state"]["run_key"] = "2026-11-01"
    assert run_job(job).batches == ["replay-00000001"]
    again = run_job(job)
    assert again.exit_code == 0 and again.batches == []


def test_the_timeout_stops_the_run_and_fails_the_batch(tmp_path):
    job = _two_outputs(tmp_path, False)
    job["steps"][0] = {"name": "data", "materialize": "table",
                       "sql": "SELECT count(*) AS id FROM range(100000000) a, range(1000) b"}
    job["runtime"]["timeout_seconds"] = 1
    result = run_job(job)
    assert result.exit_code == 1 and "INTERRUPT" in result.error.upper()


class LeakyAdapter(ProviderAdapter):
    """A driver that echoes its credential in errors (the path of a missing file)."""
    provider = "leaky"
    connection_fields = MappingProxyType({"folder": Field("string", required=True)})
    secret_keys = ("folder",)
    reads = MappingProxyType({"csv": ("path",)})

    def compile_read(self, context, step):
        return "SELECT * FROM read_csv(%s)" % sql_string(os.path.join(context.connection["folder"], step["path"]))


def test_secret_values_are_redacted_from_errors_state_and_logs(tmp_path, caplog, monkeypatch):
    monkeypatch.setattr(loading, "BUILT_IN_ADAPTERS", MappingProxyType(dict(BUILT_IN_ADAPTERS, leaky=LeakyAdapter)))
    job = {"kind": "DuckDBJob", "name": "leaky",
           "spec": {"secrets": {"folder": {"type": "string"}}},
           "connections": {"lake": {"provider": "leaky", "folder": "{{ secrets.folder }}"},
                           "wh": {"provider": "duckdb", "path": ":memory:"}},
           "steps": [{"name": "raw", "connection": "lake", "read": "csv", "path": "missing.csv"}],
           "outputs": [{"name": "o", "connection": "wh", "step": "raw", "table": "t", "mode": "overwrite"}],
           "runtime": {"state": {"sqlite": str(tmp_path / "state.db")}}}
    with caplog.at_level(logging.INFO):
        result = run_job(job, secrets={"folder": str(tmp_path / SECRET)})
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        stored = con.execute("SELECT error_message FROM streamwright_batches").fetchone()[0]
    assert result.exit_code == 1
    for text in (result.error, stored, caplog.text):
        assert "SECRET-123" not in text
    assert "***" in stored
