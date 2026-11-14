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

"""DuckDBJob runs through the shared Runner: state in the job's own catalog, replay, job keys, limits, redaction."""

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
                                    "runtime": dict({"state": {"connection": "analytics_warehouse",
                                                               "apply_migrations": True}}, **runtime)}))
    return str(path)


def test_the_example_runs_twice_with_its_state_in_its_own_warehouse(tmp_path):
    for expected in ("files_orders-00000001", "files_orders-00000002"):
        result = run_job(EXAMPLE, overlays=[_overlay(tmp_path)], config={"min_amount_usd": "20"})
        assert result.exit_code == 0, result.error
        assert result.batches == [expected]
        assert result.outputs == {"warehouse_fact_orders": 5, "country_lake_export": 5}
    with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as con:
        assert con.execute("SELECT count(*) FROM main.fact_orders").fetchall() == [(5,)]
        assert con.execute("SELECT status FROM streamwright.sw_jobs ORDER BY sequence").fetchall() == [
            ("completed",), ("completed",)]


def _two_outputs(tmp_path, fail_second):
    second = "SELECT * FROM missing_table" if fail_second else "SELECT 2 AS id"
    return {"kind": "DuckDBJob", "name": "replay",
            "connections": {"wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")}},
            "steps": [{"name": "data", "sql": "SELECT 1 AS id"}],
            "outputs": [{"name": "first", "connection": "wh", "step": "data", "table": "a", "mode": "append"},
                        {"name": "second", "connection": "wh", "sql": second, "table": "b", "mode": "overwrite"}],
            "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}}}


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


def test_a_job_key_runs_once(tmp_path):
    job = _two_outputs(tmp_path, False)
    job["runtime"]["state"]["job_key"] = "2026-11-01"
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
           "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}}}
    with caplog.at_level(logging.INFO):
        result = run_job(job, secrets={"folder": str(tmp_path / SECRET)})
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        stored = con.execute("SELECT error_message FROM sw_jobs").fetchone()[0]
    assert result.exit_code == 1
    for text in (result.error, stored, caplog.text):
        assert "SECRET-123" not in text
    assert "***" in stored


def test_engine_itemized_multi_resource_pruning_rewrites_query_and_executes(tmp_path):
    state_file = str(tmp_path / "state.db")
    wh_file = str(tmp_path / "wh.duckdb")
    job = {
        "kind": "DuckDBJob",
        "name": "prune_job",
        "spec": {
            "config": {
                "client": {"type": "string", "default": "clientA"},
                "accounts": {"type": "string", "default": "1,2,3"},
            },
        },
        "connections": {"wh": {"provider": "duckdb", "path": wh_file}},
        "steps": [{"name": "s", "sql": "SELECT '{{ config.accounts }}' AS processed"}],
        "outputs": [{"name": "o", "connection": "wh", "step": "s", "table": "processed_accounts", "mode": "overwrite"}],
        "runtime": {
            "state": {
                "sqlite": state_file,
                "apply_migrations": True,
                "lock": {
                    "each": "{{ config.accounts }}",
                    "token": "{{ config.client }}:{{ item }}",
                    "policy": "prune",
                },
            },
        },
    }

    # Pre-lock clientA:2
    store = open_sqlite(state_file)
    store.initialize()
    lease_2 = store.acquire_lease("prune_job", "other_worker", 300, tokens=("clientA:2",))

    result = run_job(job, config={"client": "clientA", "accounts": "1,2,3"})
    assert result.exit_code == 0
    assert result.outputs == {"o": 1}

    with duckdb.connect(wh_file, read_only=True) as con:
        rows = con.execute("SELECT processed FROM processed_accounts").fetchall()
        assert rows == [("1,3",)]

    store.release_lease(lease_2)


def test_engine_itemized_multi_resource_disjoint_runs_concurrently(tmp_path):
    state_file = str(tmp_path / "state.db")
    wh_file = str(tmp_path / "wh.duckdb")
    job = {
        "kind": "DuckDBJob",
        "name": "multi_run_job",
        "spec": {
            "config": {
                "client": {"type": "string", "default": "clientA"},
                "accounts": {"type": "string", "default": "1,2"},
            },
        },
        "connections": {"wh": {"provider": "duckdb", "path": wh_file}},
        "steps": [{"name": "s", "sql": "SELECT '{{ config.accounts }}' AS processed"}],
        "outputs": [{"name": "o", "connection": "wh", "step": "s", "table": "processed_accounts", "mode": "append"}],
        "runtime": {
            "state": {
                "sqlite": state_file,
                "apply_migrations": True,
                "lock": {
                    "each": "{{ config.accounts }}",
                    "token": "{{ config.client }}:{{ item }}",
                    "policy": "fail",
                },
            },
        },
    }

    store = open_sqlite(state_file)
    store.initialize()
    # Acquire tokens 1 and 2
    lease_1_2 = store.acquire_lease("multi_run_job", "worker_1", 300, tokens=("clientA:1", "clientA:2"))

    # Disjoint accounts 3 and 4 should run concurrently without issue
    result = run_job(job, config={"client": "clientA", "accounts": "3,4"})
    assert result.exit_code == 0
    assert result.outputs == {"o": 1}

    # Overlapping accounts 2 and 5 should fail under policy "fail"
    result_conflict = run_job(job, config={"client": "clientA", "accounts": "2,5"})
    assert result_conflict.exit_code == 1
    assert "lease is held" in result_conflict.error

    store.release_lease(lease_1_2)

