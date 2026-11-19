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

import json
import os

import pytest
import yaml

from streamwright.core.jobs.duckdb.cli import main
from streamwright.core.jobs.state import open_sqlite

EXAMPLE = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "examples", "files", "jobs", "orders",
                       "sync.yaml")


def _job(tmp_path, **extra):
    job = dict({"kind": "DuckDBJob", "name": "cli_job",
                "spec": {"config": {"limit": {"type": "integer", "default": 1}},
                         "secrets": {"password": {"type": "string"}}},
                "connections": {"wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")}},
                "steps": [{"name": "s", "sql": "SELECT {{ config.limit }} AS n"}],
                "outputs": [{"name": "o", "connection": "wh", "step": "s", "table": "t", "mode": "overwrite"}],
                "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}}}, **extra)
    path = tmp_path / "job.yaml"
    path.write_text(yaml.safe_dump(job))
    return str(path)


def test_validate_and_plan_need_no_secrets_and_report_issues(tmp_path, capsys):
    assert main(["validate", EXAMPLE, "--graph"]) == 0
    assert "harmonized_orders" in capsys.readouterr().out
    assert main(["plan", _job(tmp_path)]) == 0
    bad = _job(tmp_path, outputs=[{"name": "o", "connection": "wh", "step": "s", "table": "t"}])
    assert main(["validate", bad]) == 1
    assert "[missing-key] outputs[0]" in capsys.readouterr().err


def test_inputs_errors_are_usage_errors(tmp_path, capsys):
    config = tmp_path / "config.yaml"
    config.write_text("config: {limit: 3}\nstreams: [a]\n")
    assert main(["validate", _job(tmp_path), "--config", str(config)]) == 2
    with pytest.raises(SystemExit) as usage:
        main(["validate", _job(tmp_path), "--set", "limit"])
    assert usage.value.code == 2
    assert main(["validate", _job(tmp_path), "--set", "nope=1"]) == 1
    assert "unknown config: nope" in capsys.readouterr().err


def test_jobs_take_the_logging_options_of_streamwright_run(tmp_path, capsys):
    # --log is its own option: not a prefix of --log-level
    assert main(["validate", _job(tmp_path), "--log-level", "debug", "--log", "streamwright.network=DEBUG",
                 "--log", "vendor.sdk=INFO", "--log-format", "json", "--log-max-chars", "100"]) == 0
    with pytest.raises(SystemExit) as usage:
        main(["validate", _job(tmp_path), "--log", "DEBUG"])
    assert usage.value.code == 2 and "expected NAME=LEVEL" in capsys.readouterr().err


def test_the_job_is_a_path_or_the_job_option(tmp_path, capsys):
    job = _job(tmp_path)
    assert main(["validate", "--job", job]) == 0 and main(["plan", "-j", job]) == 0
    assert main(["validate", job, "--job", job]) == 0
    for argv in (["validate"], ["validate", job, "--job", str(tmp_path / "other.yaml")]):
        with pytest.raises(SystemExit) as usage:
            main(argv)
        assert usage.value.code == 2
    error = capsys.readouterr().err
    assert "a job manifest is required" in error and "pass the job once" in error


def test_run_then_list_state(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("STREAMWRIGHT_SECRET_PASSWORD", "x")
    config = tmp_path / "config.yaml"
    config.write_text("config: {limit: 3}\n")
    assert main(["run", _job(tmp_path), "--config", str(config), "--set", "limit=4"]) == 0
    assert "job cli_job finished: 1 batch(es) (cli_job-00000001); o 1 rows" in capsys.readouterr().err
    assert main(["state", "list", _job(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "cli_job-00000001" in out and "completed" in out
    assert main(["state", "retry", _job(tmp_path), "cli_job-00000001", "--log-level", "DEBUG"]) == 1
    assert "is not failed" in capsys.readouterr().err
    with pytest.raises(SystemExit) as usage:                       # only retry, which runs the job, takes logging
        main(["state", "list", _job(tmp_path), "--log-level", "DEBUG"])
    assert usage.value.code == 2


def test_a_missing_required_secret_fails_the_run_without_running(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("STREAMWRIGHT_SECRET_PASSWORD", raising=False)
    assert main(["run", _job(tmp_path)]) == 2
    assert "missing required secrets 'password'" in capsys.readouterr().err


def test_state_lock_options_fail_fast_without_persistent_state(tmp_path, capsys, monkeypatch):
    job = _job(tmp_path, runtime={})
    monkeypatch.setenv("STREAMWRIGHT_SECRET_PASSWORD", "x")
    assert main(["run", job, "--state-lock-policy", "prune"]) == 2
    err = capsys.readouterr().err
    assert "need a persistent runtime.state" in err

    assert main(["run", job, "--state-lock-timeout", "10s"]) == 2
    err = capsys.readouterr().err
    assert "need a persistent runtime.state" in err


def test_state_lock_policy_prune_requires_itemized_locking(tmp_path, capsys, monkeypatch):
    job = _job(tmp_path)
    monkeypatch.setenv("STREAMWRIGHT_SECRET_PASSWORD", "x")
    assert main(["run", job, "--state-lock-policy", "prune"]) == 2
    err = capsys.readouterr().err
    assert "lock policy prune needs runtime.state.lock.each" in err


def test_state_lock_timeout_with_fail_policy_fails(tmp_path, capsys, monkeypatch):
    job = _job(tmp_path)
    monkeypatch.setenv("STREAMWRIGHT_SECRET_PASSWORD", "x")
    assert main(["run", job, "--state-lock-timeout", "30s", "--state-lock-policy", "fail"]) == 2
    err = capsys.readouterr().err
    assert "--state-lock-timeout needs lock policy wait" in err


def test_cli_itemized_locking_prune_and_wait(tmp_path, capsys, monkeypatch):
    state_file = str(tmp_path / "multi_state.db")
    job = {
        "kind": "DuckDBJob",
        "name": "multi_acc_job",
        "spec": {
            "config": {
                "client": {"type": "string", "default": "clientA"},
                "accounts": {"type": "string", "default": "1,2,3"},
            },
        },
        "connections": {"wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")}},
        "steps": [{"name": "s", "sql": "SELECT '{{ config.accounts }}' AS accs"}],
        "outputs": [{"name": "o", "connection": "wh", "step": "s", "table": "t", "mode": "overwrite"}],
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
    path = tmp_path / "itemized.yaml"
    path.write_text(yaml.safe_dump(job))

    # Pre-lock token clientA:2 in state store
    store = open_sqlite(state_file)
    store.initialize()
    lease_2 = store.acquire_lease("multi_acc_job", "other_runner", 300, tokens=("clientA:2",))

    # Run with default policy (fail): should fail because clientA:2 is held
    assert main(["run", str(path)]) == 1
    err = capsys.readouterr().err
    assert "lease is held" in err or "lease_held" in err

    # Run with --state-lock-policy prune: should prune clientA:2 and successfully run clientA:1 and clientA:3
    assert main(["run", str(path), "--state-lock-policy", "prune"]) == 0
    assert "job multi_acc_job finished: 1 batch(es)" in capsys.readouterr().err

    # Now lock clientA:1 and clientA:3 as well
    lease_1_3 = store.acquire_lease("multi_acc_job", "other_runner", 300, tokens=("clientA:1", "clientA:3"))

    # Run with --state-lock-policy prune: all tokens are held, exits 0 cleanly and is recorded as skipped
    assert main(["run", str(path), "--state-lock-policy", "prune", "--summary"]) == 0
    assert "job multi_acc_job skipped: the lock tokens it needs are held" in capsys.readouterr().err
    assert store.list_batches("multi_acc_job", limit=1)[0]["status"] == "skipped"

    # Release locks
    store.release_lease(lease_2)
    store.release_lease(lease_1_3)

    # Now run again with default policy: succeeds
    assert main(["run", str(path)]) == 0



def test_a_run_applies_state_migrations_only_when_asked(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("STREAMWRIGHT_SECRET_PASSWORD", "x")
    job = _job(tmp_path, runtime={"state": {"sqlite": str(tmp_path / "state.db")}})     # the default: check only
    assert main(["run", job]) == 1
    error = capsys.readouterr().err
    assert "the state tables in sqlite file %s need migrations: 0001_initial_schema" % (tmp_path / "state.db") in error
    assert main(["run", job, "--log-format", "json"]) == 1                 # a log line, in the run's log format
    line = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert (line["level"], line["logger"], line["event"], line["status"]) == ("ERROR", "streamwright.duckdb",
                                                                               "state_error", "failed")
    assert line["message"].startswith("the state tables in sqlite file")
    assert main(["state", "list", job]) == 1                       # state actions do not migrate either
    assert not open_sqlite(str(tmp_path / "state.db")).check_schema()
    assert main(["state", "migrate", job]) == 0
    assert main(["run", job]) == 0
    other = tmp_path / "other"
    other.mkdir()
    flagged = _job(other, runtime={"state": {"sqlite": str(other / "state.db")}})
    assert main(["run", flagged, "--apply-state-migrations"]) == 0
    third = tmp_path / "third"
    third.mkdir()
    assert main(["run", _job(third)]) == 0                         # the helper's runtime.state.apply_migrations: true

def test_a_failed_batch_lists_on_one_line_with_the_first_line_of_its_error(tmp_path, capsys):
    job = _job(tmp_path, spec={}, steps=[{"name": "s", "sql": "SELECT error('first line' || chr(10) || 'second')"}])
    assert main(["run", job]) == 1
    capsys.readouterr()
    assert main(["state", "list", job]) == 0
    header, row = capsys.readouterr().out.splitlines()
    assert header.startswith("JOB_ID") and "failed" in row and "first line" in row and "second" not in row


def test_the_failed_job_policy_abandons_by_default_and_can_retry_or_block(tmp_path, capsys):
    job = _job(tmp_path, spec={}, steps=[{"name": "s", "sql": "SELECT error('boom')"}])
    retry = ["--failed-job-policy", "retry", "--max-job-attempts", "2"]
    assert main(["run", job]) == 1                                                  # attempt 1
    assert main(["run", job, "--failed-job-policy", "block"]) == 1
    assert "is not retried (failed job policy block)" in capsys.readouterr().err
    assert main(["run", job, *retry]) == 1                                          # attempt 2
    assert "retry: job_id=cli_job-00000001, attempt=2 of 2" in capsys.readouterr().err
    assert main(["run", job, *retry]) == 1
    assert "is not retried (started 2 time(s), max job attempts 2)" in capsys.readouterr().err
    assert main(["state", "list", job]) == 0
    header, row = capsys.readouterr().out.splitlines()
    assert header.split()[:4] == ["JOB_ID", "STATUS", "SEQUENCE", "ATTEMPTS"] and row.split()[:4] == [
        "cli_job-00000001", "failed", "1", "2"]
    assert main(["run", job]) == 1                       # the default abandons it; the new batch fails in turn
    assert "abandoned: job_id=cli_job-00000001, reason=failed job policy abandon" in capsys.readouterr().err
    assert [line.split()[1] for line in _listed(job, capsys)] == ["failed", "abandoned"]
    assert main(["run", job, "--max-job-attempts", "2"]) == 2
    assert "--max-job-attempts needs failed job policy retry (got abandon)" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        main(["run", job, "--max-job-attempts", "0"])
    assert "give a whole number of at least 1" in capsys.readouterr().err
    state = {"sqlite": str(tmp_path / "state.db")}
    assert main(["validate", _job(tmp_path, runtime={"state": {**state, "failed_job_policy": "skip"}})]) == 1
    assert "runtime.state.failed_job_policy" in capsys.readouterr().err
    assert main(["validate", _job(tmp_path, runtime={"state": {**state, "max_job_attempts": 5}})]) == 1
    assert "max_job_attempts needs failed_job_policy: retry (the policy is abandon)" in capsys.readouterr().err


def _listed(job, capsys):
    assert main(["state", "list", job]) == 0
    return capsys.readouterr().out.splitlines()[1:]


def _scoped(tmp_path):
    return _job(tmp_path, spec={"config": {"client": {"type": "string"}}},
                steps=[{"name": "s", "sql": "SELECT '{{ config.client }}' AS c"}],
                runtime={"state": {"sqlite": str(tmp_path / "state.db"), "scope": "client:{{ config.client }}",
                                   "apply_migrations": True}})


def test_state_actions_take_only_their_options(tmp_path, capsys):
    job = _scoped(tmp_path)
    assert main(["state", "check", job, "--set", "client=a"]) == 1          # check never creates the tables
    assert not os.path.exists(tmp_path / "state.db") or open_sqlite(str(tmp_path / "state.db")).schema_version() is None
    assert main(["state", "migrate", job, "--set", "client=a"]) == 0
    assert main(["state", "check", job, "--set", "client=a"]) == 0
    for client in ("a", "b"):
        assert main(["run", job, "--set", "client=%s" % client]) == 0
    capsys.readouterr()
    assert main(["state", "list", job, "--set", "client=a", "--scope", "client:b", "--json"]) == 0
    listed = [line for line in capsys.readouterr().out.splitlines() if line]
    assert len(listed) == 1 and '"scope": "client:b"' in listed[0]
    assert main(["state", "status", job, "--set", "client=a"]) == 0
    job_id = capsys.readouterr().out.splitlines()[-1].split()[0]
    assert main(["state", "show", "--job", job, job_id, "--set", "client=a"]) == 0    # --job JOB JOB_ID
    shown = json.loads(capsys.readouterr().out)
    assert (shown["scope"], shown["job_id"], shown["sequence"]) == ("client:a", job_id, 1) and "batch_id" not in shown
    assert main(["state", "purge", job, "--older-than", "0s", "--scope", "client:b", "--set", "client=a"]) == 0
    assert "purged 1" in capsys.readouterr().out
    for wrong in (["state", "list", job, "--strict"], ["state", "list", job, "--job-id", job_id],
                  ["state", "migrate", job, "--limit", "3"]):
        with pytest.raises(SystemExit) as usage:
            main(wrong)
        assert usage.value.code == 2
    with pytest.raises(SystemExit):
        main(["state", "retry", job, "--set", "client=a"])                     # no job id
    assert "acts on one batch: give its job id" in capsys.readouterr().err


def test_a_job_key_from_the_command_line_runs_once_per_scope(tmp_path, capsys):
    job = _scoped(tmp_path)
    for client, expected in (("a", 1), ("a", 0), ("b", 1)):
        assert main(["run", job, "--set", "client=%s" % client, "--job-key", "2026-11-13"]) == 0
        assert len(open_sqlite(str(tmp_path / "state.db")).list_batches("cli_job", scope="client:%s" % client)) \
            == (1 if client == "a" else expected)
    no_state = _job(tmp_path, spec={"config": {"limit": {"type": "integer", "default": 1}}}, runtime={})
    assert main(["run", no_state, "--job-key", "k"]) == 2
    assert "--job-key needs a persistent runtime.state" in capsys.readouterr().err


def test_a_connector_that_is_not_installed_is_named_once_with_how_to_install_it(tmp_path, capsys):
    job = _job(tmp_path, spec={}, connections={"raw": {"provider": "nosuch", "roots": ["data"]},
                                      "wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")}},
               steps=[{"name": "s", "connection": "raw", "read": "parquet", "path": "a.parquet"},
                      {"name": "t", "connection": "raw", "read": "csv", "path": "b.csv"}])
    assert main(["run", job]) == 1
    err = capsys.readouterr().err
    assert ("provider 'nosuch' of connection 'raw' (used by steps[0], steps[1]) is not installed: install it with "
            "`streamwright connectors install nosuch`") in err
    assert "is not one of" not in err and "does not use `path`" not in err
    assert main(["validate", job]) == 0                    # offline it is a warning: the connector may come later
    assert "warning [connector-not-installed]" in capsys.readouterr().err
