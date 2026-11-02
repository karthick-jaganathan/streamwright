import os

import pytest
import yaml

from streamwright.core.jobs.duckdb.cli import main

EXAMPLE = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "examples", "jobs", "files", "orders.yaml")


def _job(tmp_path, **extra):
    job = dict({"kind": "DuckDBJob", "name": "cli_job",
                "spec": {"config": {"limit": {"type": "integer", "default": 1}},
                         "secrets": {"password": {"type": "string"}}},
                "connections": {"wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")}},
                "steps": [{"name": "s", "sql": "SELECT {{ config.limit }} AS n"}],
                "outputs": [{"name": "o", "connection": "wh", "step": "s", "table": "t", "mode": "overwrite"}],
                "runtime": {"state": {"sqlite": str(tmp_path / "state.db")}}}, **extra)
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
    assert "o" in capsys.readouterr().out
    assert main(["state", "list", _job(tmp_path)]) == 0
    assert "cli_job-00000001" in capsys.readouterr().out
    assert main(["state", "retry", _job(tmp_path), "--batch", "cli_job-00000001"]) == 1
    assert "is not failed" in capsys.readouterr().err


def test_a_missing_required_secret_fails_the_run_without_running(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("STREAMWRIGHT_SECRET_PASSWORD", raising=False)
    assert main(["run", _job(tmp_path)]) == 2
    assert "missing required secrets 'password'" in capsys.readouterr().err
