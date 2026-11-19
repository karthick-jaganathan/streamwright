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

"""spec.lookups and runtime.fan_out: formats, the lookup kinds, exec opt-in, and DuckDB runs per account."""

import json
import pathlib
import re
import sqlite3
import sys
import threading

import duckdb
import pytest
import yaml

from streamwright.core.jobs.duckdb.cli import main
from streamwright.core.jobs.lookups import (
    LookupExecNotAllowedError,
    LookupFailedError,
    Lookups,
    LookupValueError,
    extract,
    parse_output,
)
from streamwright.core.runtime.logs import bind, current_context, log_context
from streamwright.core.runtime.logs.context import context_tag

_SECRET_SCRIPT = """import json, sys
name = sys.argv[sys.argv.index("-s") + 1]
if name == "fail":
    sys.stderr.write("vault says no\\n"); sys.exit(3)
print(json.dumps(json.dumps({"credentials": {"clientId": "cid-" + name, "clientSecret": "s3cr3t"}})))
"""


def test_outputs_parse_as_text_json_or_json_in_a_string_and_fields_pick_their_part():
    assert parse_output("  tok123\n") == "tok123"
    assert parse_output('{"a": 1}') == {"a": 1}
    assert parse_output('"{\\"a\\": {\\"b\\": [\\"x\\"]}}"') == {"a": {"b": ["x"]}}
    assert parse_output('"plain"') == "plain" and parse_output("123", "text") == "123"
    with pytest.raises(LookupValueError, match="not JSON"):
        parse_output("nope", "json")
    holder = {"status": "ok", "auth": '{"client_id": "c1", "items": [{"id": 7}]}'}
    assert extract(holder, "auth.client_id") == "c1" and extract(holder, "auth.items[0].id") == 7
    with pytest.raises(LookupValueError, match="auth has no 'missing'"):
        extract(holder, "auth.missing")


def _accounts_db(path):
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE accounts (account_id TEXT, client_id TEXT, login TEXT, token_b64 TEXT)")
        conn.executemany("INSERT INTO accounts VALUES (?, ?, ?, ?)", [
            ("a1", "c1", "one@x", "dG9rLWEx"), ("a2", "c1", "two@x", "dG9rLWEy"), ("a3", "c2", "three@x", "dG9rLWEz"),
            ("a0", "c9", "bad@x", "***not base64***"), ("a4", "c9", "four@x", "dG9rLWE0")])
    return str(path)


def _lookups(tmp_path, **overrides):
    lookups = {
        "accounts": {"type": "database", "connection": "creds", "rows": "many", "secret_fields": ["token_b64"],
                     "query": "SELECT account_id, login, token_b64 FROM accounts WHERE (:client_id IS NULL OR "
                              "client_id = :client_id) AND (:account_ids IS NULL OR account_id IN :account_ids) "
                              "ORDER BY account_id",
                     "params": {"client_id": "{{ config.client_id }}", "account_ids": "{{ config.account_ids }}"},
                     "transforms": {"token": {"type": "builtin", "apply": "base64", "value": "{{ row.token_b64 }}"}}},
        "app": {"type": "command", "command": [sys.executable, str(tmp_path / "secrets_cli.py"), "-s",
                                               "{{ config.client_id }}"]},
    }
    lookups.update(overrides)
    return lookups


def _job(tmp_path, **extra):
    (tmp_path / "secrets_cli.py").write_text(_SECRET_SCRIPT)
    job = {"kind": "DuckDBJob", "name": "per_account",
           "spec": {"config": {"client_id": {"type": "string", "required": False},
                               "account_ids": {"type": "list", "items": "string", "required": False},
                               "account_id": {"type": "string", "lookup": {"from": "account", "field": "account_id"}},
                               "login": {"type": "string", "lookup": {"from": "account", "field": "login"}}},
                    "secrets": {"refresh_token": {"type": "string", "lookup": {"from": "account", "field": "token"}},
                                "client_secret": {"type": "string",
                                                  "lookup": {"from": "app", "field": "credentials.clientSecret"}}},
                    "lookups": _lookups(tmp_path)},
           "connections": {"creds": {"provider": "sqlite", "path": _accounts_db(tmp_path / "creds.db")},
                           "wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")}},
           "steps": [{"name": "s",
                      "sql": "SELECT '{{ config.account_id }}' AS account, '{{ config.login }}' AS login"}],
           "outputs": [{"name": "o", "connection": "wh", "step": "s", "table": "t", "mode": "append"}],
           "runtime": {"fan_out": {"over": "accounts", "as": "account"},
                       "state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True,
                                 "scope": "account:{{ config.account_id }}"}}}
    job.update(extra)
    path = tmp_path / "job.yaml"
    path.write_text(yaml.safe_dump(job))
    return str(path), job


def _rows(tmp_path):
    with duckdb.connect(str(tmp_path / "wh.duckdb")) as conn:
        return conn.execute("SELECT account, login FROM t ORDER BY account").fetchall()


def test_a_database_lookup_binds_lists_and_nulls_and_masks_its_secret_columns_and_transforms(tmp_path):
    _, job = _job(tmp_path)
    masked = []
    lookups = Lookups(job, {"account_ids": "a2,a3"}, {"client_secret": "given"}, enrol=masked.append)
    items = lookups.items()
    assert [label for label, _ in items] == ["a2", "a3"]
    config, secrets = lookups.inputs(items[1][1])
    assert (config["account_id"], config["login"], secrets["refresh_token"]) == ("a3", "three@x", "tok-a3")
    assert secrets["client_secret"] == "given" and {"dG9rLWEz", "tok-a3"} <= set(masked)  # explicit wins
    assert [label for label, _ in Lookups(job, {"client_id": "c1", "account_ids": "a2,a3"}, {}).items()] == ["a2"]
    unopted = Lookups(job, {"account_ids": "a1"}, {})
    with pytest.raises(LookupExecNotAllowedError, match="runs a command: allow it with --allow-lookup-exec"):
        unopted.inputs(unopted.items()[0][1])        # client_secret needs the command lookup


def test_a_command_lookup_gets_no_shell_and_fails_by_name_without_its_stderr(tmp_path):
    _, job = _job(tmp_path)
    job["spec"]["lookups"]["app"]["command"][-1] = "{{ config.vault }}"
    job["spec"]["config"]["vault"] = {"type": "string", "required": False}
    job["spec"]["config"]["oauth_id"] = {"type": "string", "lookup": {"from": "app", "field": "credentials.clientId"}}
    allowed = Lookups(job, {"account_ids": "a1", "vault": "x; echo pwned"}, {}, allow_exec=True)
    config, _ = allowed.inputs(allowed.items()[0][1])
    assert config["oauth_id"] == "cid-x; echo pwned"        # one argument, never a shell
    failing = Lookups(job, {"vault": "fail", "account_ids": "a1"}, {}, allow_exec=True)
    with pytest.raises(LookupFailedError) as raised:
        failing.inputs(failing.items()[0][1])
    assert "failed with exit code 3" in str(raised.value) and "vault says no" not in str(raised.value)


def test_fan_out_runs_each_account_in_its_own_scope_by_client_or_by_id(tmp_path, capsys):
    path, _ = _job(tmp_path)
    summary = tmp_path / "summary.json"
    assert main(["run", path, "--set", "client_id=c1", "--allow-lookup-exec", "--summary", str(summary)]) == 0
    written = json.loads(summary.read_text())
    assert written["fan_out"] == {"over": "accounts", "as": "account"}
    assert [(p["item"], p["status"]) for p in written["passes"]] == [("a1", "ok"), ("a2", "ok")]
    assert "s3cr3t" not in summary.read_text() and "tok-a1" not in capsys.readouterr().err
    assert main(["run", path, "--set", "account_ids=a3", "--allow-lookup-exec"]) == 0       # one account
    assert _rows(tmp_path) == [("a1", "one@x"), ("a2", "two@x"), ("a3", "three@x")]
    assert main(["state", "list", path]) == 0
    assert {line.split()[4] for line in capsys.readouterr().out.splitlines()[1:]} == {
        "account:a1", "account:a2", "account:a3"}
    assert main(["run", path, "--set", "client_id=c1"]) == 2                               # no opt-in
    assert "--allow-lookup-exec" in capsys.readouterr().err


def test_one_failing_account_does_not_stop_the_others_unless_on_failure_stop(tmp_path, capsys):
    path, _ = _job(tmp_path)
    assert main(["run", path, "--set", "client_id=c9", "--allow-lookup-exec"]) == 1
    err = capsys.readouterr().err
    assert "account a0 failed: LookupFailedError" in err and "account a4 ok" in err
    assert "1 of 2 account(s) failed or did not run: a0" in err
    assert main(["run", path, "--set", "client_id=c9", "--allow-lookup-exec", "--fan-out-on-failure", "stop"]) == 1
    assert "account a4 not run" in capsys.readouterr().err
    assert main(["run", path, "--set", "client_id=none", "--allow-lookup-exec"]) == 1     # on_empty: fail
    assert "gave no item" in capsys.readouterr().err


def test_validate_names_what_a_binding_or_fan_out_gets_wrong(tmp_path, capsys):
    path, job = _job(tmp_path)
    job["runtime"]["state"]["scope"] = "all"
    job["spec"]["secrets"]["client_secret"]["lookup"]["from"] = "vault"
    (tmp_path / "bad.yaml").write_text(yaml.safe_dump(job))
    assert main(["validate", str(tmp_path / "bad.yaml")]) == 1
    err = capsys.readouterr().err
    assert "no lookup 'vault' in spec.lookups" in err and "the scope must name the item" in err
    assert main(["validate", path]) == 0


def test_the_log_context_takes_only_its_keys_cleans_values_nests_and_follows_bound_threads():
    with pytest.raises(ValueError, match="unknown log context key"), log_context(client="x"):
        pass
    with log_context(job="j", labels={"tenant": "t1"}), log_context(item="account=a1", step="line\nbreak"):
        assert dict(current_context()) == {"job": "j", "labels": {"tenant": "t1"}, "item": "account=a1",
                                           "step": "line break"}
        assert context_tag(current_context()) == " [j tenant=t1 account=a1 step=line break]"
        seen = []
        thread = threading.Thread(target=bind(lambda: seen.append(current_context().get("item"))))
        thread.start()
        thread.join()
        assert seen == ["account=a1"]
    assert dict(current_context()) == {}


def test_each_pass_tags_its_own_lines_with_the_job_its_labels_and_its_account(tmp_path, capsys):
    path, job = _job(tmp_path)
    assert main(["run", path, "--set", "client_id=c1", "--allow-lookup-exec", "--fan-out-concurrency", "2"]) == 0
    assert "share the DuckDB database of connection(s) wh: they run one at a time" in capsys.readouterr().err
    job["runtime"]["log"] = {"labels": {"client": "{{ config.client_id }}"}}
    job["connections"]["wh"] = {"provider": "files", "roots": [str(tmp_path / "out")]}
    job["outputs"] = [{"name": "o", "connection": "wh", "step": "s", "mode": "overwrite", "format": "csv",
                       "path": str(tmp_path / "out"), "split_by": ["account"], "file_name": "{account}.csv"}]
    pathlib.Path(path).write_text(yaml.safe_dump(job))
    assert main(["run", path, "--set", "client_id=c1", "--allow-lookup-exec", "--fan-out-concurrency", "2"]) == 0
    tags = {line.split(": ", 1)[0].split("streamwright.duckdb", 1)[1] for line in capsys.readouterr().err.splitlines()
            if "output o: 1 rows" in line}
    assert {re.sub(r" req=sw_[A-Za-z0-9_-]{22}]$", "]", tag) for tag in tags} == {
        " [per_account client=c1 account=a1 output=o]", " [per_account client=c1 account=a2 output=o]"}
    assert len({tag.rsplit(" req=", 1)[1] for tag in tags}) == 1              # one run: one request id
    assert main(["run", path, "--set", "account_ids=a3", "--allow-lookup-exec", "--log-format", "json"]) == 0
    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines() if line.startswith("{")]
    output = next(line for line in lines if line["message"].startswith("output o: 1 rows"))
    assert (output["job"], output["item"], output["output"], output["job_id"].startswith("per_account-")) == (
        "per_account", "account=a3", "o", True)
    assert "labels" not in output                       # client_id not given: the label stays out


def test_labels_use_only_declared_config_inputs(tmp_path, capsys):
    _, job = _job(tmp_path)
    job["runtime"]["log"] = {"labels": {"client": "{{ secrets.client_secret }}", "Bad-Name": "x", "job": "y"}}
    job["runtime"]["fan_out"]["key"] = "token_b64"
    (tmp_path / "labels.yaml").write_text(yaml.safe_dump(job))
    assert main(["validate", str(tmp_path / "labels.yaml")]) == 1
    err = capsys.readouterr().err
    assert "{{ secrets.client_secret }}: a label uses only declared {{ config.* }} inputs" in err
    assert err.count("a label name is lowercase") == 2 and "'token_b64' is a secret field" in err
