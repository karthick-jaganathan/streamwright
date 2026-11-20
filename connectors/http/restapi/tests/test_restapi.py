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

import duckdb
import requests
import yaml

from streamwright.core.jobs.duckdb.engine import run_job
from streamwright.core.jobs.duckdb.plugin import JOB_ENGINE
from streamwright.core.sdk.testing import ERROR, MemoryOutput, SourceRunner, run_cli, validate_text


def test_source_validation_with_restapi():
    source = {
        "kind": "source",
        "name": "generic_rest_source",
        "spec": {
            "secrets": {
                "api_key": {"type": "string"},
            },
        },
        "auth": {
            "provider": "restapi",
            "type": "bearer",
            "token": "{{ secrets.api_key }}",
        },
        "http": {
            "base_url": "https://api.example.com",
        },
        "streams": [
            {
                "name": "items",
                "requests": [
                    {
                        "name": "raw_items",
                        "operation": {
                            "path": "/v1/items",
                            "method": "GET",
                        },
                    },
                ],
                "transform": {
                    "mode": "page",
                    "steps": [
                        {
                            "name": "items",
                            "select": "SELECT record->>'id' AS id FROM raw_items",
                        },
                    ],
                },
                "export": {
                    "items": {
                        "step": "items",
                    },
                },
            },
        ],
    }
    issues = validate_text(yaml.safe_dump(source, sort_keys=False), "source.yaml")
    assert not [i for i in issues if i.severity == ERROR]


def test_restapi_rejects_sdk_requests():
    source = {
        "kind": "source",
        "name": "invalid_sdk_source",
        "auth": {
            "provider": "restapi",
            "type": "bearer",
            "token": "test-token",
        },
        "streams": [
            {
                "name": "items",
                "requests": [
                    {
                        "name": "raw_items",
                        "operation": {"method": "list"},
                    },
                ],
                "transform": {
                    "mode": "page",
                    "steps": [
                        {
                            "name": "items",
                            "select": "SELECT * FROM raw_items",
                        },
                    ],
                },
                "export": {
                    "items": {
                        "step": "items",
                    },
                },
            },
        ],
    }
    issues = validate_text(yaml.safe_dump(source, sort_keys=False), "source.yaml")
    errors = [i for i in issues if i.severity == ERROR]
    assert any("restapi is a REST API connector: its operation is an HTTP request (path, method, params, ...)" in str(e)
               for e in errors)


def test_restapi_runner_execution(monkeypatch):
    source = {
        "kind": "source",
        "name": "test_restapi_run",
        "auth": {
            "provider": "restapi",
            "type": "bearer",
            "token": "secret-token-123",
        },
        "http": {
            "base_url": "https://api.example.com",
            "records": {"path": "data"},
        },
        "streams": [
            {
                "name": "users",
                "requests": [
                    {
                        "name": "raw_users",
                        "operation": {
                            "path": "/users",
                        },
                    },
                ],
                "transform": {
                    "mode": "page",
                    "steps": [
                        {
                            "name": "users",
                            "select": "SELECT (record->>'id')::INT AS user_id, record->>'name' AS user_name "
                                      "FROM raw_users",
                        },
                    ],
                },
                "export": {
                    "users": {
                        "step": "users",
                    },
                },
            },
        ],
    }

    captured_headers = {}

    def mock_send(session_self, request, **kwargs):
        captured_headers.update(request.headers)
        resp = requests.Response()
        resp.status_code = 200
        resp.url = "https://api.example.com/users"
        resp.headers = {"content-type": "application/json"}
        resp._content = json.dumps({"data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}).encode("utf-8")
        return resp

    monkeypatch.setattr(requests.Session, "send", mock_send)

    output = MemoryOutput()
    runner = SourceRunner(source, config={}, secrets={"api_key": "dummy"}, output=output)
    runner.run()

    # Verify authorization header applied by Authenticator
    assert captured_headers.get("Authorization") == "Bearer secret-token-123"

    # Verify rows written to memory output
    rows = [rec for name, rec in output.records if name == "users"]
    assert len(rows) == 2
    assert rows[0] == {"user_id": 1, "user_name": "Alice"}
    assert rows[1] == {"user_id": 2, "user_name": "Bob"}


def test_arbitrary_query_params_reach_the_request(monkeypatch):
    """An undeclared query parameter in the `http.params` block is forwarded verbatim to the real HTTP call."""
    source = {
        "kind": "source", "name": "vendor_params",
        "auth": {"provider": "restapi", "type": "bearer", "token": "secret-token-123"},
        "http": {"base_url": "https://api.example.com", "records": {"path": "data"}},
        "streams": [{
            "name": "items",
            "requests": [{"name": "raw", "operation": {"path": "/items",
                         "params": {"vendor_filter": "enabled", "page_size": 250}}}],
            "transform": {"mode": "page", "steps": [{"name": "items",
                         "select": "SELECT (record->>'id')::INT AS id FROM raw"}]},
            "export": {"items": {"step": "items"}},
        }],
    }
    seen = {}

    def mock_send(session_self, request, **kwargs):
        seen["url"] = request.url
        resp = requests.Response()
        resp.status_code = 200
        resp.url = request.url
        resp.headers = {"content-type": "application/json"}
        resp._content = json.dumps({"data": [{"id": 1}]}).encode("utf-8")
        return resp

    monkeypatch.setattr(requests.Session, "send", mock_send)
    runner = SourceRunner(source, config={}, secrets={}, output=MemoryOutput())
    runner.run()
    assert "vendor_filter=enabled" in seen["url"] and "page_size=250" in seen["url"]


def test_cli_connector_lines_includes_restapi(capsys):
    assert run_cli(["connectors"]) == 0
    text = capsys.readouterr().out
    assert "restapi" in text


def test_a_duckdb_job_step_runs_a_rest_report_job_inline(api, tmp_path):
    """submit (POST), poll (GET until done), then results read with the connection's http defaults (records)."""
    statuses = [{"status": "RUNNING"}, {"status": "DONE"}]
    with api:
        api.routes[("POST", "/v1/reports")] = lambda request: (200, {"id": "r9"})
        api.routes[("GET", "/v1/reports/r9")] = lambda request: (200, statuses.pop(0))
        api.routes[("GET", "/v1/reports/r9/rows")] = lambda request: (200, {"data": [{"day": "2026-10-01", "n": 3}]})
        job = {
            "kind": "DuckDBJob", "name": "report", "spec": {"secrets": {"token": {"type": "string"}}},
            "connections": {"api": {"provider": "restapi", "type": "bearer", "token": "{{ secrets.token }}",
                                    "http": {"base_url": api.url, "records": {"path": "data"}}},
                            "wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")}},
            "steps": [{"name": "report", "connection": "api", "async_job": {
                "submit": {"operation": {"path": "/v1/reports", "method": "POST", "json": {"kind": "daily"}}},
                "poll": {"operation": {"path": "/v1/reports/{{ submit.id }}"}, "every": "1s", "timeout": "1m",
                         "done_when": {"path": "status", "equals": "DONE"}},
                "results": {"operation": {"path": "/v1/reports/{{ submit.id }}/rows"}}},
                "sql": "SELECT record->>'day' AS day, (record->>'n')::INT AS n FROM records"}],
            "outputs": [{"name": "out", "connection": "wh", "step": "report", "table": "report", "mode": "overwrite"}],
            "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}}}
        assert JOB_ENGINE.validate_job(job).valid
        assert run_job(job, secrets={"token": "t"}).exit_code == 0
        assert [json.loads(call["body"]) for call in api.calls("/v1/reports")] == [{"kind": "daily"}]
        assert len(api.calls("/v1/reports/r9")) == 2
    with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as con:
        assert con.execute("SELECT day, n FROM report").fetchall() == [("2026-10-01", 3)]


def test_an_inline_async_job_is_checked_like_a_sources():
    """A poll needs every, timeout and done_when (durations as `15s`, a condition with equals or in)."""
    def issues(poll, **job):
        manifest = {"kind": "DuckDBJob", "name": "report", "spec": {"secrets": {"token": {"type": "string"}}},
                    "connections": {"api": {"provider": "restapi", "type": "bearer", "token": "{{ secrets.token }}",
                                            "http": {"base_url": "https://api.example.com"}}},
                    "steps": [{"name": "report", "connection": "api", "async_job": dict({
                        "submit": {"operation": {"path": "/reports", "method": "POST"}},
                        "poll": dict({"operation": {"path": "/reports/{{ submit.id }}"}}, **poll),
                        "results": {"operation": {"path": "/reports/{{ submit.id }}/rows"}}}, **job)}],
                    "outputs": [{"name": "out", "connection": "api", "step": "report",
                                 "operation": {"path": "/sink", "method": "POST"}}]}
        return sorted((issue.code, issue.path, issue.message) for issue in JOB_ENGINE.validate_job(manifest).issues
                      if issue.path.startswith("steps"))
    missing = issues({})
    assert [(code, path) for code, path, _ in missing] == [
        ("missing-key", "steps[0].async_job.poll.done_when"), ("missing-key", "steps[0].async_job.poll.every"),
        ("missing-key", "steps[0].async_job.poll.timeout")]
    bad = issues({"every": "soon", "timeout": "30m", "done_when": {"path": "status"}})
    assert [message for _, _, message in bad] == [
        "async_job.poll.done_when needs exactly one of `equals` or `in`",
        "async_job.poll.every: invalid duration 'soon' (expected e.g. 15s, 30m, 1h, 7d)"]
