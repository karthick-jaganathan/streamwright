import json

import requests
import yaml

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
                        "http": {
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
                        "sdk": "restapi",
                        "method": "list",
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
    assert any("restapi is a REST API connector: use an http request" in str(e) for e in errors)


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
                        "http": {
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
            "requests": [{"name": "raw", "http": {"path": "/items",
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
