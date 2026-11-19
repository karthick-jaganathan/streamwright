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
"""Apply outputs of REST API connectors: an output's request sends rows as JSON bodies (`http.send`)."""

import json
import sqlite3

import pytest
import responses

from streamwright.core.jobs.duckdb.engine import run_job
from streamwright.core.jobs.duckdb.plugin import JOB_ENGINE
from streamwright.core.runtime import components
from streamwright.core.sdk.contract import ConnectorDefinition
from streamwright.core.sdk.operations import http

BASE = "https://api.example.test/v1"

REST_API = ConnectorDefinition(
    name="rest_writer", title="REST writer", family="test", connection=http.auth_fields(("bearer",)),
    operations=(http.REQUEST, http.SEND), adapters={"source": "streamwright.core.sdk.engines.source:HttpSourceAdapter"})

ROWS = "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name) ORDER BY id"


@pytest.fixture
def rest_api():
    components.register(REST_API)
    yield
    components.unregister("rest_writer")


def _job(tmp_path, operation, *, apply=None, policy=None, sql=ROWS):
    return {
        "kind": "DuckDBJob", "name": "rest_push",
        "spec": {"secrets": {"token": {"type": "string", "required": True}}},
        "connections": {"api": {"provider": "rest_writer", "type": "bearer", "token": "{{ secrets.token }}",
                                "http": {"base_url": BASE, "headers": {"X-Client": "sw"}}}},
        "steps": [{"name": "items", "sql": sql}],
        "outputs": [{"name": "push", "connection": "api", "step": "items", "operation": operation,
                     **({"apply": apply} if apply else {}), **({"policy": policy} if policy else {})}],
        "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}},
    }


def _run(job):
    return run_job(job, secrets={"token": "tok-1"})


def _outbox(tmp_path):
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        return [(row[0], json.loads(row[1]) if row[1] else None, row[2]) for row in con.execute(
            "SELECT status, result, error_code FROM sw_events ORDER BY sequence")]


def _codes(job):
    return {(issue.code, issue.path) for issue in JOB_ENGINE.validate_job(job).issues}


def test_a_request_output_validates_its_verb_and_row_references(tmp_path, rest_api):
    assert JOB_ENGINE.validate_job(_job(tmp_path, {"path": "/items/{{ row.id }}", "method": "PUT"})).valid
    assert JOB_ENGINE.validate_job(_job(tmp_path, {"path": "/items", "json": {"data": "{{ rows }}"}})).valid
    assert ("bad-call", "outputs[0]") in _codes(_job(tmp_path, {"path": "/items", "method": "GET"}))
    assert ("bad-value", "outputs[0]") in _codes(_job(tmp_path, {"path": "/items/{{ row.id }}",
                                                                 "json": {"data": "{{ rows }}"}}))
    assert ("bad-value", "outputs[0]") in _codes(_job(tmp_path, {"path": "/{{ rows }}"}))
    paged = _job(tmp_path, {"path": "/items", "paginator": {"type": "none"}})
    assert ("unsupported-key", "outputs[0]") in _codes(paged)


@responses.activate
def test_each_row_is_a_request_and_a_rejected_row_fails_alone(tmp_path, rest_api):
    responses.post(BASE + "/items", json={"id": "x-1"})
    responses.post(BASE + "/items", status=400, json={"error": "name taken"})
    responses.post(BASE + "/items", json={"id": "x-3"})
    result = _run(_job(tmp_path, {"path": "/items"}, apply={"on_row_error": "continue"}))
    assert result.exit_code == 0, result.error
    sent = [json.loads(call.request.body) for call in responses.calls]
    assert sent == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]   # no `op` column
    assert responses.calls[0].request.headers["Authorization"] == "Bearer tok-1"
    assert responses.calls[0].request.headers["X-Client"] == "sw"
    assert _outbox(tmp_path) == [("applied", {"id": "x-1"}, None), ("failed", None, "400"),
                                 ("applied", {"id": "x-3"}, None)]


@responses.activate
def test_row_values_render_into_the_path_and_body_and_put_is_retried(tmp_path, rest_api):
    responses.put(BASE + "/items/1", status=503)
    responses.put(BASE + "/items/1", json={"ok": True})
    for key in (2, 3):
        responses.put(BASE + "/items/%d" % key, json={"ok": True})
    operation = {"path": "/items/{{ row.id }}", "method": "PUT", "json": {"item": {"label": "{{ row.name }}"}}}
    result = _run(_job(tmp_path, operation, policy={"retry": {"max_attempts": 2, "backoff": "constant"}}))
    assert result.exit_code == 0, result.error
    assert [call.request.url for call in responses.calls] == [BASE + "/items/1"] * 2 + [BASE + "/items/2",
                                                                                      BASE + "/items/3"]
    assert json.loads(responses.calls[-1].request.body) == {"item": {"label": "c"}}
    assert [status for status, _, _ in _outbox(tmp_path)] == ["applied"] * 3


@responses.activate
def test_a_chunk_is_one_request_with_rows_and_results_per_row(tmp_path, rest_api):
    responses.post(BASE + "/batch", json={"results": [{"id": "r1"}, {"id": "r2"}]})
    responses.post(BASE + "/batch", json={"results": [{"id": "r3"}]})
    operation = {"path": "/batch", "json": {"data": "{{ rows }}", "source": "sw"}, "records": {"path": "results"}}
    result = _run(_job(tmp_path, operation, apply={"chunk_size": 2}))
    assert result.exit_code == 0, result.error
    assert [json.loads(call.request.body) for call in responses.calls] == [
        {"data": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], "source": "sw"},
        {"data": [{"id": 3, "name": "c"}], "source": "sw"}]
    assert [result for _, result, _ in _outbox(tmp_path)] == [{"id": "r1"}, {"id": "r2"}, {"id": "r3"}]


@responses.activate
def test_a_post_cut_short_keeps_the_rows_sent_before_it_and_holds_the_rest(tmp_path, rest_api):
    responses.post(BASE + "/items", json={"id": "x-1"})
    responses.post(BASE + "/items", status=503)
    result = _run(_job(tmp_path, {"path": "/items"}))
    assert result.exit_code != 0 and "did not complete" in result.error
    assert len(responses.calls) == 2                                          # a POST is sent once: no retry
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        statuses = [row[0] for row in con.execute("SELECT status FROM sw_events ORDER BY sequence")]
    assert statuses == ["applied", "in_flight", "in_flight"]
