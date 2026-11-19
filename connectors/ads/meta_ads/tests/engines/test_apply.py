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
"""Apply outputs through meta_ads: creates, updates, deletes and Conversions API events, from a DuckDBJob, against the
real SDK with the Graph API faked (responses)."""

import json
import sqlite3
from types import MappingProxyType
from urllib.parse import parse_qsl

import pytest
import responses

from streamwright.core.jobs.duckdb.engine import run_job
from streamwright.core.jobs.duckdb.plugin import JOB_ENGINE

SECRETS = MappingProxyType({"access_token": "token-123"})
GRAPH = "https://graph.facebook.com/v26.0"


def _job(tmp_path, sql, service="AdAccount", method="create_campaign", arguments=None, **apply):
    operation = {"service": service, "method": method,
                 "arguments": {"id": "act_123"} if arguments is None else arguments}
    output = {"name": "push", "connection": "ads", "step": "rows", "operation": operation,
              "policy": {"retry": {"max_attempts": 1}}}
    if apply:
        output["apply"] = apply
    return {
        "kind": "DuckDBJob", "name": "ads_push",
        "spec": {"secrets": {"access_token": {"type": "string", "required": True}}},
        "connections": {"ads": {"provider": "meta_ads", "access_token": "{{ secrets.access_token }}",
                                "api_version": "v26.0"}},
        "steps": [{"name": "rows", "sql": sql}],
        "outputs": [output],
        "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}},
    }


def _outbox(tmp_path):
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        return [(row[0], json.loads(row[1]) if row[1] else None, row[2], row[3]) for row in con.execute(
            "SELECT status, result, error_code, error_message FROM sw_events ORDER BY sequence")]


def _form(call):
    """A POST's form parameters, JSON values decoded (the SDK JSON-encodes objects, lists and booleans)."""
    def decoded(value):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return {key: decoded(value) for key, value in parse_qsl(call.request.body)}


def _error(code, message, status=400, transient=False, user_msg=None):
    error = {"message": message, "type": "OAuthException", "code": code, "is_transient": transient,
             "fbtrace_id": "trace-1"}
    if user_msg:
        error["error_user_msg"] = user_msg
    return status, {}, json.dumps({"error": error})


def _answers(*replies):
    """A callback giving `replies` in turn: a dict is a 200 JSON body, a tuple a full (status, headers, body)."""
    queue = list(replies)

    def reply(request):
        item = queue.pop(0)
        return item if isinstance(item, tuple) else (200, {}, json.dumps(item))
    return reply


@pytest.fixture
def graph():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        yield mock


CAMPAIGNS = ("SELECT * FROM (VALUES ('Brand', 'OUTCOME_SALES'), ('Generic', 'BOGUS'), ('Search', 'OUTCOME_LEADS')) "
             "t(name, objective)")
STATIC = MappingProxyType({"params": {"status": "PAUSED", "special_ad_categories": []}})


def test_a_write_output_validates_against_the_installed_sdk(tmp_path):
    assert JOB_ENGINE.validate_job(_job(tmp_path, CAMPAIGNS)).valid
    issues = [str(issue) for issue in JOB_ENGINE.validate_job(_job(tmp_path, CAMPAIGNS,
                                                                   method="create_campaignz")).issues]
    assert any("create_campaign" in issue for issue in issues), issues          # "did you mean" from the SDK


def test_campaigns_are_created_and_a_rejected_row_fails_alone(tmp_path, graph):
    graph.add_callback("POST", GRAPH + "/act_123/campaigns", callback=_answers(
        {"id": "900"}, _error(100, "Invalid parameter", user_msg="Objective BOGUS is invalid."), {"id": "902"}))
    job = _job(tmp_path, CAMPAIGNS, arguments={"id": "act_123", **STATIC}, on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    first = _form(graph.calls[0])
    assert first["name"] == "Brand" and first["objective"] == "OUTCOME_SALES" and first["status"] == "PAUSED"
    assert first["special_ad_categories"] == [] and "op" not in first
    assert _outbox(tmp_path) == [
        ("applied", {"id": "900"}, None, None),
        ("failed", None, "100", "Invalid parameter: Objective BOGUS is invalid."),
        ("applied", {"id": "902"}, None, None)]


def test_a_transient_error_mid_chunk_keeps_the_earlier_rows_and_holds_the_rest(tmp_path, graph):
    graph.add_callback("POST", GRAPH + "/act_123/campaigns", callback=_answers(
        {"id": "900"}, _error(17, "User request limit reached")))
    result = run_job(_job(tmp_path, CAMPAIGNS, arguments={"id": "act_123", **STATIC}), secrets=dict(SECRETS))
    assert result.exit_code != 0
    assert len(graph.calls) == 2                                               # the third row was never sent
    assert [(status, row_result) for status, row_result, _, _ in _outbox(tmp_path)] == [
        ("applied", {"id": "900"}), ("in_flight", None), ("in_flight", None)]


def test_updates_need_an_id_and_deletes_need_allow_remove(tmp_path, graph):
    graph.add_callback("POST", GRAPH + "/601/", callback=_answers({"success": True}))
    sql = "SELECT * FROM (VALUES ('601', 'ACTIVE', 7500), (NULL, 'PAUSED', NULL)) t(id, status, daily_budget)"
    job = _job(tmp_path, sql, service="AdSet", method="api_update", arguments={}, on_row_error="continue",
               nulls="skip_field")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    assert _form(graph.calls[0]) == {"status": "ACTIVE", "daily_budget": 7500}
    assert [(status, row_result, code) for status, row_result, code, _ in _outbox(tmp_path)] == [
        ("applied", {"id": "601"}, None), ("failed", None, "INVALID_ROW")]

    graph.add_callback("DELETE", GRAPH + "/77/", callback=_answers({"success": True}))
    removes = "SELECT 'remove' AS op, '77' AS id"
    (tmp_path / "held").mkdir()
    held = _job(tmp_path / "held", removes, service="Ad", method="api_delete", arguments={}, on_row_error="continue")
    assert run_job(held, secrets=dict(SECRETS)).exit_code == 0
    assert [code for _, _, code, _ in _outbox(tmp_path / "held")] == ["REMOVE_NOT_ALLOWED"]
    (tmp_path / "allowed").mkdir()
    allowed = _job(tmp_path / "allowed", removes, service="Ad", method="api_delete", arguments={}, allow_remove=True)
    assert run_job(allowed, secrets=dict(SECRETS)).exit_code == 0
    assert graph.calls[-1].request.method == "DELETE"
    assert [status for status, _, _, _ in _outbox(tmp_path / "allowed")] == ["applied"]


EVENTS = ("SELECT 'Purchase' AS event_name, 1763337600 + k AS event_time, 'physical_store' AS action_source, "
          "'order-' || k AS event_id, '{\"em\": [\"' || sha256('a' || k || '@example.com') || '\"]}' AS user_data, "
          "{'currency': 'USD', 'value': (10.5 * (k + 1))::DOUBLE, 'order_id': NULL} AS custom_data FROM range(3) t(k)")


def test_events_are_sent_a_chunk_per_request(tmp_path, graph):
    graph.add_callback("POST", GRAPH + "/42/events", callback=_answers(
        {"events_received": 2, "messages": [], "fbtrace_id": "t-1"},
        {"events_received": 1, "messages": [], "fbtrace_id": "t-2"}))
    job = _job(tmp_path, EVENTS, service="AdsPixel", method="create_event",
               arguments={"id": "42", "params": {"test_event_code": "TEST1"}}, chunk_size=2, nulls="skip_field")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    first, second = (_form(call) for call in graph.calls)
    assert first["test_event_code"] == "TEST1" and len(first["data"]) == 2 and len(second["data"]) == 1
    event = first["data"][0]
    assert event["event_name"] == "Purchase" and event["event_time"] == 1763337600 and event["event_id"] == "order-0"
    assert isinstance(event["user_data"]["em"], list) and len(event["user_data"]["em"][0]) == 64  # JSON text parsed
    assert event["custom_data"] == {"currency": "USD", "value": 10.5}          # a struct, its NULL field dropped
    assert [row_result for _, row_result, _, _ in _outbox(tmp_path)] == [
        {"events_received": 2, "fbtrace_id": "t-1"}] * 2 + [{"events_received": 1, "fbtrace_id": "t-2"}]


def test_a_rejected_event_call_fails_its_rows(tmp_path, graph):
    graph.add_callback("POST", GRAPH + "/42/events", callback=_answers(_error(100, "Invalid parameter")))
    job = _job(tmp_path, EVENTS, service="AdsPixel", method="create_event", arguments={"id": "42"},
               on_row_error="continue", nulls="skip_field")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    assert [(status, code) for status, _, code, _ in _outbox(tmp_path)] == [("failed", "100")] * 3
