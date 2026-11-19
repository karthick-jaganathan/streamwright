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
"""Apply outputs through microsoft_ads: Add*, Delete* and conversion uploads, from a DuckDBJob, against fake SOAP."""

import io
import json
import os
import re
import sqlite3
from types import MappingProxyType

import pytest
import yaml
from bingads import service_client
from bingads.authorization import OAuthTokens, _UriOAuthService
from suds.transport import Reply, TransportError
from suds.transport.http import HttpTransport

from streamwright.connectors.microsoft_ads import writes
from streamwright.core.jobs.duckdb.engine import run_job
from streamwright.core.jobs.duckdb.plugin import JOB_ENGINE

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))
EXAMPLE = os.path.join(REPO_ROOT, "examples", "microsoft_ads")
SECRETS = MappingProxyType({"developer_token": "dev-token-1", "client_id": "client-1", "refresh_token": "refresh-1"})
CAMPAIGNS = "https://bingads.microsoft.com/CampaignManagement/v13"
XSI = 'xmlns:i="http://www.w3.org/2001/XMLSchema-instance"'
ARRAYS = 'xmlns:a="http://schemas.microsoft.com/2003/10/Serialization/Arrays"'


def unprefixed(xml):
    return re.sub(r'type="[\w-]+:', 'type="', re.sub(r"<(/?)[\w-]+:", r"<\1", xml))


def envelope(body):
    return ('<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Header/><s:Body>%s</s:Body>'
            '</s:Envelope>' % body)


def response(operation, body):
    return envelope('<%sResponse xmlns="%s" %s>%s</%sResponse>' % (operation, CAMPAIGNS, XSI, body, operation))


def batch_errors(*errors):
    """PartialErrors of (index, code, error code, message)."""
    return "<PartialErrors>%s</PartialErrors>" % "".join(
        '<BatchError><Code>%d</Code><Details i:nil="true"/><ErrorCode>%s</ErrorCode><FieldPath>%s</FieldPath>'
        '<ForwardCompatibilityMap i:nil="true"/><Index>%d</Index><Message>%s</Message><Type>BatchError</Type>'
        '</BatchError>' % (code, name, "MicrosoftClickId", index, message) for index, code, name, message in errors)


def fault(code, name, message):
    return envelope(
        '<s:Fault><faultcode>s:Server</faultcode><faultstring>Invalid client data.</faultstring><detail>'
        '<AdApiFaultDetail xmlns="https://adapi.microsoft.com" %s><TrackingId>tracking-9</TrackingId><Errors>'
        '<AdApiError><Code>%d</Code><Detail i:nil="true"/><ErrorCode>%s</ErrorCode><Message>%s</Message>'
        '</AdApiError></Errors></AdApiFaultDetail></detail></s:Fault>' % (XSI, code, name, message))


class FakeSoap(HttpTransport):
    """Answers operations with handlers[operation](xml) -> a response body (a `fault(...)` body is sent as HTTP 500)."""

    def __init__(self, handlers=None, sent=None):
        HttpTransport.__init__(self)
        self.handlers = {} if handlers is None else handlers
        self.sent = [] if sent is None else sent

    def copy(self):
        return FakeSoap(self.handlers, self.sent)

    def send(self, request):
        xml = unprefixed(request.message.decode("utf-8"))
        operation = re.search(r"<(\w+)Request>", xml).group(1)
        self.sent.append((operation, xml))
        body = self.handlers[operation](xml)
        if "<s:Fault>" in body:
            raise TransportError("Internal Server Error", 500, io.BytesIO(body.encode("utf-8")))
        return Reply(200, {"Content-Type": "text/xml; charset=utf-8"}, body.encode("utf-8"))


@pytest.fixture
def soap(monkeypatch):
    fake = FakeSoap()
    original = service_client.Client
    monkeypatch.setattr(service_client, "Client",
                        lambda url, **options: original(url, transport=fake.copy(), **options))
    monkeypatch.setattr(_UriOAuthService, "get_access_token", staticmethod(lambda **request: OAuthTokens(
        access_token="access-1", access_token_expires_in_seconds=3600, refresh_token="refresh-2")))
    return fake


def _job(tmp_path, sql, call=None, policy=None, **apply):
    call = dict({"service": "CampaignManagementService", "method": "AddCampaigns",
                 "arguments": {"request": {"AccountId": 123456}}}, **(call or {}))
    output = {"name": "push", "connection": "ads", "step": "rows", "operation": call}
    if apply:
        output["apply"] = apply
    if policy:
        output["policy"] = policy
    return {
        "kind": "DuckDBJob", "name": "ads_push",
        "spec": {"secrets": {name: {"type": "string", "required": True} for name in SECRETS}},
        "connections": {"ads": {"provider": "microsoft_ads", "customer_id": "555", "account_id": "777",
                                **{name: "{{ secrets.%s }}" % name for name in SECRETS}}},
        "steps": [{"name": "rows", "sql": sql}],
        "outputs": [output],
        "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}},
    }


def _outbox(tmp_path):
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        return [(row[0], json.loads(row[1]) if row[1] else None, row[2], row[3]) for row in con.execute(
            "SELECT status, result, error_code, error_message FROM sw_events ORDER BY job_id, sequence")]


NEW_CAMPAIGNS = ("SELECT * FROM (VALUES ('Brand', 'Paused', 25.0, ['All']), ('Generic', 'Paused', 10.0, ['All']), "
                 "('Bad', 'Paused', 5.0, ['All'])) t(Name, Status, DailyBudget, Languages)")
CONVERSIONS = ("SELECT 'click-' || k AS MicrosoftClickId, 'Offline purchase' AS ConversionName, "
               "'2026-11-15T14:30:00Z' AS ConversionTime, 35.5 AS ConversionValue, "
               "'USD' AS ConversionCurrencyCode FROM range(3) t(k)")


def test_apply_outputs_validate_against_the_spec(tmp_path):
    assert JOB_ENGINE.validate_job(_job(tmp_path, NEW_CAMPAIGNS)).valid
    upload = _job(tmp_path, CONVERSIONS, call={"method": "ApplyOfflineConversions", "arguments": {}})
    assert JOB_ENGINE.validate_job(upload).valid
    issues = [str(issue) for issue in JOB_ENGINE.validate_job(_job(tmp_path, NEW_CAMPAIGNS, call={
        "method": "PauseCampaigns"})).issues]
    assert any("has no apply operation" in issue for issue in issues), issues


def test_created_rows_get_their_ids_and_a_rejected_row_fails_alone(tmp_path, soap):
    soap.handlers["AddCampaigns"] = lambda xml: response("AddCampaigns", (
        '<CampaignIds %s><a:long>901</a:long><a:long>902</a:long><a:long i:nil="true"/></CampaignIds>%s' % (
            ARRAYS, batch_errors((2, 1102, "CampaignServiceInvalidCampaignName", "The name is not valid.")))))
    job = _job(tmp_path, "SELECT *, 'ManualCpcBiddingScheme' AS \"BiddingScheme.@type\" FROM (%s)" % NEW_CAMPAIGNS,
               on_row_error="continue")
    result = run_job(job, secrets=dict(SECRETS))
    assert result.exit_code == 0, result.error
    ((operation, xml),) = soap.sent
    assert operation == "AddCampaigns" and "<AccountId>123456</AccountId>" in xml
    assert "<CustomerAccountId>123456</CustomerAccountId>" in xml and "<CustomerId>555</CustomerId>" in xml
    assert xml.count("<Campaign>") == 3 and "<Name>Brand</Name>" in xml and "<string>All</string>" in xml
    assert 'type="ManualCpcBiddingScheme"' in xml
    assert _outbox(tmp_path) == [
        ("applied", {"Id": 901}, None, None), ("applied", {"Id": 902}, None, None),
        ("failed", None, "CampaignServiceInvalidCampaignName", "The name is not valid. (field: MicrosoftClickId)")]


def test_a_row_that_does_not_fit_the_item_fails_without_a_call(tmp_path, soap):
    job = _job(tmp_path, "SELECT 'Brand' AS Name, 'x' AS Bogus", on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    ((status, _, code, message),) = _outbox(tmp_path)
    assert (status, code) == ("failed", "INVALID_ROW") and "has no field 'Bogus'" in message
    assert soap.sent == []


def test_a_remove_needs_allow_remove_and_sends_a_list_of_ids(tmp_path, soap):
    soap.handlers["DeleteCampaigns"] = lambda xml: response("DeleteCampaigns", '<PartialErrors i:nil="true"/>')
    sql = "SELECT * FROM (VALUES (11), (12)) t(CampaignId)"
    call = {"method": "DeleteCampaigns"}
    assert run_job(_job(tmp_path, sql, call=call, on_row_error="continue"), secrets=dict(SECRETS)).exit_code == 0
    assert [code for _, _, code, _ in _outbox(tmp_path)] == ["REMOVE_NOT_ALLOWED"] * 2 and soap.sent == []

    (tmp_path / "allowed").mkdir()
    job = _job(tmp_path / "allowed", sql, call=call, allow_remove=True)
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    ((_, xml),) = soap.sent
    assert re.search(r"<CampaignIds><long>11</long><long>12</long></CampaignIds>", xml)
    assert _outbox(tmp_path / "allowed") == [("applied", None, None, None)] * 2


def test_conversions_upload_and_a_retryable_fault_leaves_the_chunk_in_flight_for_the_next_run(tmp_path, soap):
    calls = []

    def upload(xml):
        calls.append(xml)
        if len(calls) == 2:
            return fault(0, "InternalError", "An internal error has occurred.")
        return response("ApplyOfflineConversions", batch_errors((1, 3801, "OfflineConversionMicrosoftClickIdInvalid",
                                                                 "The click id is not valid.")) if
                        "click-1" in xml else '<PartialErrors i:nil="true"/>')
    soap.handlers["ApplyOfflineConversions"] = upload
    job = _job(tmp_path, CONVERSIONS, call={"method": "ApplyOfflineConversions", "arguments": {}},
               policy={"retry": {"max_attempts": 1}}, chunk_size=2, on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code != 0
    assert "<CustomerAccountId>777</CustomerAccountId>" in calls[0]                   # the connection's account
    assert "<ConversionTime>2026-11-15T14:30:00Z</ConversionTime>" in calls[0]
    assert [status for status, *_ in _outbox(tmp_path)] == ["applied", "failed", "in_flight"]

    # the next run sends the row in flight again first (an idempotent upload), then its own batch
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    assert "click-2" in calls[2] and "click-0" not in calls[2]
    assert [status for status, *_ in _outbox(tmp_path)][:3] == ["applied", "failed", "applied"]
    assert _outbox(tmp_path)[1][2:] == ("OfflineConversionMicrosoftClickIdInvalid",
                                        "The click id is not valid. (field: MicrosoftClickId)")


def test_a_fault_for_the_whole_call_fails_its_rows(tmp_path, soap):
    soap.handlers["ApplyOfflineConversions"] = lambda xml: fault(1100, "InvalidAccountId", "The account is not valid.")
    job = _job(tmp_path, CONVERSIONS, call={"method": "ApplyOfflineConversions", "arguments": {}},
               on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    assert {(status, code) for status, _, code, _ in _outbox(tmp_path)} == {("failed", "1100")}
    assert "The account is not valid." in _outbox(tmp_path)[0][3]


def test_nested_partial_errors_and_identities_map_to_their_rows():
    response = {"AdExtensionIdentities": {"AdExtensionIdentity": [{"Id": 7, "Version": 1}, None]},
                "NestedPartialErrors": {"BatchErrorCollection": [
                    {"Index": 1, "Code": 1, "ErrorCode": None, "Message": None, "FieldPath": None,
                     "BatchErrors": {"BatchError": [{"Index": 0, "Message": "The URL is not valid."}]}}]}}
    assert writes._failures(response) == {1: ("1", "The URL is not valid.")}
    assert writes._ids(response) == [{"Id": 7, "Version": 1}, None]


def test_the_upload_example_logs_the_rejected_conversions(tmp_path, soap, caplog):
    with open(os.path.join(EXAMPLE, "jobs", "conversions", "upload.yaml"), encoding="utf-8") as handle:
        job = yaml.safe_load(handle)
    job["runtime"]["state"]["sqlite"] = str(tmp_path / "state.db")
    with open(os.path.join(EXAMPLE, "payloads", "conversions", "clicks.json"), encoding="utf-8") as handle:
        rows = json.load(handle)
    goals = "".join('<ConversionGoal i:type="OfflineConversionGoal"><Id>%d</Id><Name>%s</Name><Status>Active</Status>'
                    '</ConversionGoal>' % (number, name)
                    for number, name in enumerate(("Offline purchase", "Offline qualified lead"), start=1))
    soap.handlers["GetConversionGoalsByIds"] = lambda xml: response(
        "GetConversionGoalsByIds", "<ConversionGoals>%s</ConversionGoals>" % goals)
    soap.handlers["ApplyOfflineConversions"] = lambda xml: response("ApplyOfflineConversions", batch_errors(
        (0, 3801, "OfflineConversionMicrosoftClickIdInvalid", "The click id is not valid.")))
    result = run_job(job, config={"customer_id": "555", "account_id": "123456"},
                     secrets=dict(SECRETS, client_secret="secret-1"), input_rows=rows, apply_state_migrations=True)
    assert result.exit_code == 0, result.error
    # the goals first; the conversion naming an unknown goal is skipped; then 5 rows in calls of 2
    assert [operation for operation, _ in soap.sent] == ["GetConversionGoalsByIds"] + ["ApplyOfflineConversions"] * 3
    assert any("no offline goal named No such goal" in record.getMessage() for record in caplog.records)
    soap.sent.pop(0)
    assert "<ConversionValue>" not in soap.sent[0][1].split("</OfflineConversion>")[0]       # nulls: skip_field
    rejected = [record.getMessage() for record in caplog.records if "rejected" in record.getMessage()]
    assert len(rejected) == 3 and "click EXAMPLE_MSCLKID_0001 (Offline qualified lead) rejected: " \
        "OfflineConversionMicrosoftClickIdInvalid: The click id is not valid." in rejected[0]
