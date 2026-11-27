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
"""Apply outputs through google_ads: mutations and conversion uploads, from a DuckDBJob, against fake services."""

import json
import sqlite3
from types import MappingProxyType

import grpc
import pytest
from google.ads.googleads import oauth2
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.auth.credentials import AnonymousCredentials
from google.rpc import status_pb2

from streamwright.core.jobs.duckdb.engine import run_job
from streamwright.core.jobs.duckdb.plugin import JOB_ENGINE

SECRETS = MappingProxyType({"developer_token": "dev-token-1", "client_id": "client-1", "client_secret": "secret-1",
                            "refresh_token": "refresh-1"})


def _types(name):
    return GoogleAdsClient(AnonymousCredentials(), developer_token="x", use_proto_plus=True).get_type(name)


def _partial_failure(indexes):
    """A google.rpc.Status whose GoogleAdsFailure rejects the operations at `indexes`."""
    failure = _types("GoogleAdsFailure")
    for index in indexes:
        error = _types("GoogleAdsError")
        error.message = "Duplicate name %d." % index
        error.error_code.campaign_error = type(error.error_code.campaign_error).DUPLICATE_CAMPAIGN_NAME
        element = type(_types("ErrorLocation")).FieldPathElement
        error.location.field_path_elements.extend([element(field_name="operations", index=index),
                                                   element(field_name="create"), element(field_name="name")])
        failure.errors.append(error)
    status = status_pb2.Status(code=3, message="partial failure")
    status.details.add().value = type(failure).serialize(failure)
    return status


class FakeCampaignService:
    def __init__(self):
        self.requests = []
        self.reject = ()            # the operations Google rejects (located errors, empty results)
        self.status = None          # else this partial_failure_error, with the operations in `empty` left empty
        self.empty = ()

    def mutate_campaigns(self, request):
        self.requests.append(request)
        response = _types("MutateCampaignsResponse")
        for index, _ in enumerate(request["operations"]):
            result = _types("MutateCampaignResult")
            if index not in (*self.reject, *self.empty) and not request["validate_only"]:
                result.resource_name = "customers/%s/campaigns/%d" % (request["customer_id"], 900 + index)
            response.results.append(result)
        if self.reject or self.status is not None:
            response.partial_failure_error = self.status or _partial_failure(self.reject)
        return response


def _upload_failure(errors, *, details=True):
    """
    The partial_failure_error of an upload as Google sends it: a Status packing one GoogleAdsFailure (and a RequestInfo
    beside it), each error (index or None, code) located at `conversions[index].gclid`.
    """
    failure = _types("GoogleAdsFailure")
    for index, code in errors:
        error = _types("GoogleAdsError")
        error.message = "%s at %s." % (code, index)
        enum = type(error.error_code.conversion_upload_error)
        error.error_code.conversion_upload_error = getattr(enum, code)
        if index is not None:
            element = type(_types("ErrorLocation")).FieldPathElement
            error.location.field_path_elements.extend([element(field_name="conversions", index=index),
                                                       element(field_name="gclid")])
        failure.errors.append(error)
    status = status_pb2.Status(code=3, message="Conversions failed.")
    if details:
        status.details.add(type_url="type.googleapis.com/google.ads.googleads.v26.errors.GoogleAdsFailure",
                           value=type(failure).serialize(failure))
        status.details.add(type_url="type.googleapis.com/google.rpc.RequestInfo", value=b"\n\x03abc")
    return status


class FakeUploadService:
    def __init__(self):
        self.requests = []
        self.failed = ()            # the indexes Google fails (their results empty), with `failure` as the status
        self.failure = None

    def upload_click_conversions(self, request):
        self.requests.append(request)
        response = _types("UploadClickConversionsResponse")
        for index, conversion in enumerate(request["conversions"]):
            result = _types("ClickConversionResult")
            if index not in self.failed:
                result.gclid = conversion.gclid
            response.results.append(result)
        if self.failure is not None:
            response.partial_failure_error = self.failure
        return response


class _Rpc(grpc.RpcError):
    """The gRPC error of a call Google rejected."""

    def code(self):
        return grpc.StatusCode.INVALID_ARGUMENT


class FakeConversionGoalService:
    """mutate_campaign_conversion_goals: update-only operations, no partial failure (all rows or none)."""

    def __init__(self):
        self.requests = []
        self.reject = ()            # the operations whose errors make Google reject the whole call

    def mutate_campaign_conversion_goals(self, request):
        self.requests.append(request)
        if self.reject:
            failure = type(_types("GoogleAdsFailure")).deserialize(_partial_failure(self.reject).details[0].value)
            raise GoogleAdsException(_Rpc(), None, failure, "req-7")
        response = _types("MutateCampaignConversionGoalsResponse")
        for operation in request["operations"]:
            result = _types("MutateCampaignConversionGoalResult")
            result.resource_name = operation.update.resource_name
            response.results.append(result)
        return response


@pytest.fixture
def services(monkeypatch):
    found = {"CampaignService": FakeCampaignService(), "ConversionUploadService": FakeUploadService(),
             "CampaignConversionGoalService": FakeConversionGoalService()}
    monkeypatch.setattr(oauth2, "get_credentials", lambda config: AnonymousCredentials())
    monkeypatch.setattr(GoogleAdsClient, "get_service", lambda client, name, **options: found[name])
    return found


def _job(tmp_path, sql, call=None, **apply):
    call = dict({"service": "CampaignService", "method": "mutate_campaigns",
                 "arguments": {"customer_id": "123-456-7890"}}, **(call or {}))
    output = {"name": "push", "connection": "ads", "step": "rows", "operation": call}
    if apply:
        output["apply"] = apply
    return {
        "kind": "DuckDBJob", "name": "ads_push",
        "spec": {"secrets": {name: {"type": "string", "required": True} for name in SECRETS}},
        "connections": {"ads": {"provider": "google_ads", **{name: "{{ secrets.%s }}" % name for name in SECRETS}}},
        "steps": [{"name": "rows", "sql": sql}],
        "outputs": [output],
        "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}},
    }


def _outbox(tmp_path):
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        return [(row[0], json.loads(row[1]) if row[1] else None, row[2]) for row in con.execute(
            "SELECT status, result, error_code FROM sw_events ORDER BY sequence")]


def _errors(tmp_path):
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        return [row[0] for row in con.execute("SELECT error_message FROM sw_events ORDER BY sequence")]


CAMPAIGNS = ("SELECT * FROM (VALUES ('create', NULL, 'Brand', 'PAUSED', true), "
             "('update', 'customers/1234567890/campaigns/7', 'Generic', NULL, false)) "
             "t(op, \"campaign.resource_name\", \"campaign.name\", \"campaign.status\", "
             "\"campaign.network_settings.target_google_search\")")


def test_a_mutate_output_validates_against_the_installed_library(tmp_path):
    assert JOB_ENGINE.validate_job(_job(tmp_path, CAMPAIGNS)).valid
    bad = _job(tmp_path, CAMPAIGNS, call={"method": "mutate_campaignz"})
    issues = [str(issue) for issue in JOB_ENGINE.validate_job(bad).issues]
    assert any("mutate_campaigns" in issue for issue in issues), issues          # "did you mean" from the SDK


def test_campaigns_are_created_and_updated_with_masks_from_the_row_columns(tmp_path, services):
    result = run_job(_job(tmp_path, CAMPAIGNS, nulls="clear"), secrets=dict(SECRETS))
    assert result.exit_code == 0, result.error
    request = services["CampaignService"].requests[0]
    create, update = request["operations"]
    assert request["customer_id"] == "1234567890" and request["partial_failure"] is True
    assert create.create.name == "Brand" and create.create.network_settings.target_google_search is True
    assert update.update.resource_name == "customers/1234567890/campaigns/7"
    assert sorted(update.update_mask.paths) == ["name", "network_settings.target_google_search", "status"]
    assert [row[1]["resource_name"] for row in _outbox(tmp_path)] == [
        "customers/1234567890/campaigns/900", "customers/1234567890/campaigns/901"]


def test_partial_failures_are_per_row_and_validate_only_changes_nothing(tmp_path, services):
    services["CampaignService"].reject = (1,)
    job = _job(tmp_path, CAMPAIGNS, nulls="clear", on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    assert [(status, code) for status, _, code in _outbox(tmp_path)] == [
        ("applied", None), ("failed", "DUPLICATE_CAMPAIGN_NAME")]
    assert _errors(tmp_path)[1] == "Duplicate name 1. (field: create.name)"      # the field Google names

    (tmp_path / "dry").mkdir()
    services["CampaignService"].reject = ()
    dry = _job(tmp_path / "dry", CAMPAIGNS, call={"arguments": {"customer_id": "123-456-7890", "validate_only": True}},
               nulls="clear")
    assert run_job(dry, secrets=dict(SECRETS)).exit_code == 0
    assert services["CampaignService"].requests[-1]["validate_only"] is True
    assert [result for _, result, _ in _outbox(tmp_path / "dry")] == [{"validate_only": True}] * 2


def test_a_remove_needs_allow_remove_and_a_bad_row_fails_alone(tmp_path, services):
    sql = ("SELECT * FROM (VALUES ('remove', 'customers/1234567890/campaigns/7', NULL), "
           "('create', NULL, 'Brand')) t(op, resource_name, \"campaign.bogus\")")
    job = _job(tmp_path, sql, nulls="skip_field", on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    assert [code for _, _, code in _outbox(tmp_path)] == ["REMOVE_NOT_ALLOWED", "INVALID_ROW"]
    assert services["CampaignService"].requests == []                          # nothing valid was left to send


def test_click_conversions_are_uploaded(tmp_path, services):
    sql = ("SELECT 'g-' || k AS gclid, 'customers/1234567890/conversionActions/5' AS conversion_action, "
           "'2026-11-15 14:30:00+00:00' AS conversion_date_time, 350.0 AS conversion_value, 'USD' AS currency_code "
           "FROM range(3) t(k)")
    job = _job(tmp_path, sql, call={"service": "ConversionUploadService", "method": "upload_click_conversions"},
               chunk_size=2)
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    assert [len(request["conversions"]) for request in services["ConversionUploadService"].requests] == [2, 1]
    assert [result["gclid"] for _, result, _ in _outbox(tmp_path)] == ["g-0", "g-1", "g-2"]


UPLOADS = ("SELECT 'g-' || conversion_number AS gclid, "
           "'customers/1234567890/conversionActions/5' AS conversion_action, "
           "'2026-11-15 14:30:00+00:00' AS conversion_date_time FROM range(4) AS conversions(conversion_number)")
UPLOAD_CALL = MappingProxyType({"service": "ConversionUploadService", "method": "upload_click_conversions"})


@pytest.mark.parametrize(("failure", "failed", "expected"), [
    # every error Google located on a conversion, the first's code; another detail type (RequestInfo) skipped
    (_upload_failure([(1, "UNPARSEABLE_GCLID"), (1, "EXPIRED_EVENT"), (3, "CLICK_NOT_FOUND")]), (1, 3),
     [None, ("UNPARSEABLE_GCLID", "UNPARSEABLE_GCLID at 1. (field: gclid); EXPIRED_EVENT at 1. (field: gclid)"),
      None, ("CLICK_NOT_FOUND", "CLICK_NOT_FOUND at 3. (field: gclid)")]),
    # an error not located on an operation: the conversion with an empty result fails with it
    (_upload_failure([(None, "UNPARSEABLE_GCLID")]), (2,),
     [None, None, ("UNPARSEABLE_GCLID", "Google rejected it without locating the operation: UNPARSEABLE_GCLID at "
                                        "None."), None]),
    # a status without a GoogleAdsFailure: the empty results fail with the status
    (_upload_failure([], details=False), (0,),
     [("INVALID_ARGUMENT", "Google rejected it without locating the operation: Conversions failed."), None, None,
      None]),
])
def test_upload_partial_failures_never_apply_a_failed_conversion(tmp_path, services, failure, failed, expected):
    upload = services["ConversionUploadService"]
    upload.failure, upload.failed = failure, failed
    assert run_job(_job(tmp_path, UPLOADS, call=dict(UPLOAD_CALL), on_row_error="continue"),
                   secrets=dict(SECRETS)).exit_code == 0
    assert upload.requests[0]["partial_failure"] is True
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        found = [(code, message) if status == "failed" else None for status, code, message in con.execute(
            "SELECT status, error_code, error_message FROM sw_events ORDER BY sequence")]
    assert found == expected


def test_update_only_operations_without_partial_failure_are_sent_all_or_nothing(tmp_path, services):
    sql = ("SELECT 'update' AS op, 'customers/1234567890/campaignConversionGoals/7~PURCHASE~WEBSITE' AS "
           "\"campaign_conversion_goal.resource_name\", true AS \"campaign_conversion_goal.biddable\" "
           "UNION ALL SELECT 'create', NULL, false")
    call = {"service": "CampaignConversionGoalService", "method": "mutate_campaign_conversion_goals"}
    job = _job(tmp_path, sql, call=call, nulls="skip_field", on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    (request,) = services["CampaignConversionGoalService"].requests
    assert "partial_failure" not in request                                       # the request has no such field
    (operation,) = request["operations"]
    assert operation.update.biddable is True and list(operation.update_mask.paths) == ["biddable"]
    outcome = _outbox(tmp_path)
    assert outcome[0] == ("applied", {"resource_name": "customers/1234567890/campaignConversionGoals/7~PURCHASE~"
                                                       "WEBSITE"}, None)
    assert outcome[1][0] == "failed" and _errors(tmp_path)[1] == (
        "CampaignConversionGoalOperation has no create: its actions are update")


GOALS = ("SELECT * FROM (VALUES ('create', 'customers/1234567890/campaignConversionGoals/6~LEAD~WEBSITE'), "
         "('update', 'customers/1234567890/campaignConversionGoals/7~PURCHASE~WEBSITE'), "
         "('update', 'customers/1234567890/campaignConversionGoals/8~PURCHASE~WEBSITE')) "
         "t(op, \"campaign_conversion_goal.resource_name\")")


def test_a_rejected_all_or_nothing_call_fails_each_row_with_its_own_reason(tmp_path, services):
    services["CampaignConversionGoalService"].reject = (1,)          # the second operation: the third row
    call = {"service": "CampaignConversionGoalService", "method": "mutate_campaign_conversion_goals"}
    assert run_job(_job(tmp_path, GOALS, call=call, on_row_error="continue"), secrets=dict(SECRETS)).exit_code == 0
    assert [code for _, _, code in _outbox(tmp_path)] == ["INVALID_ROW", "DUPLICATE_CAMPAIGN_NAME",
                                                          "DUPLICATE_CAMPAIGN_NAME"]
    invalid, bystander, rejected = _errors(tmp_path)
    assert invalid == "CampaignConversionGoalOperation has no create: its actions are update"      # kept
    assert bystander.startswith("not applied: Google rejects the whole call for the errors of operations[1]: "
                                "google_ads: DUPLICATE_CAMPAIGN_NAME: Duplicate name 1. (operations[1].create.name)")
    assert "(request id req-7)" in bystander
    assert rejected == "Duplicate name 1. (field: create.name)"


def test_a_partial_failure_finds_its_row_past_the_rows_never_sent(tmp_path, services):
    services["CampaignService"].reject = (1,)                       # the second operation sent: the third row
    sql = ("SELECT * FROM (VALUES ('create', 'Bad', 'x'), ('create', 'Brand', NULL), ('create', 'Generic', NULL)) "
           "t(op, \"campaign.name\", \"campaign.bogus\")")
    assert run_job(_job(tmp_path, sql, nulls="skip_field", on_row_error="continue"),
                   secrets=dict(SECRETS)).exit_code == 0
    assert [(status, code) for status, _, code in _outbox(tmp_path)] == [
        ("failed", "INVALID_ROW"), ("applied", None), ("failed", "DUPLICATE_CAMPAIGN_NAME")]


def _dry(tmp_path):
    return _job(tmp_path, CAMPAIGNS, call={"arguments": {"customer_id": "123-456-7890", "validate_only": True}},
                nulls="clear", on_row_error="continue")


def test_validate_only_fails_the_rows_google_finds_fault_with(tmp_path, services):
    services["CampaignService"].reject = (1,)
    assert run_job(_dry(tmp_path), secrets=dict(SECRETS)).exit_code == 0
    assert [(status, result, code) for status, result, code in _outbox(tmp_path)] == [
        ("applied", {"validate_only": True}, None), ("failed", None, "DUPLICATE_CAMPAIGN_NAME")]


def test_validate_only_with_unlocated_errors_fails_every_row(tmp_path, services):
    services["CampaignService"].status = _upload_failure([(None, "UNPARSEABLE_GCLID")])
    assert run_job(_dry(tmp_path), secrets=dict(SECRETS)).exit_code == 0
    assert [(status, code) for status, _, code in _outbox(tmp_path)] == [("failed", "UNPARSEABLE_GCLID")] * 2
    assert _errors(tmp_path)[0] == "Google rejected it without locating the operation: UNPARSEABLE_GCLID at None."


def test_an_empty_mutate_result_with_an_unlocated_failure_is_not_applied(tmp_path, services):
    campaigns = services["CampaignService"]
    campaigns.status, campaigns.empty = _upload_failure([(None, "UNPARSEABLE_GCLID")]), (1,)
    assert run_job(_job(tmp_path, CAMPAIGNS, nulls="clear", on_row_error="continue"),
                   secrets=dict(SECRETS)).exit_code == 0
    assert [(status, code) for status, _, code in _outbox(tmp_path)] == [
        ("applied", None), ("failed", "UNPARSEABLE_GCLID")]


def test_every_failure_detail_is_read(tmp_path, services):
    upload = services["ConversionUploadService"]
    first, second = _upload_failure([(0, "UNPARSEABLE_GCLID")]), _upload_failure([(2, "CLICK_NOT_FOUND")])
    first.details.append(second.details[0])                         # two GoogleAdsFailure details
    upload.failure, upload.failed = first, (0, 2)
    assert run_job(_job(tmp_path, UPLOADS, call=dict(UPLOAD_CALL), on_row_error="continue"),
                   secrets=dict(SECRETS)).exit_code == 0
    assert [code for _, _, code in _outbox(tmp_path)] == ["UNPARSEABLE_GCLID", None, "CLICK_NOT_FOUND", None]
