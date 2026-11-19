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

import pytest
from google.ads.googleads import oauth2
from google.ads.googleads.client import GoogleAdsClient
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
        self.reject = ()

    def mutate_campaigns(self, request):
        self.requests.append(request)
        response = _types("MutateCampaignsResponse")
        for index, _ in enumerate(request["operations"]):
            result = _types("MutateCampaignResult")
            if index not in self.reject and not request["validate_only"]:
                result.resource_name = "customers/%s/campaigns/%d" % (request["customer_id"], 900 + index)
            response.results.append(result)
        if self.reject:
            response.partial_failure_error = _partial_failure(self.reject)
        return response


class FakeUploadService:
    def __init__(self):
        self.requests = []

    def upload_click_conversions(self, request):
        self.requests.append(request)
        response = _types("UploadClickConversionsResponse")
        for conversion in request["conversions"]:
            result = _types("ClickConversionResult")
            result.gclid = conversion.gclid
            response.results.append(result)
        return response


@pytest.fixture
def services(monkeypatch):
    found = {"CampaignService": FakeCampaignService(), "ConversionUploadService": FakeUploadService()}
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
