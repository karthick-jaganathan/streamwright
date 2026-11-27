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
"""The examples/google_data_manager jobs, run end to end against fake service clients."""

import hashlib
import logging
import os
from types import MappingProxyType

import duckdb
import pytest
import yaml
from google.ads import datamanager_v1 as dm
from google.ads.googleads import oauth2
from google.ads.googleads.client import GoogleAdsClient
from google.auth.credentials import AnonymousCredentials

from streamwright.core.config.inputs import InputError
from streamwright.core.jobs.duckdb.engine import run_job

SECRETS = MappingProxyType({"client_id": "client-1", "client_secret": "secret-1", "refresh_token": "refresh-1"})

EXAMPLES = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "examples",
                                        "google_data_manager"))
# the consent is required config (the examples assume none): DENIED for personalization shows it is the one sent
ACCOUNTS = MappingProxyType({"operating_account": "1234567890", "login_account": "1112223333",
                             "ad_user_data_consent": "CONSENT_GRANTED", "ad_personalization_consent": "CONSENT_DENIED"})


def _example(tmp_path, name):
    """An example job, its state and warehouse in tmp_path."""
    with open(os.path.join(EXAMPLES, "jobs", name), encoding="utf-8") as handle:
        job = yaml.safe_load(handle)
    if "runtime" in job:
        job["runtime"]["state"].update(sqlite=str(tmp_path / "state.db"), apply_migrations=True)
    if "warehouse" in job["connections"]:
        job["connections"]["warehouse"]["path"] = str(tmp_path / "dw.duckdb")
    return job


def _sha256(text):
    return hashlib.sha256(text.encode()).hexdigest()


def test_the_conversions_example_hashes_identifiers_as_google_asks(tmp_path, services, caplog):
    caplog.set_level(logging.INFO)
    config = dict(ACCOUNTS, conversion_action_id="555", validate_only=True)
    result = run_job(_example(tmp_path, "conversions/ingest.yaml"), config=config, secrets=dict(SECRETS),
                     input_source=os.path.join(EXAMPLES, "payloads", "conversions", "offline.json"))
    assert result.exit_code == 0, result.error
    (request,) = services["IngestionService"].requests
    (destination,) = request.destinations
    assert (destination.operating_account.account_id, destination.login_account.account_id,
            destination.product_destination_id) == ("1234567890", "1112223333", "555")
    assert request.validate_only is True and request.encoding == dm.Encoding.HEX
    first, second, third = sorted(request.events, key=lambda event: event.transaction_id)
    email, phone = first.user_data.user_identifiers
    assert email.email_address == _sha256("johnsmith@gmail.com")       # trimmed, lowercase, Gmail dots and +suffix
    assert phone.phone_number == _sha256("+18005550199")
    assert first.ad_identifiers.gclid == "EXAMPLE_GCLID_0001" and first.conversion_value == 30.03
    assert first.event_timestamp.isoformat() == "2026-01-31T14:07:00+00:00"
    assert first.event_source == dm.EventSource.WEB
    assert not second.user_data.user_identifiers and second.currency == "USD"
    # the conversion's own consent; the others take the request's
    assert request.consent.ad_user_data == dm.ConsentStatus.CONSENT_GRANTED
    assert request.consent.ad_personalization == dm.ConsentStatus.CONSENT_DENIED
    assert second.consent.ad_personalization == dm.ConsentStatus.CONSENT_DENIED
    assert not first.consent.ad_personalization and not third.consent.ad_user_data
    assert not third.ad_identifiers.gclid and third.user_data.user_identifiers[0].email_address == _sha256(
        "lead@example.com")
    assert "output ingest: request req-1: applied: 3 event(s)" in caplog.text


def test_the_audiences_example_sends_a_person_s_identifiers_together(tmp_path, services):
    result = run_job(_example(tmp_path, "audiences/ingest.yaml"), config=dict(ACCOUNTS, user_list_id="777"),
                     secrets=dict(SECRETS),
                     input_source=os.path.join(EXAMPLES, "payloads", "audiences", "members.json"))
    assert result.exit_code == 0, result.error
    (request,) = services["IngestionService"].requests
    assert request.consent.ad_personalization == dm.ConsentStatus.CONSENT_DENIED
    assert request.terms_of_service.customer_match_terms_of_service_status == dm.TermsOfServiceStatus.ACCEPTED
    identifiers = sorted([[item.email_address or item.phone_number for item in member.user_data.user_identifiers]
                          for member in request.audience_members], key=len)
    assert sorted(identifiers) == sorted([[_sha256("johnsmith@gmail.com"), _sha256("+18005550199")],
                                          [_sha256("lead@example.com")], [_sha256("+442079460958")]])


def test_the_ingest_examples_need_the_consent_collected(tmp_path, services):
    config = {key: value for key, value in ACCOUNTS.items() if key != "ad_personalization_consent"}
    with pytest.raises(InputError, match="missing required config 'ad_personalization_consent'"):
        run_job(_example(tmp_path, "conversions/ingest.yaml"), config=dict(config, conversion_action_id="555"),
                secrets=dict(SECRETS), input_source=os.path.join(EXAMPLES, "payloads", "conversions", "offline.json"))
    assert services["IngestionService"].requests == []


def test_the_status_example_keeps_a_row_per_destination_and_logs_the_failed_records(tmp_path, services,
                                                                                    caplog):
    result = run_job(_example(tmp_path, "requests/status.yaml"), config={"request_id": "req-9"},
                     secrets=dict(SECRETS))
    assert result.exit_code == 0, result.error
    assert services["IngestionService"].requests == [{"request_id": "req-9"}]
    with duckdb.connect(str(tmp_path / "dw.duckdb")) as con:
        assert con.execute("SELECT request_id, status FROM data_manager_request_status").fetchall() == [
            ("req-9", "PARTIAL_SUCCESS")]
    assert "1 record(s) failed with PROCESSING_ERROR_REASON_INVALID_GCLID: [2]" in caplog.text


class FakeGoogleAdsService:
    """GoogleAdsService.search_stream over two enabled upload-click actions (the query's IN list is not applied)."""

    def __init__(self):
        self.queries = []

    def search_stream(self, customer_id, query, **extras):
        self.queries.append((customer_id, query))
        types = GoogleAdsClient(AnonymousCredentials(), developer_token="x", use_proto_plus=True)
        response = types.get_type("SearchGoogleAdsStreamResponse")
        for action_id, name in ((111, "Offline purchase"), (222, "Offline qualified lead")):
            row = types.get_type("GoogleAdsRow")
            row.conversion_action.id = action_id
            row.conversion_action.name = name
            response.results.append(row)
        return iter([response])


def test_the_by_action_name_example_looks_up_the_actions_and_routes_each_event(tmp_path, services, monkeypatch,
                                                                                 caplog):
    ads = FakeGoogleAdsService()
    monkeypatch.setattr(oauth2, "get_credentials", lambda config: AnonymousCredentials())
    monkeypatch.setattr(GoogleAdsClient, "get_service", lambda client, name, **options: ads)
    secrets = dict(SECRETS, developer_token="dev-1")
    result = run_job(_example(tmp_path, "conversions/ingest_by_action_name.yaml"), config=dict(ACCOUNTS),
                     secrets=secrets,
                     input_source=os.path.join(EXAMPLES, "payloads", "conversions", "offline_by_action_name.json"))
    assert result.exit_code == 0, result.error
    ((customer, query),) = ads.queries
    assert customer == "1234567890" and "conversion_action.name IN ('No such action', 'Offline purchase', " in query
    (request,) = services["IngestionService"].requests                  # one request, a destination per action
    by_reference = {item.reference: item for item in request.destinations}
    assert {event.transaction_id: [by_reference[reference].product_destination_id
                                   for reference in event.destination_references] for event in request.events} == {
        "EXAMPLE-ORDER-0101": ["111"], "EXAMPLE-LEAD-0102": ["222"], "EXAMPLE-ORDER-0103": ["111"]}
    assert {item.login_account.account_id for item in request.destinations} == {"1112223333"}
    assert "skipped transaction EXAMPLE-ORDER-0104: no enabled upload-click action named No such action" in caplog.text


def test_the_conversions_example_logs_google_s_rejections_and_warnings(tmp_path, services, invalid_argument,
                                                                       caplog):
    caplog.set_level(logging.INFO)
    ingestion = services["IngestionService"]
    ingestion.errors = [invalid_argument(
        ("events.events[2].user_data.user_identifiers", "INVALID_HEX_ENCODING", "Email is not hex encoded."))]
    ingestion.warnings = [("events.events[0].currency", "WARNING_REASON_UNSPECIFIED", "Currency ignored.")]
    config = dict(ACCOUNTS, conversion_action_id="555")
    result = run_job(_example(tmp_path, "conversions/ingest.yaml"), config=config, secrets=dict(SECRETS),
                     input_source=os.path.join(EXAMPLES, "payloads", "conversions", "offline.json"))
    assert result.exit_code == 0, result.error
    rejected = ingestion.requests[0].events[2].transaction_id
    accepted = ingestion.requests[1].events[0].transaction_id
    assert "request req-2: applied: 2 event(s)" in caplog.text
    assert "request -: failed: 1 event(s)" in caplog.text
    assert ("transaction %s: INVALID_HEX_ENCODING: Email is not hex encoded. (field: user_data.user_identifiers)"
            % rejected) in caplog.text
    assert 'transaction %s: warnings: [{"field":"currency","description":"Currency ignored."}]' % accepted in (
        caplog.text)
