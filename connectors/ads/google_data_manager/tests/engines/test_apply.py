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
"""Apply outputs and call steps through google_data_manager, from a DuckDBJob, against fake service clients."""

import hashlib
import json
import sqlite3
from types import MappingProxyType

from google.ads import datamanager_v1 as dm

from streamwright.core.jobs.duckdb.engine import run_job
from streamwright.core.jobs.duckdb.plugin import JOB_ENGINE

SECRETS = MappingProxyType({"client_id": "client-1", "client_secret": "secret-1", "refresh_token": "refresh-1"})
DESTINATION = MappingProxyType({"operating_account": {"account_type": "GOOGLE_ADS", "account_id": "1234567890"},
                                "product_destination_id": "555"})


def _job(tmp_path, sql, operation=None, steps=(), **apply):
    operation = dict({"service": "IngestionService", "method": "ingest_events",
                      "arguments": {"destinations": [dict(DESTINATION)],
                                    "consent": {"ad_user_data": "CONSENT_GRANTED"}}}, **(operation or {}))
    output = {"name": "push", "connection": "dm", "step": "rows", "operation": operation}
    if apply:
        output["apply"] = apply
    return {
        "kind": "DuckDBJob", "name": "dm_push",
        "spec": {"secrets": {name: {"type": "string", "required": True} for name in SECRETS}},
        "connections": {"dm": {"provider": "google_data_manager",
                               **{name: "{{ secrets.%s }}" % name for name in SECRETS}}},
        "steps": [*steps, {"name": "rows", "sql": sql}],
        "outputs": [output],
        "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}},
    }


def _outbox(tmp_path):
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        return [(row[0], json.loads(row[1]) if row[1] else None, row[2], row[3]) for row in con.execute(
            "SELECT status, result, error_code, error_message FROM sw_events ORDER BY sequence")]


# a SQL timestamp, a STRUCT (ad_identifiers), a LIST of STRUCTs and JSON text (user_data), a dotted column
EVENTS = """
    SELECT 't-' || event_number AS transaction_id,
           TIMESTAMPTZ '2026-11-20 10:00:00+05:30' + to_hours(event_number) AS event_timestamp,
           {'gclid': 'g-' || event_number} AS ad_identifiers,
           CASE WHEN event_number = 0
                THEN json_object('user_identifiers', [json_object('email_address', sha256('a@b.com'))])
           END AS user_data,
           12.5 * event_number AS conversion_value,
           'USD' AS currency,
           'WEB' AS event_source,
           CASE WHEN event_number = 1 THEN 'CONSENT_DENIED' END AS "consent.ad_personalization"
    FROM range(3) AS events(event_number)
"""


def test_an_ingest_output_validates_against_the_installed_library(tmp_path):
    assert JOB_ENGINE.validate_job(_job(tmp_path, EVENTS)).valid
    bad = _job(tmp_path, EVENTS, operation={"method": "ingest_event"})
    issues = [str(issue) for issue in JOB_ENGINE.validate_job(bad).issues]
    assert any("did you mean ingest_events" in issue for issue in issues), issues


def test_events_are_ingested_a_request_per_chunk_with_their_request_ids(tmp_path, services):
    result = run_job(_job(tmp_path, EVENTS, chunk_size=2, nulls="skip_field"), secrets=dict(SECRETS))
    assert result.exit_code == 0, result.error
    first, second = services["IngestionService"].requests
    assert [len(request.events) for request in (first, second)] == [2, 1]
    assert first.destinations[0].operating_account.account_id == "1234567890"
    assert first.destinations[0].product_destination_id == "555"
    assert first.consent.ad_user_data == dm.ConsentStatus.CONSENT_GRANTED
    assert first.encoding == dm.Encoding.HEX and first.validate_only is False          # the declared default
    event, other = first.events
    assert event.transaction_id == "t-0" and event.ad_identifiers.gclid == "g-0"
    assert event.event_timestamp.isoformat() == "2026-11-20T04:30:00+00:00"
    # user_data from JSON text
    assert event.user_data.user_identifiers[0].email_address == hashlib.sha256(b"a@b.com").hexdigest()
    assert event.event_source == dm.EventSource.WEB and event.currency == "USD"
    assert other.conversion_value == 12.5 and other.consent.ad_personalization == dm.ConsentStatus.CONSENT_DENIED
    assert [result for _, result, _, _ in _outbox(tmp_path)] == [
        {"request_id": "req-1", "record_index": 0}, {"request_id": "req-1", "record_index": 1},
        {"request_id": "req-2", "record_index": 0}]


def test_rows_google_rejects_fail_alone_and_the_rest_are_sent_again(tmp_path, services, invalid_argument):
    ingestion = services["IngestionService"]
    # Google's paths (`events.events[1]...`); a violation may have no description
    ingestion.errors = [invalid_argument(
        ("events.events[1].user_data.user_identifiers", "INVALID_HEX_ENCODING", "Email is not hex encoded."),
        ("events.events[1]", "INVALID_SHA256_FORMAT", ""))]
    ingestion.warnings = [
        ("events.events[1].cart_data.items[0].merchant_product_id",
         "WARNING_REASON_CART_DATA_ITEM_MERCHANT_PRODUCT_ID_MISSING", "The merchant product ID is missing."),
        ("consent", "WARNING_REASON_UNSPECIFIED", "Not about a record.")]
    result = run_job(_job(tmp_path, EVENTS, nulls="skip_field", on_row_error="continue"), secrets=dict(SECRETS))
    assert result.exit_code == 0, result.error
    assert [len(request.events) for request in ingestion.requests] == [3, 2]
    assert [event.transaction_id for event in ingestion.requests[1].events] == ["t-0", "t-2"]
    assert _outbox(tmp_path) == [
        ("applied", {"request_id": "req-2", "record_index": 0}, None, None),
        ("failed", None, "INVALID_HEX_ENCODING",
         "Email is not hex encoded. (field: user_data.user_identifiers); INVALID_SHA256_FORMAT"),
        ("applied", {"request_id": "req-2", "record_index": 1,
                     "warnings": [{"field": "cart_data.items[0].merchant_product_id",
                                   "reason": "WARNING_REASON_CART_DATA_ITEM_MERCHANT_PRODUCT_ID_MISSING",
                                   "description": "The merchant product ID is missing."}]}, None, None)]


def test_a_request_level_rejection_fails_the_chunk(tmp_path, services, invalid_argument):
    services["IngestionService"].errors = [invalid_argument(
        ("destinations[0].operating_account", "INVALID_ACCOUNT", "Unknown account."))]
    result = run_job(_job(tmp_path, EVENTS, nulls="skip_field"), secrets=dict(SECRETS))
    assert result.exit_code != 0 and result.rejected == {"push": 3}
    assert {message for _, _, _, message in _outbox(tmp_path)} == {
        "google_data_manager: INVALID_ARGUMENT: There was a problem with the request. "
        "(destinations[0].operating_account: Unknown account.) (request id support-1)"}
    assert len(services["IngestionService"].requests) == 1                     # not retried: INVALID_ARGUMENT


def test_a_row_that_does_not_fit_fails_alone_and_validate_only_changes_nothing(tmp_path, services):
    sql = "SELECT * FROM (VALUES ('t-1', NULL), ('t-2', 'x')) t(transaction_id, bogus_field)"
    arguments = {"destinations": [dict(DESTINATION)], "validate_only": True}
    job = _job(tmp_path, sql, operation={"arguments": arguments}, nulls="skip_field", on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    (request,) = services["IngestionService"].requests
    assert request.validate_only is True and [event.transaction_id for event in request.events] == ["t-1"]
    outbox = _outbox(tmp_path)
    assert outbox[0][:3] == ("applied", {"request_id": "req-1", "record_index": 0, "validate_only": True}, None)
    assert outbox[1][2] == "INVALID_ROW" and "bogus_field" in outbox[1][3]


def test_audience_members_are_added_and_removing_needs_allow_remove(tmp_path, services):
    sql = "SELECT {'user_identifiers': [{'email_address': sha256('a@b.com')}]} AS user_data"
    terms = {"customer_match_terms_of_service_status": "ACCEPTED"}
    add = {"method": "ingest_audience_members",
           "arguments": {"destinations": [dict(DESTINATION)], "terms_of_service": terms}}
    assert run_job(_job(tmp_path, sql, operation=add), secrets=dict(SECRETS)).exit_code == 0
    (request,) = services["IngestionService"].requests
    assert len(request.audience_members[0].user_data.user_identifiers) == 1
    assert request.terms_of_service.customer_match_terms_of_service_status == dm.TermsOfServiceStatus.ACCEPTED

    (tmp_path / "remove").mkdir()
    remove = {"method": "remove_audience_members", "arguments": {"destinations": [dict(DESTINATION)]}}
    job = _job(tmp_path / "remove", "SELECT 'remove' AS op, " + sql[len("SELECT "):], operation=remove,
               on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    assert [code for _, _, code, _ in _outbox(tmp_path / "remove")] == ["REMOVE_NOT_ALLOWED"]
    assert len(services["IngestionService"].requests) == 1

    (tmp_path / "removed").mkdir()                       # allowed: a RemoveAudienceMembersResponse has no warnings
    job = _job(tmp_path / "removed", "SELECT 'remove' AS op, " + sql[len("SELECT "):], operation=remove,
               allow_remove=True)
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    assert [result for _, result, _, _ in _outbox(tmp_path / "removed")] == [{"request_id": "req-2", "record_index": 0}]


def test_the_request_status_and_paged_user_lists_are_read_in_call_steps(tmp_path, services):
    steps = [
        {"name": "status", "connection": "dm",
         "operation": {"service": "IngestionService", "method": "retrieve_request_status",
                       "arguments": {"request_id": "req-9"}},
         "sql": "SELECT record->>'$.request_id' AS request_id, record->>'$.request_status' AS status, "
                "record->'$.error_info.error_counts[0].record_indices' AS failed FROM records"},
        {"name": "lists", "connection": "dm",
         "operation": {"service": "UserListService", "method": "list_user_lists",
                       "arguments": {"parent": "accountTypes/GOOGLE_ADS/accounts/1234567890"}},
         "sql": "SELECT record->>'$.display_name' AS name FROM records ORDER BY name"},
    ]
    sql = ("SELECT s.request_id || ':' || s.status || ':' || CAST(s.failed AS VARCHAR) AS transaction_id "
           "FROM status AS s UNION ALL SELECT name FROM lists")
    job = _job(tmp_path, sql, steps=steps, operation={"arguments": {"destinations": [dict(DESTINATION)],
                                                                    "validate_only": True}})
    result = run_job(job, secrets=dict(SECRETS))
    assert result.exit_code == 0, result.error
    sent = [event.transaction_id for event in services["IngestionService"].requests[-1].events]
    assert sorted(sent) == ["Buyers", "Leads", 'req-9:PARTIAL_SUCCESS:[2]']
    assert services["IngestionService"].requests[0] == {"request_id": "req-9"}
    assert [request.get("page_token") for request in services["UserListService"].requests] == [None, "p2"]
    assert services["UserListService"].requests[0]["parent"] == "accountTypes/GOOGLE_ADS/accounts/1234567890"


def _destination_sql(action):
    return ("{'operating_account': {'account_type': 'GOOGLE_ADS', 'account_id': '1234567890'}, "
            "'product_destination_id': '%s'}" % action)


def _sent(request):
    """{transaction_id: [the product_destination_id of each destination it references]}."""
    by_reference = {item.reference: item.product_destination_id for item in request.destinations}
    return {event.transaction_id: [by_reference[reference] for reference in event.destination_references]
            for event in request.events}


def test_rows_with_their_own_destination_share_one_request_by_reference(tmp_path, services):
    sql = ("SELECT 't-' || event_number AS transaction_id, 'WEB' AS event_source, "
           "TIMESTAMPTZ '2026-11-20 10:00:00+00' AS event_timestamp, "
           "{'gclid': 'g-' || event_number} AS ad_identifiers, "
           "CASE WHEN event_number % 2 = 0 THEN " + _destination_sql("111") + " ELSE " + _destination_sql("222") +
           " END AS destination FROM range(3) AS events(event_number)")
    job = _job(tmp_path, sql, operation={"arguments": {"consent": {"ad_user_data": "CONSENT_GRANTED"}}})
    result = run_job(job, secrets=dict(SECRETS))
    assert result.exit_code == 0, result.error
    (request,) = services["IngestionService"].requests                  # Google's model: one request, references
    assert [(item.reference, item.product_destination_id) for item in request.destinations] == [
        ("destination_1", "111"), ("destination_2", "222")]
    assert _sent(request) == {"t-0": ["111"], "t-1": ["222"], "t-2": ["111"]}
    assert request.consent.ad_user_data == dm.ConsentStatus.CONSENT_GRANTED
    assert [result for _, result, _, _ in _outbox(tmp_path)] == [
        {"request_id": "req-1", "record_index": index} for index in range(3)]


def test_known_destinations_are_referenced_with_the_api_s_own_fields(tmp_path, services):
    destinations = [dict(DESTINATION, product_destination_id="111", reference="purchase"),
                    dict(DESTINATION, product_destination_id="222", reference="lead")]
    sql = ("SELECT * FROM (VALUES ('t-1', ['purchase']), ('t-2', ['lead']), ('t-3', NULL), ('t-4', ['sale'])) "
           "AS rows(transaction_id, destination_references)")
    job = _job(tmp_path, sql, operation={"arguments": {"destinations": destinations}}, nulls="skip_field",
               on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    (request,) = services["IngestionService"].requests
    assert [item.reference for item in request.destinations] == ["purchase", "lead"]
    assert [list(event.destination_references) for event in request.events] == [["purchase"], ["lead"], []]
    assert [code for _, _, code, _ in _outbox(tmp_path)] == [None, None, None, "INVALID_ROW"]   # not the whole chunk


def test_an_event_may_go_to_several_destinations_and_rows_mix_with_the_output_s(tmp_path, services):
    both = "[" + _destination_sql("111") + ", " + _destination_sql("222") + "]"
    sql = ("SELECT * FROM (VALUES ('t-1', CAST(to_json(" + both + ") AS VARCHAR)), ('t-2', NULL)) "
           "AS rows(transaction_id, destination)")
    job = _job(tmp_path, sql, nulls="skip_field")                         # the output's: DESTINATION (555)
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    (request,) = services["IngestionService"].requests
    assert _sent(request) == {"t-1": ["111", "222"], "t-2": ["555"]}      # every event names its destinations


def test_past_ten_destinations_a_chunk_splits_and_a_failed_request_fails_its_own_rows(tmp_path, services,
                                                                                      invalid_argument):
    services["IngestionService"].errors = [invalid_argument(
        ("destinations[3].product_destination_id", "INVALID_PRODUCT_DESTINATION_ID", "Unknown."))]
    sql = ("SELECT 't-' || action AS transaction_id, {'operating_account': {'account_type': 'GOOGLE_ADS', "
           "'account_id': '1234567890'}, 'product_destination_id': CAST(action AS VARCHAR)} AS destination "
           "FROM range(100, 112) AS actions(action)")
    job = _job(tmp_path, sql, operation={"arguments": {}}, on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    first, second = services["IngestionService"].requests
    assert (len(first.destinations), len(second.destinations)) == (10, 2)
    outbox = _outbox(tmp_path)
    assert [code for _, _, code, _ in outbox] == ["INVALID_ARGUMENT"] * 10 + [None, None]
    assert "destinations[3].product_destination_id: Unknown." in outbox[0][3]


def test_rows_without_a_usable_destination_fail_alone(tmp_path, services):
    sql = ("SELECT * FROM (VALUES ('t-1', NULL, NULL), ('t-2', '{\"operating_account\": \"nope\"}', NULL), "
           "('t-3', CAST(to_json(" + _destination_sql("111") + ") AS VARCHAR), ['x'])) "
           "AS rows(transaction_id, destination, destination_references)")
    job = _job(tmp_path, sql, operation={"arguments": {}}, nulls="skip_field", on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    outbox = _outbox(tmp_path)
    assert [code for _, _, code, _ in outbox] == ["INVALID_ROW"] * 3
    assert "no destination" in outbox[0][3] and outbox[1][3].startswith("destination: ")
    assert "not both" in outbox[2][3] and services["IngestionService"].requests == []


def test_a_reference_the_output_does_not_have_fails_its_row_instead_of_reaching_every_destination(tmp_path,
                                                                                                    services):
    destinations = [dict(DESTINATION, reference="purchase")]                        # 555
    sql = ("SELECT * FROM (VALUES ('t-1', CAST(to_json(" + _destination_sql("111") + ") AS VARCHAR), NULL), "
           "('t-2', NULL, ['typo']), ('t-3', NULL, ['purchase']), ('t-4', NULL, ['purchase', 'typo'])) "
           "AS rows(transaction_id, destination, destination_references)")
    job = _job(tmp_path, sql, operation={"arguments": {"destinations": destinations}}, nulls="skip_field",
               on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    (request,) = services["IngestionService"].requests
    assert _sent(request) == {"t-1": ["111"], "t-3": ["555"]}
    outbox = _outbox(tmp_path)
    assert [code for _, _, code, _ in outbox] == [None, "INVALID_ROW", None, "INVALID_ROW"]
    assert outbox[1][3] == ("destination_references typo: not the reference of any of the output's destinations "
                            "(purchase)")


def test_a_row_with_more_destinations_than_a_request_takes_fails_alone(tmp_path, services):
    many = "[" + ", ".join(_destination_sql(str(action)) for action in range(100, 111)) + "]"
    sql = ("SELECT * FROM (VALUES ('t-1', CAST(to_json(" + many + ") AS VARCHAR)), ('t-2', NULL)) "
           "AS rows(transaction_id, destination)")
    job = _job(tmp_path, sql, nulls="skip_field", on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    (request,) = services["IngestionService"].requests
    assert _sent(request) == {"t-2": ["555"]}
    outbox = _outbox(tmp_path)
    assert [code for _, _, code, _ in outbox] == ["INVALID_ROW", None]
    assert outbox[0][3] == "11 destinations: a request takes at most 10"


def test_an_event_needs_its_transaction_id_so_a_resend_is_deduplicated(tmp_path, services):
    sql = "SELECT * FROM (VALUES ('t-1'), (NULL)) AS rows(transaction_id)"
    job = _job(tmp_path, sql, nulls="skip_field", on_row_error="continue")
    assert run_job(job, secrets=dict(SECRETS)).exit_code == 0
    (request,) = services["IngestionService"].requests
    assert [event.transaction_id for event in request.events] == ["t-1"]
    outbox = _outbox(tmp_path)
    assert [code for _, _, code, _ in outbox] == [None, "INVALID_ROW"] and "no transaction_id" in outbox[1][3]
