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

"""The google_data_manager spec: certified, writes only as apply outputs, and the library's errors translated."""

from types import MappingProxyType

import pytest
from google.ads import datamanager_v1 as dm
from google.api_core import exceptions as api_errors
from google.auth.credentials import AnonymousCredentials
from google.protobuf import duration_pb2
from google.rpc import error_details_pb2

from streamwright.connectors.google_data_manager import CONNECTOR, client, ingestion
from streamwright.core.runtime.source_connectors import check_call
from streamwright.core.sdk import ConnectorError
from streamwright.core.sdk.contract import check_connection
from streamwright.core.sdk.engines.source import ConnectorContext
from streamwright.core.sdk.testing import certify, components

TestCertification = certify(CONNECTOR)

DECLARED = MappingProxyType({"client_id": "{{ secrets.cid }}", "client_secret": "{{ secrets.cs }}",
                             "refresh_token": "{{ secrets.rt }}"})


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_the_credentials_are_secret_references_of_one_kind():
    assert check_connection(CONNECTOR, DECLARED) == []
    assert check_connection(CONNECTOR, {"service_account_json": "{{ secrets.key }}"}) == []
    assert codes(check_connection(CONNECTOR, dict(DECLARED, refresh_token="1//written"))) == [
        ("literal-secret", "refresh_token")]
    assert client.validate_connection({}) == [
        "one of refresh_token (with client_id and client_secret) or service_account_json is required"]
    assert client.validate_connection({"refresh_token": "r"}) == ["refresh_token needs client_id and client_secret"]
    assert client.validate_connection({"refresh_token": "r", "service_account_json": "{}"}) == [
        "refresh_token and service_account_json are two kinds of credentials: give one"]


def test_writes_are_apply_operations_and_streams_read_only():
    applies = {operation.name: (operation.idempotent, operation.max_chunk, operation.destructive_actions)
               for operation in CONNECTOR.operations if operation.kind == "apply"}
    assert applies == {"IngestionService.ingest_events": (True, 2000, ()),
                       "IngestionService.ingest_audience_members": (True, 10000, ()),
                       "IngestionService.remove_audience_members": (True, 10000, ("remove",)),
                       "IngestionService.ingest_*": (False, 1000, ()),
                       "IngestionService.remove_*": (False, 1000, ("remove",))}
    users = CONNECTOR.operation("IngestionService", "ingest_users")          # the family, bound to the method
    assert (users.name, users.idempotent, users.max_chunk) == ("IngestionService.ingest_users", False, 1000)
    connector = components.load("google_data_manager")
    (problem,) = check_call(connector, {"service": "IngestionService", "method": "ingest_events"})
    assert "IngestionService.ingest_events is not supported" in problem      # a write runs as an apply output only
    assert check_call(connector, {"service": "UserListService", "method": "list_user_lists"}) == []
    assert check_call(connector, {"service": "IngestionService", "method": "retrieve_request_status",
                                  "arguments": {"request_id": "r"}}) == []


def test_a_typo_is_caught_against_the_installed_sdk_before_a_run():
    connector = components.load("google_data_manager")
    (problem,) = check_call(connector, {"service": "UserListService", "method": "list_user_list"})
    assert "UserListService has no list_user_list method" in problem and "list_user_lists" in problem
    (problem,) = check_call(connector, {"service": "UserListsService", "method": "list_user_lists"})
    assert "UserListsService is not a service" in problem and "UserListService" in problem


def _quota_error(quota, delay=None):
    details = [error_details_pb2.ErrorInfo(reason="RATE_LIMIT_EXCEEDED", metadata={"quota_limit": quota})]
    if delay:
        details.append(error_details_pb2.RetryInfo(retry_delay=duration_pb2.Duration(seconds=delay)))
    return api_errors.ResourceExhausted("Quota exceeded.", details=details, error_info=details[0])


def test_a_per_minute_quota_is_retried_after_googles_wait_and_a_daily_one_is_not():
    context = ConnectorContext("google_data_manager", str, default_codes=CONNECTOR.retry_codes)
    minute = client.translate_error(_quota_error("IngestionMutateRequestsPerMinutePerProject", delay=7))
    assert minute.codes == ("RATE_LIMIT_EXCEEDED", "RESOURCE_EXHAUSTED") and minute.retry_after == 7
    assert context.retryable(minute)
    daily = client.translate_error(_quota_error("IngestionMutateRequestsPerDayPerProject"))
    assert daily.codes == (client.DAILY_QUOTA, "IngestionMutateRequestsPerDayPerProject")
    assert not context.retryable(daily) and "daily quota" in str(daily)


def test_rejected_fields_are_kept_with_the_error():
    detail = error_details_pb2.BadRequest(field_violations=[error_details_pb2.BadRequest.FieldViolation(
        field="events.events[%d].currency" % index, reason="INVALID_CURRENCY", description="bad")
        for index in range(7)])
    error = client.translate_error(api_errors.InvalidArgument("Invalid.", details=[detail]))
    assert len(error.violations) == 7 and error.codes == ("INVALID_ARGUMENT",)
    assert str(error).endswith("events.events[4].currency: bad, and 2 more)")
    assert ingestion._by_record(error.violations, "events", 7)[6] == [
        {"field": "currency", "reason": "INVALID_CURRENCY", "description": "bad"}]
    # over REST: the details are dicts, the names camelCase
    rest = client._violations([{"fieldViolations": [{"field": "audienceMembers.audienceMembers[1].userData",
                                                     "reason": "INVALID_SHA256_FORMAT"}]}])
    assert ingestion._by_record(rest, "audience_members", 2) == {1: [{"field": "userData",
                                                                      "reason": "INVALID_SHA256_FORMAT"}]}
    assert ingestion._by_record([("destinations[0]", "INVALID", "x")], "events", 2) is None


def test_a_service_account_key_must_be_one():
    with pytest.raises(ConnectorError, match="neither a service account key nor the path of one"):
        client.credentials({"service_account_json": "{not json"}, str)
    with pytest.raises(ConnectorError, match="invalid service account key"):
        client.credentials({"service_account_json": '{"type": "service_account"}'}, str)


def test_refreshed_access_tokens_are_masked():
    remembered = []
    credentials = client.credentials(dict.fromkeys(("client_id", "client_secret", "refresh_token"), "x"),
                                     remembered.append)
    assert credentials.scopes == [client.SCOPE]

    class Refreshing:
        token = "old"

        def refresh(self, request):
            self.token = "new"
    found = Refreshing()
    client.remember_tokens(found, remembered.append)
    found.refresh(None)
    assert remembered[-2:] == ["old", "new"]


@pytest.mark.parametrize(("value", "expected"), [
    ("2026-11-20 10:00:00+05:30", "2026-11-20T10:00:00+05:30"),       # DuckDB TIMESTAMPTZ text
    ("2026-11-20 04:30:00+00", "2026-11-20T04:30:00+00:00"),          # a whole-hour offset
    ("2026-11-20 04:30:00", "2026-11-20T04:30:00Z"),                  # naive: UTC
    ("2026-11-20T04:30:00Z", "2026-11-20T04:30:00Z"),
])
def test_sql_timestamps_become_rfc_3339(value, expected):
    assert ingestion._timestamp(value) == expected


def test_types_are_described_from_the_installed_library():
    event = components.describe_type("google_data_manager", "Event")
    assert event["kind"] == "object" and event["fields"]["user_data"] == "UserData"
    assert event["fields"]["destination_references"] == "list<string>"
    account = components.describe_type("google_data_manager", "ProductAccount.AccountType")
    assert account["kind"] == "enum" and "GOOGLE_ADS" in account["values"]
    assert components.describe_type("google_data_manager", "NoSuchThing") is None


def test_clients_accept_a_large_error_header(monkeypatch):
    """Google's guide: raise gRPC's metadata limit so many record violations are reported, not a header error."""
    seen = {}
    transport = dm.IngestionServiceClient.get_transport_class("grpc")
    create = transport.create_channel

    def create_channel(*args, **kwargs):
        seen.update(kwargs)
        return create(*args, **kwargs)
    monkeypatch.setattr(transport, "create_channel", create_channel)
    connection = client.DataManagerConnection(AnonymousCredentials())
    assert callable(connection.method("IngestionService", "ingest_events"))
    assert seen["options"] == [("grpc.max_metadata_size", 16 * 1024 * 1024)]
    connection.close()
