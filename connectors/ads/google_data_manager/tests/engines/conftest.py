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
"""Fake Data Manager service clients (google-ads-datamanager types), in place of the real ones."""

from types import MappingProxyType

import pytest
from google.ads import datamanager_v1 as dm
from google.api_core import exceptions as api_errors
from google.rpc import error_details_pb2

from streamwright.connectors.google_data_manager import client

STATUS_DESTINATION = MappingProxyType({"operating_account": {"account_type": "GOOGLE_ADS", "account_id": "1234567890"},
                                       "product_destination_id": "555"})


def invalid(*violations):
    """
    The INVALID_ARGUMENT Google raises for a request with bad fields, as its guide shows: an ErrorInfo, a BadRequest
    of (field, reason, description) violations, and a RequestInfo.
    """
    info = error_details_pb2.ErrorInfo(reason="INVALID_ARGUMENT", domain="datamanager.googleapis.com")
    detail = error_details_pb2.BadRequest(field_violations=[
        error_details_pb2.BadRequest.FieldViolation(field=field, reason=reason, description=text)
        for field, reason, text in violations])
    return api_errors.InvalidArgument("There was a problem with the request.", error_info=info, details=[
        info, detail, error_details_pb2.RequestInfo(request_id="support-1")])


class FakeIngestionService:
    def __init__(self):
        self.requests = []
        self.errors = []            # raised by the next calls, in order
        self.warnings = ()

    def _respond(self, request, response_type):
        self.requests.append(request)
        if self.errors:
            raise self.errors.pop(0)
        fields = set(response_type.meta.fields)                       # the response's own fields, as Google's
        found = {"request_id": "req-%d" % len(self.requests), "field_warnings": [
            dm.FieldWarning(field=field, reason=reason, description=text) for field, reason, text in self.warnings]}
        return response_type({key: value for key, value in found.items() if key in fields})

    def ingest_events(self, request):
        return self._respond(request, dm.IngestEventsResponse)

    def ingest_audience_members(self, request):
        return self._respond(request, dm.IngestAudienceMembersResponse)

    def remove_audience_members(self, request):
        return self._respond(request, dm.RemoveAudienceMembersResponse)

    def retrieve_request_status(self, request):
        self.requests.append(request)
        status = dm.RequestStatusPerDestination(
            destination=dm.Destination(**STATUS_DESTINATION), request_status="PARTIAL_SUCCESS",
            events_ingestion_status={"record_count": 3},
            error_info={"error_counts": [{"record_count": 1, "reason": "PROCESSING_ERROR_REASON_INVALID_GCLID",
                                          "record_indices": [2]}]})
        return dm.RetrieveRequestStatusResponse(request_status_per_destination=[status])


class FakeUserListService:
    def __init__(self):
        self.requests = []

    def list_user_lists(self, request):
        self.requests.append(dict(request))
        if not request.get("page_token"):
            return dm.ListUserListsResponse(user_lists=[dm.UserList(id=1, display_name="Buyers")], next_page_token="p2")
        return dm.ListUserListsResponse(user_lists=[dm.UserList(id=2, display_name="Leads")])


@pytest.fixture
def invalid_argument():
    """invalid_argument((field, description), ...): the INVALID_ARGUMENT Google raises for those fields."""
    return invalid


@pytest.fixture
def services(monkeypatch):
    found = {"IngestionService": FakeIngestionService(), "UserListService": FakeUserListService()}
    monkeypatch.setattr(client.DataManagerConnection, "client", lambda connection, service: found[service])
    return found
