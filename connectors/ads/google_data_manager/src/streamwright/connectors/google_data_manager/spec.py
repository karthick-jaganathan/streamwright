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

"""
What the google_data_manager connector accepts: its connection fields and its operations over the Google Data Manager
API (pure data). Apply operations (apply outputs) are the IngestionService writes: events (conversions) and audience
members, a row each, deduplicated by Google (so idempotent: a call cut short is sent again; an event needs its
transaction_id, which Google deduplicates on); the other `ingest_*` and
`remove_*` methods are families whose request fields are vendor arguments. Reads are the request status of an
ingestion and the `list_*`/`get_*`/`search_*`/`retrieve_*` methods of any `*Service` (user lists, partner links,
licenses, reports). The installed google-ads-datamanager library decides which services, methods and fields exist.
"""

from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, OperationKind

__all__ = ["CONNECTOR"]

_DESTINATIONS = Field("destinations", "list",
                      description="the request's Destination objects (up to 10), e.g. {operating_account: "
                                  "{account_type: GOOGLE_ADS, account_id: '1234567890'}, login_account: {...}, "
                                  "product_destination_id: '<conversion action or user list id>', "
                                  "reference: 'purchase'} (an event's destination_references pick them); or each "
                                  "row's own `destination` "
                                  "column (one of the two is needed)")
_CONSENT = Field("consent", "mapping",
                 description="the request's consent ({ad_user_data: CONSENT_GRANTED, ad_personalization: ...}); a "
                             "row's own `consent` takes precedence")
_ENCODING = Field("encoding", "string", default="HEX", choices=("HEX", "BASE64"),
                  description="how the rows' hashed identifiers (email_address, phone_number, ...) are encoded")
_VALIDATE_ONLY = Field("validate_only", "boolean", default=False,
                       description="Google checks the request and ingests nothing (a dry run of the call)")
_TERMS = Field("terms_of_service", "mapping",
               description="the Customer Match terms of service, e.g. {customer_match_terms_of_service_status: "
                           "ACCEPTED}")


def _ingest(method: str, max_chunk: int, arguments: tuple[Field, ...], description: str) -> Operation:
    return Operation(service="IngestionService", method=method, kind=OperationKind.APPLY, idempotent=True,
                     max_chunk=max_chunk, actions=("ingest",), arguments=arguments, description=description)


def _read(method: str, description: str) -> Operation:
    return Operation(service="*Service", method=method, kind=OperationKind.READ, description=description)


CONNECTOR = ConnectorDefinition(
    name="google_data_manager",
    title="Google Data Manager",
    family="advertising",
    summary="Google Data Manager API: conversions and audience members ingested into Google Ads, GA4, DV360 and "
            "more; user lists and request status",
    connection=(
        Field("client_id", "string", secret=True, description="the OAuth client id of the refresh token"),
        Field("client_secret", "string", secret=True, description="the OAuth client secret of the refresh token"),
        Field("refresh_token", "string", secret=True,
              description="an OAuth refresh token granted the https://www.googleapis.com/auth/datamanager scope"),
        Field("service_account_json", "string", secret=True,
              description="a service account key (JSON text or the path of a JSON file), instead of a refresh token"),
    ),
    operations=(
        _ingest("ingest_events", 2000, (_DESTINATIONS, _CONSENT, _ENCODING, _VALIDATE_ONLY),
                "Ingest events (offline conversions, store sales, web and app events), a row each: the columns are "
                "Event fields (transaction_id, event_timestamp, ad_identifiers.gclid, user_data, "
                "conversion_value, currency, ...). Every event needs a transaction_id: Google deduplicates on it, "
                "so a call cut short is sent again safely."),
        _ingest("ingest_audience_members", 10000, (_DESTINATIONS, _CONSENT, _ENCODING, _VALIDATE_ONLY, _TERMS),
                "Add audience members to the destinations' user lists, a row each: the columns are AudienceMember "
                "fields (user_data, mobile_data, pair_data, consent, ...)."),
        Operation(service="IngestionService", method="remove_audience_members", kind=OperationKind.APPLY,
                  idempotent=True, max_chunk=10000, actions=("remove",), destructive_actions=("remove",),
                  arguments=(_DESTINATIONS, _ENCODING, _VALIDATE_ONLY),
                  description="Remove audience members from the destinations' user lists, a row each (AudienceMember "
                              "fields); an output must allow `remove`."),
        # the other ingestion writes (users, ad events, ...): their request fields are vendor arguments; not declared
        # idempotent, so a call cut short is held for an operator rather than sent again
        Operation(service="IngestionService", method="ingest_*", kind=OperationKind.APPLY, idempotent=False,
                  max_chunk=1000, actions=("ingest",),
                  description="Any other IngestionService ingest method, a row per element of its request's list "
                              "(request fields such as destinations are vendor arguments)."),
        Operation(service="IngestionService", method="remove_*", kind=OperationKind.APPLY, idempotent=False,
                  max_chunk=1000, actions=("remove",), destructive_actions=("remove",),
                  description="Any other IngestionService remove method, a row per element of its request's list "
                              "(request fields such as destinations are vendor arguments)."),
        Operation(service="IngestionService", method="retrieve_request_status", kind=OperationKind.READ,
                  arguments=(Field("request_id", "string", required=True,
                                   description="the request_id an ingestion returned (each applied row's result)"),),
                  description="The processing status of an ingestion, a row per destination (record counts, error "
                              "reasons and the failed record indices)."),
        _read("list_*", "List the resources of any `*Service`, page by page (its request fields are vendor "
                        "arguments, e.g. parent: accountTypes/GOOGLE_ADS/accounts/1234567890)."),
        _read("get_*", "Read one resource of any `*Service` (its request fields are vendor arguments)."),
        _read("search_*", "Search the resources of any `*Service`, page by page (vendor arguments)."),
        _read("retrieve_*", "Retrieve a report or insights of any `*Service` (its request fields are vendor "
                            "arguments)."),
    ),
    adapters={"source": "streamwright.connectors.google_data_manager.engines.source:DataManagerSourceAdapter"},
    validator="streamwright.connectors.google_data_manager.client:validate_connection",
    surface="streamwright.connectors.google_data_manager.surface:surface",
    describe_type="streamwright.connectors.google_data_manager.type_info:describe_type",
    # the transient statuses Google's guide says to retry (a daily quota is DAILY_QUOTA_EXHAUSTED, not retried); a
    # stream's `retry.codes` replace them
    retry_codes=("RESOURCE_EXHAUSTED", "UNAVAILABLE", "DEADLINE_EXCEEDED", "INTERNAL", "UNKNOWN", "ABORTED"),
    loggers=("google.ads.datamanager_v1", "google.auth"),
)
