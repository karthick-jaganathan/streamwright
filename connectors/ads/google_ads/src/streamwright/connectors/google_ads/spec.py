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
What the google_ads connector accepts: its connection fields and its operations over the Google Ads API (pure data).
`GoogleAdsService.search`/`search_stream` are typed (a `customer_id` and a GAQL `query`); the `get_*`/`list_*` read
methods of any `*Service` are families (contract v1.3 §6b) whose request fields are vendor arguments. Apply operations
(apply outputs): the `mutate_*` methods of any `*Service` (create, update, remove rows; not idempotent) and the
conversion uploads (deduplicated by Google, so idempotent). The installed google-ads library decides which services,
methods and request fields exist; the connector never lists them.
"""

from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, OperationKind

__all__ = ["CONNECTOR"]

_CUSTOMER = Field("customer_id", ("string", "integer"), required=True, pattern=r"^[0-9-]+$",
                  description="the customer to query, digits (dashes are removed)")
_QUERY = Field("query", "string", required=True, literal=True,
               description="`{gaql: {select: [...], from: RESOURCE, where: [{field, op, type, value}], order_by, "
                           "limit}}`, the query builder that writes values safely (the only way to use config, "
                           "partition or window values; see the connector guide's Query builder section), or GAQL "
                           "text without `{{ }}` references")


def _search(method: str, description: str) -> Operation:
    return Operation(service="GoogleAdsService", method=method, kind=OperationKind.READ,
                     description=description, arguments=(_CUSTOMER, _QUERY))


def _read(method: str, description: str) -> Operation:
    return Operation(service="*Service", method=method, kind=OperationKind.READ, description=description)


_APPLY_ARGUMENTS = (
    _CUSTOMER,
    Field("partial_failure", "boolean", default=True,
          description="a rejected row fails alone (true) or the whole call (false)"),
    Field("validate_only", "boolean", default=False,
          description="Google checks every row and changes nothing (a dry run of the call)"),
)


CONNECTOR = ConnectorDefinition(
    name="google_ads",
    title="Google Ads",
    family="advertising",
    summary="Google Ads API campaigns, ad groups, keywords and metrics through GAQL",
    connection=(
        Field("developer_token", "string", required=True, secret=True, description="the Google Ads developer token"),
        Field("client_id", "string", required=True, secret=True, description="the OAuth client id"),
        Field("client_secret", "string", required=True, secret=True, description="the OAuth client secret"),
        Field("refresh_token", "string", required=True, secret=True, description="the OAuth refresh token"),
        Field("login_customer_id", ("string", "integer"), description="the manager (MCC) account the calls go through"),
        Field("api_version", "string", pattern=r"^v[0-9]+$",
              description="the API version, e.g. v18 (default: the library's)"),
    ),
    operations=(
        _search("search_stream", "Stream the rows of a GAQL query, in the batches the API sends."),
        _search("search", "Page through the rows of a GAQL query."),
        _read("get_*", "Read one resource of any `*Service` (its request fields are vendor arguments)."),
        _read("list_*", "List the resources of any `*Service` (its request fields are vendor arguments)."),
        Operation(service="*Service", method="mutate_*", kind=OperationKind.APPLY, idempotent=False, max_chunk=5000,
                  actions=("create", "update", "remove"), destructive_actions=("remove",), arguments=_APPLY_ARGUMENTS,
                  description="Create, update or remove resources of any `*Service`, a row each (columns are the "
                              "resource's field paths; `op` the action; update needs `resource_name`)."),
        # the conversion uploads (upload_click_conversions, upload_call_conversions, upload_conversion_adjustments):
        # Google deduplicates them, so a call cut short is sent again
        Operation(service="*UploadService", method="upload_*", kind=OperationKind.APPLY, idempotent=True,
                  max_chunk=2000, actions=("upload",), arguments=_APPLY_ARGUMENTS,
                  description="Upload conversions or conversion adjustments, a row each (e.g. "
                              "ConversionUploadService.upload_click_conversions: gclid, conversion_action, "
                              "conversion_date_time, ...)."),
    ),
    adapters={"source": "streamwright.connectors.google_ads.engines.source:GoogleAdsSourceAdapter"},
    validator="streamwright.connectors.google_ads.client:validate_connection",
    surface="streamwright.connectors.google_ads.surface:surface",
    query_builders={"gaql": "streamwright.connectors.google_ads.gaql:GaqlBuilder"},
    # gRPC statuses and Google Ads error names worth retrying; a stream's `retry.codes` replace them
    retry_codes=("RESOURCE_EXHAUSTED", "UNAVAILABLE", "DEADLINE_EXCEEDED", "INTERNAL", "ABORTED",
                 "RESOURCE_TEMPORARILY_EXHAUSTED", "INTERNAL_ERROR", "TRANSIENT_ERROR"),
    loggers=("google.ads.googleads.client",),
)
