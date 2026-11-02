# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the google_ads connector accepts: its connection fields and its read-only policy over the Google Ads API (pure
data). `GoogleAdsService.search`/`search_stream` are typed (a `customer_id` and a GAQL `query`); the `get_*`/`list_*`
read methods of any `*Service` are families (contract v1.3 §6b) whose request fields are vendor arguments. The
installed google-ads library decides which services, methods and request fields exist; the connector never lists them.
"""

from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, OperationKind

__all__ = ["CONNECTOR"]

_CUSTOMER = Field("customer_id", ("string", "integer"), required=True, pattern=r"^[0-9-]+$",
                  description="the customer to query, digits (dashes are removed)")
_QUERY = Field("query", "string", required=True, literal=True,
               description="GAQL text, or `{gaql: {...}}`, the query builder that writes values safely")


def _search(method: str, description: str) -> Operation:
    return Operation(service="GoogleAdsService", method=method, kind=OperationKind.READ,
                     description=description, arguments=(_CUSTOMER, _QUERY))


def _read(method: str, description: str) -> Operation:
    return Operation(service="*Service", method=method, kind=OperationKind.READ, description=description)


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
    ),
    adapters={"source": "streamwright.connectors.google_ads.engines.source:GoogleAdsSourceAdapter"},
    validator="streamwright.connectors.google_ads.client:validate_connection",
    surface="streamwright.connectors.google_ads.surface:surface",
    query_builders={"gaql": "streamwright.connectors.google_ads.gaql:GaqlBuilder"},
    # gRPC statuses and Google Ads error names worth retrying; a stream's `retry.codes` replace them
    retry_codes=("RESOURCE_EXHAUSTED", "UNAVAILABLE", "DEADLINE_EXCEEDED", "INTERNAL", "ABORTED",
                 "RESOURCE_TEMPORARILY_EXHAUSTED", "INTERNAL_ERROR", "TRANSIENT_ERROR"),
)
