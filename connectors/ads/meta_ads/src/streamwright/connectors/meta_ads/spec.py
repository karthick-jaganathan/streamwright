# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the meta_ads connector accepts: its connection fields and its read-only policy over the Marketing API (pure
data). The operations are families (contract v1.3 §6b): `*.api_get` reads any object, `*.get_*` reads any of its
edges. The installed facebook_business decides which objects, edges and fields exist; the connector never lists them.
"""

from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, OperationKind

__all__ = ["CONNECTOR"]

_ARGUMENTS = (
    Field("id", ("string", "integer"), required=True,
          description="the object's id, e.g. act_<account id> for an account"),
    Field("fields", ("list", "string"), item_type="string", description="the fields to read (text: comma-separated)"),
    Field("params", "mapping", description="the call's parameters, e.g. level and time_range of insights"),
)


def _read(method: str, description: str) -> Operation:
    return Operation(service="*", method=method, kind=OperationKind.READ, description=description,
                     arguments=_ARGUMENTS)


CONNECTOR = ConnectorDefinition(
    name="meta_ads",
    title="Meta Ads",
    family="advertising",
    summary="Meta Marketing API objects and edges (campaigns, ad sets, ads, insights)",
    connection=(
        Field("access_token", "string", required=True, secret=True,
              description="a system-user or long-lived user token"),
        Field("app_id", "string", description="the app id"),
        Field("app_secret", "string", secret=True, description="the app secret: enables appsecret_proof verification"),
        Field("api_version", "string", pattern=r"^v[0-9]+\.[0-9]+$", description="the Graph API version, e.g. v26.0"),
    ),
    operations=(
        _read("api_get", "Read the object itself (any Marketing API object)."),
        _read("get_*", "Read an edge of the object, in pages (any `get_<edge>` the object offers)."),
    ),
    adapters={"source": "streamwright.connectors.meta_ads.engines.source:MetaAdsSourceAdapter"},
    validator="streamwright.connectors.meta_ads.client:validate_connection",
    # Graph API error codes worth retrying (throttling, temporary errors); a stream's `retry.codes` replace them
    retry_codes=(1, 2, 4, 17, 32, 341, 613, 80000, 80001, 80002, 80003, 80004, 80005, 80006, 80008, 80009, 80014),
    surface="streamwright.connectors.meta_ads.client:surface",
)
