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
What the meta_ads connector accepts: its connection fields and its operations over the Marketing API (pure data).
Reads are families (contract v1.3 §6b): `*.api_get` reads any object, `*.get_*` reads any of its edges. Apply
operations (apply outputs): `*.create_*` creates objects on a parent's edge (not idempotent), `*.api_update` and
`*.api_delete` change or delete objects by id, and `AdsPixel.create_event` sends Conversions API events (Meta
deduplicates them, so idempotent). The installed facebook_business decides which objects, edges and fields exist; the
connector never lists them.
"""

from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, OperationKind

__all__ = ["CONNECTOR"]

_ARGUMENTS = (
    Field("id", ("string", "integer"), required=True,
          description="the object's id, e.g. act_<account id> for an account"),
    Field("fields", ("list", "string"), item_type="string", description="the fields to read (text: comma-separated)"),
    Field("params", "mapping", description="the call's parameters, e.g. level and time_range of insights"),
)


_PARAMS = Field("params", "mapping",
                description="parameters every row gets, e.g. {status: PAUSED}; a row's own columns win")
_CREATE_ARGUMENTS = (
    Field("id", ("string", "integer"), required=True,
          description="the parent object's id, e.g. act_<account id> to create campaigns or ad sets in an account"),
    _PARAMS,
)
_EVENT_ARGUMENTS = (
    Field("id", ("string", "integer"), required=True, description="the pixel (dataset) id the events are sent to"),
    Field("params", "mapping",
          description="the call's other parameters, e.g. {test_event_code: TEST123} to see the events in Test Events"),
)


def _read(method: str, description: str) -> Operation:
    return Operation(service="*", method=method, kind=OperationKind.READ, description=description,
                     arguments=_ARGUMENTS)


def _write(service: str, method: str, description: str, *, actions: tuple[str, ...], idempotent: bool,
           max_chunk: int = 100, arguments: tuple[Field, ...] = (_PARAMS,), destructive: bool = False) -> Operation:
    return Operation(service=service, method=method, kind=OperationKind.APPLY, idempotent=idempotent,
                     max_chunk=max_chunk, actions=actions, destructive_actions=actions if destructive else (),
                     arguments=arguments, description=description)


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
        _write("*", "create_*", "Create objects on an edge of the parent `id` (any `create_<edge>`, e.g. "
               "AdAccount.create_campaign), a request per row (columns are the new object's parameters).",
               actions=("create",), idempotent=False, arguments=_CREATE_ARGUMENTS),
        _write("*", "api_update", "Update objects, a request per row (the `id` column names the object; the other "
               "columns are the parameters to change).", actions=("update",), idempotent=True),
        _write("*", "api_delete", "Delete objects, a request per row (the `id` column names the object).",
               actions=("remove",), idempotent=True, arguments=(), destructive=True),
        _write("AdsPixel", "create_event", "Send server events to the Conversions API, a chunk per request (columns "
               "are the event's fields: event_name, event_time, action_source, event_id, user_data, custom_data, ...).",
               actions=("upload",), idempotent=True, max_chunk=1000, arguments=_EVENT_ARGUMENTS),
    ),
    adapters={"source": "streamwright.connectors.meta_ads.engines.source:MetaAdsSourceAdapter"},
    validator="streamwright.connectors.meta_ads.client:validate_connection",
    # Graph API error codes worth retrying (throttling, temporary errors); a stream's `retry.codes` replace them
    retry_codes=(1, 2, 4, 17, 32, 341, 613, 80000, 80001, 80002, 80003, 80004, 80005, 80006, 80008, 80009, 80014),
    surface="streamwright.connectors.meta_ads.client:surface",
    loggers=("urllib3",)  # facebook_business sends its requests through requests/urllib3,
)
