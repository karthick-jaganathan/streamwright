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
What the microsoft_ads connector accepts: its connection fields and its operations over the Microsoft Advertising
SOAP API (pure data). Reads are families (contract v1.3 §6b): the `Get*`, `Search*` and `Find*` methods of any
`*Service`, plus the two report operations. Apply operations (apply outputs): the `Add*` (create; not idempotent),
`Update*` and `Delete*` (update, remove) methods of any `*Service`, a row per item of the request's one list, and the
conversion uploads `ApplyOfflineConversions`, `ApplyOfflineConversionAdjustments` and
`ApplyOnlineConversionAdjustments` (Microsoft rejects duplicates, so idempotent). A call's `request` holds the SOAP
request's fields (their names are the WSDL's); `account_id` and `customer_id` name the account and customer the SOAP
headers carry. The installed bingads WSDL decides which services, methods and request fields exist; the connector
never lists them.
"""

from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, OperationKind

__all__ = ["CONNECTOR"]

_ARGUMENTS = (
    Field("request", "mapping", description="the fields of the SOAP request; `@type` names the type of an object"),
    Field("account_id", ("string", "integer"), description="the account of the call (default: the request's own, "
                                                           "else the connection's)"),
    Field("customer_id", ("string", "integer"), description="the customer of the call (default: the connection's)"),
)


def _read(service: str, method: str, description: str) -> Operation:
    return Operation(service=service, method=method, kind=OperationKind.READ, arguments=_ARGUMENTS,
                     description=description)


_APPLY_ARGUMENTS = (
    Field("request", "mapping", description="the SOAP request's other fields (e.g. AccountId, CampaignId); the rows "
                                            "fill its one list, an item each"),
    *_ARGUMENTS[1:],
)


def _apply(method: str, action: str, description: str, *, idempotent: bool = True, max_chunk: int = 100,
           service: str = "*Service") -> Operation:
    return Operation(service=service, method=method, kind=OperationKind.APPLY, idempotent=idempotent,
                     max_chunk=max_chunk, actions=(action,),
                     destructive_actions=(action,) if action == "remove" else (), arguments=_APPLY_ARGUMENTS,
                     description=description)


CONNECTOR = ConnectorDefinition(
    name="microsoft_ads",
    title="Microsoft Advertising",
    family="advertising",
    summary="Microsoft Advertising campaigns, reporting and customer data through the SOAP API",
    connection=(
        Field("developer_token", "string", required=True, secret=True, description="the developer token"),
        Field("client_id", "string", required=True, secret=True, description="the application (client) id"),
        Field("refresh_token", "string", required=True, secret=True, description="the OAuth refresh token"),
        Field("client_secret", "string", secret=True,
              description="for web apps; leave out for desktop and mobile apps"),
        Field("tenant", "string", description="the Microsoft Entra tenant (default: common)"),
        Field("customer_id", ("string", "integer"), description="the customer the calls are made for"),
        Field("account_id", ("string", "integer"), description="the account the calls are made for"),
        Field("environment", "string", default="production", choices=("production", "sandbox"),
              description="the API environment"),
    ),
    operations=(
        _read("*Service", "Get*", "A read of any service (`Get<Entity>By...`)."),
        _read("*Service", "Search*", "A search of any service (`Search<Entity>`)."),
        _read("*Service", "Find*", "A lookup of any service (`Find<Entity>`)."),
        _read("ReportingService", "SubmitGenerateReport", "Submit a report request."),
        _read("ReportingService", "PollGenerateReport", "Poll a submitted report request."),
        _apply("Add*", "create", "Create items of any service (`Add<Entity>s`), a row each (columns are the item's "
                                 "fields; the response's ids are the rows' results).", idempotent=False),
        _apply("Update*", "update", "Update items of any service (`Update<Entity>s`), a row each (with its `Id`)."),
        _apply("Delete*", "remove", "Remove items of any service (`Delete<Entity>s`), a row each (e.g. one id "
                                    "column for a list of ids)."),
        # ApplyOfflineConversions, ApplyOfflineConversionAdjustments, ApplyOnlineConversionAdjustments: Microsoft
        # rejects a duplicate, so a call cut short is sent again
        _apply("Apply*Conversion*", "upload", "Upload offline conversions or conversion adjustments, a row each "
                                              "(e.g. ApplyOfflineConversions: MicrosoftClickId, ConversionName, "
                                              "ConversionTime, ...).", max_chunk=1000,
               service="CampaignManagementService"),
    ),
    adapters={"source": "streamwright.connectors.microsoft_ads.engines.source:MicrosoftAdsSourceAdapter"},
    describe_type="streamwright.connectors.microsoft_ads.type_info:describe_type",
    validator="streamwright.connectors.microsoft_ads.client:validate_connection",
    # SOAP error codes (InternalError, CallRateExceeded, ConcurrentRequestOverLimit) and OAuth errors worth
    # retrying; a stream's `retry.codes` replace them
    retry_codes=(0, 117, 207, "temporarily_unavailable", "server_error"),
    loggers=("suds.client", "suds.transport")  # bingads SOAP messages and HTTP,
)
