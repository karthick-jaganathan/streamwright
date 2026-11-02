# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the microsoft_ads connector accepts: its connection fields and its read-only policy over the Microsoft
Advertising SOAP API (pure data). The operations are families (contract v1.3 §6b): the `Get*`, `Search*` and `Find*`
methods of any `*Service`, plus the two report operations. A call's `request` holds the SOAP request's fields (their
names are the WSDL's); `account_id` and `customer_id` name the account and customer the SOAP headers carry. The
installed bingads WSDL decides which services, methods and request fields exist; the connector never lists them.
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
    ),
    adapters={"source": "streamwright.connectors.microsoft_ads.engines.source:MicrosoftAdsSourceAdapter"},
    validator="streamwright.connectors.microsoft_ads.client:validate_connection",
    # SOAP error codes (InternalError, CallRateExceeded, ConcurrentRequestOverLimit) and OAuth errors worth
    # retrying; a stream's `retry.codes` replace them
    retry_codes=(0, 117, 207, "temporarily_unavailable", "server_error"),
    loggers=("suds.client", "suds.transport")  # bingads SOAP messages and HTTP,
)
