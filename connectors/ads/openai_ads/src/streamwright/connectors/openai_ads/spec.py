# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""What the openai_ads connector accepts: the authentication of the OpenAI Ads API and the HTTP request (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition
from streamwright.core.sdk.operations import http

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="openai_ads",
    title="OpenAI Ads",
    family="advertising",
    summary="OpenAI Advertising API performance and metadata, declared in the source's `http:` block",
    connection=http.auth_fields(("bearer",)),
    operations=(http.REQUEST,),
    adapters={"source": "streamwright.core.io.http:HttpSourceAdapter"},
    loggers=("streamwright.network", "urllib3"),
)
