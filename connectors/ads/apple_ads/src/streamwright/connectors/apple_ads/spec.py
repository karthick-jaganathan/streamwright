# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""What the apple_ads connector accepts: the authentication of the Apple Ads API and the HTTP request (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition
from streamwright.core.sdk.operations import http

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="apple_ads",
    title="Apple Ads",
    family="advertising",
    summary="Apple Ads campaigns, ad groups, keywords and reports, declared in the source's `http:` block",
    connection=http.auth_fields(("bearer",)),
    operations=(http.REQUEST,),
    adapters={"source": "streamwright.core.io.http:HttpSourceAdapter"},
)
