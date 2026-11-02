# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""What the linkedin_ads connector accepts: the authentication of the LinkedIn API and the HTTP request (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition
from streamwright.core.sdk.operations import http

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="linkedin_ads",
    title="LinkedIn Ads",
    family="advertising",
    summary="LinkedIn Marketing campaigns, creatives and analytics, declared in the source's `http:` block",
    connection=http.auth_fields(("bearer",)),
    operations=(http.REQUEST,),
    adapters={"source": "streamwright.core.io.http:HttpSourceAdapter"},
)
