# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""What the amazon_ads connector accepts: the authentication of the Advertising API and the HTTP request (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition
from streamwright.core.sdk.operations import http

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="amazon_ads",
    title="Amazon Ads",
    family="advertising",
    summary="Amazon Advertising API profiles and Sponsored Products, declared in the source's `http:` block",
    connection=http.auth_fields(("oauth2_refresh_token", "bearer"), secrets=("client_id",)),
    operations=(http.REQUEST,),
    adapters={"source": "streamwright.core.io.http:HttpSourceAdapter"},
)
