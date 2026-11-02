# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""What the restapi connector accepts: the built-in authentication types and the HTTP request (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition
from streamwright.core.sdk.operations import http

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="restapi",
    title="REST API",
    family="http",
    summary="Declarative HTTP extraction for any REST API: the source's `auth:` and `http:` blocks declare it",
    connection=http.auth_fields(),
    operations=(http.REQUEST,),
    adapters={"source": "streamwright.core.io.http:HttpSourceAdapter"},
    loggers=("streamwright.network", "urllib3"),
)
