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

"""What the openai_ads connector accepts: the authentication of the OpenAI Ads API and the HTTP request (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition
from streamwright.core.sdk.operations import http

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="openai_ads",
    title="OpenAI Ads",
    family="advertising",
    summary="OpenAI Advertising API performance and metadata: HTTP reads and writes (`operation:` requests)",
    connection=http.auth_fields(("bearer",)),
    operations=(http.REQUEST, http.SEND),
    adapters={"source": "streamwright.core.sdk.engines.source:HttpSourceAdapter"},
    loggers=("streamwright.network", "urllib3"),
)
