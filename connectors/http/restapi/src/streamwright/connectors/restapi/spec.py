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

"""What the restapi connector accepts: the built-in authentication types and the HTTP request (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition
from streamwright.core.sdk.operations import http

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="restapi",
    title="REST API",
    family="http",
    summary="Declarative HTTP reads and writes for any REST API: each request's `operation:` (source `http:` defaults)",
    connection=http.auth_fields(),
    operations=(http.REQUEST, http.SEND),
    adapters={"source": "streamwright.core.sdk.engines.source:HttpSourceAdapter"},
    loggers=("streamwright.network", "urllib3"),
)
