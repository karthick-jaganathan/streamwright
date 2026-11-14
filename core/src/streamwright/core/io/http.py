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
The `source` adapter of declarative HTTP connectors (`streamwright.core.sdk.operations.http`). They have no vendor
code: the source's `auth:` and `http:` blocks declare the API, and the source engine's HTTP reader runs its `http:`
requests with the authentication the connection declares. A connector's `sdk:` request would reach this adapter, which
says so.
"""

from streamwright.core.runtime.connector_types import ConnectorContext, ConnectorError
from streamwright.core.runtime.contract import ResolvedConnection

__all__ = ["HttpSourceAdapter"]


class HttpSourceAdapter:
    """Declarative HTTP connectors run `http:` requests in the engine; there is no session to open."""

    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> None:
        raise ConnectorError("%s is a REST API connector: use an http request (http: {path, method, ...}), "
                             "not sdk" % connection.connector)
