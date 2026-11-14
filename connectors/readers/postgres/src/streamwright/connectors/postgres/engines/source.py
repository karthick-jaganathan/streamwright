#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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

"""The postgres connector on the `source` engine (streamwright run): read-only SELECTs through DuckDB's extension."""

from streamwright.connectors.postgres import client
from streamwright.core.io import databases
from streamwright.core.sdk.contract import ResolvedConnection
from streamwright.core.sdk.engines.source import ConnectorContext

__all__ = ["PostgresSourceAdapter"]


class PostgresSourceAdapter:
    """Attaches the database READ_ONLY on a locked DuckDB connection of its own; the URL lives in a secret."""

    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> databases.DatabaseSession:
        return databases.open_database_session(client.DIALECT, connection, context)
