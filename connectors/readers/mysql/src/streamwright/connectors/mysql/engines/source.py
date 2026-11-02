# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The mysql connector on the `source` engine (streamwright run): read-only SELECTs through DuckDB's extension."""

from streamwright.connectors.mysql import client
from streamwright.core.io import databases
from streamwright.core.sdk.contract import ResolvedConnection
from streamwright.core.sdk.engines.source import ConnectorContext

__all__ = ["MySQLSourceAdapter"]


class MySQLSourceAdapter:
    """Attaches the database READ_ONLY on a locked DuckDB connection of its own; credentials live in a secret."""

    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> databases.DatabaseSession:
        return databases.open_database_session(client.DIALECT, connection, context)
