# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

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
