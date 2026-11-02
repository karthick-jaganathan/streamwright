# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The gcs connector on the `source` engine (streamwright run): object reads through DuckDB's httpfs extension."""

from streamwright.connectors.gcs import client
from streamwright.core.io import files
from streamwright.core.sdk.contract import ResolvedConnection
from streamwright.core.sdk.engines.source import ConnectorContext

__all__ = ["GcsSourceAdapter"]


class GcsSourceAdapter:
    """Opens a reader confined to the connection's roots; its HMAC key lives in a DuckDB secret scoped to them."""

    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> files.FileSession:
        return files.open_file_session(client.STORE, connection, context)
