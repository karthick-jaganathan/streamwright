# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The azure_blob connector on the `source` engine (streamwright run): blob reads through DuckDB's azure extension."""

from streamwright.connectors.azure_blob import client
from streamwright.core.io import files
from streamwright.core.sdk.contract import ResolvedConnection
from streamwright.core.sdk.engines.source import ConnectorContext

__all__ = ["AzureBlobSourceAdapter"]


class AzureBlobSourceAdapter:
    """Opens a reader confined to the connection's roots; its credentials live in a DuckDB secret scoped to them."""

    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> files.FileSession:
        return files.open_file_session(client.STORE, connection, context)
