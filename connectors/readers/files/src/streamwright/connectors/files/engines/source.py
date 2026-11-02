# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The files connector on the `source` engine (streamwright run): local reads on a locked DuckDB connection."""

from streamwright.core.io import files
from streamwright.core.sdk.contract import ResolvedConnection
from streamwright.core.sdk.engines.source import ConnectorContext

__all__ = ["FilesSourceAdapter"]


class FilesSourceAdapter:
    """Opens a reader that can only read inside the connection's roots: external access off, no extension, no secret."""

    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> files.FileSession:
        return files.open_local_session(connection, context)
