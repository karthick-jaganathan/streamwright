# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The s3 connector on the `source` engine (streamwright run): object reads through DuckDB's httpfs extension."""

from streamwright.connectors.s3 import client
from streamwright.core.io import files
from streamwright.core.sdk.contract import ResolvedConnection
from streamwright.core.sdk.engines.source import ConnectorContext

__all__ = ["S3SourceAdapter"]


class S3SourceAdapter:
    """Opens a reader confined to the connection's roots; its credentials live in a DuckDB secret scoped to them."""

    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> files.FileSession:
        return files.open_file_session(client.STORE, connection, context)
