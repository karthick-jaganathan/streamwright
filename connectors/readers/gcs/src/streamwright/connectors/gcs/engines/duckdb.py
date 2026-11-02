# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The gcs connector on the `duckdb` engine (kind: DuckDBJob): httpfs with one secret per root, scoped to it."""

from collections.abc import Mapping
from typing import Any

from streamwright.connectors.gcs import client
from streamwright.core.io import files
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.core.sdk.engines.duckdb import DuckDBSession, ReadContext, WriteContext, WriteReceipt

__all__ = ["GcsDuckDBAdapter"]


def _build_location(connection: Mapping[str, Any]) -> files.FileLocation:
    return files.FileLocation(client.STORE, tuple(connection["roots"]))


class GcsDuckDBAdapter:
    """Reads and writes objects inside the connection's roots; two connections never share credentials."""

    def configure(self, session: DuckDBSession, name: str, connection: ResolvedConnection) -> None:
        files.configure_store_secrets(client.STORE, session, name, connection)

    def read_sql(self, context: ReadContext, operation: Operation, arguments: Mapping[str, Any]) -> str:
        return files.read_sql(context, arguments, locate=_build_location(context.connection).resolve_path)

    def write(self, context: WriteContext, operation: Operation, arguments: Mapping[str, Any]) -> WriteReceipt:
        return files.write_files(context, arguments, locate=_build_location(context.connection).resolve_path)
