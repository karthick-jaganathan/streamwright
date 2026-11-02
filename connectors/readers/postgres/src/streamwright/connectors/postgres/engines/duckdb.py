# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
The postgres connector on the `duckdb` engine (kind: DuckDBJob): the database attached through DuckDB's postgres
extension with its URL in a scoped secret, read-only unless an output or the job's state uses the connection. The
connection can hold the job's bookkeeping state (a `streamwright` schema), through psycopg (extra `state`).
"""

from collections.abc import Mapping
from typing import Any

from streamwright.connectors.postgres import client
from streamwright.core.io import databases
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.core.sdk.engines.duckdb import (
    AdapterError,
    DuckDBSession,
    ReadContext,
    StateStore,
    WriteContext,
    WriteReceipt,
    open_store,
)

try:  # the optional extra `state`: reads and writes work without it
    import psycopg
except ImportError:
    psycopg = None

__all__ = ["PostgresDuckDBAdapter"]

DEFAULT_SCHEMA = "public"
CONNECT_TIMEOUT = 15


class PostgresDuckDBAdapter:
    """Reads (`read: query` runs on the server, `read: table`) and writes tables; holds the job's state."""

    def configure(self, session: DuckDBSession, name: str, connection: ResolvedConnection) -> None:
        databases.configure_database(client.DIALECT, session, name, connection)

    def read_sql(self, context: ReadContext, operation: Operation, arguments: Mapping[str, Any]) -> str:
        return databases.read_sql(client.DIALECT, context, operation, arguments, default_schema=DEFAULT_SCHEMA)

    def write(self, context: WriteContext, operation: Operation, arguments: Mapping[str, Any]) -> WriteReceipt:
        return databases.write_table(context, default_schema=DEFAULT_SCHEMA)

    def open_state(self, session: DuckDBSession, name: str, connection: ResolvedConnection,
                   schema: str) -> StateStore:
        if psycopg is None:
            raise AdapterError("state in PostgreSQL needs psycopg: pip install \"streamwright-postgres[state]\"")
        url = client.driver_url(connection)
        return open_store(lambda: psycopg.connect(url, connect_timeout=CONNECT_TIMEOUT), "postgres", schema)
