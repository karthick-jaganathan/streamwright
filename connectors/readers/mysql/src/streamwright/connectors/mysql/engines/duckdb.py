# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
The mysql connector on the `duckdb` engine (kind: DuckDBJob): the database attached through DuckDB's mysql extension
with its parameters in a scoped secret, read-only unless an output uses the connection. DuckDB shows each MySQL
database as a schema: a table named without one is in the connection's database.
"""

from collections.abc import Mapping
from typing import Any

from streamwright.connectors.mysql import client
from streamwright.core.io import databases
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.core.sdk.engines.duckdb import DuckDBSession, ReadContext, WriteContext, WriteReceipt

__all__ = ["MySQLDuckDBAdapter"]


def _database(connection: Mapping[str, Any]) -> str:
    return dict(client.parameters(connection)).get("database")


class MySQLDuckDBAdapter:
    """Reads (`read: query` runs on the server, `read: table`) and writes tables."""

    def configure(self, session: DuckDBSession, name: str, connection: ResolvedConnection) -> None:
        databases.configure_database(client.DIALECT, session, name, connection)

    def read_sql(self, context: ReadContext, operation: Operation, arguments: Mapping[str, Any]) -> str:
        return databases.read_sql(client.DIALECT, context, operation, arguments,
                                  default_schema=_database(context.connection))

    def write(self, context: WriteContext, operation: Operation, arguments: Mapping[str, Any]) -> WriteReceipt:
        return databases.write_table(context, default_schema=_database(context.connection))
