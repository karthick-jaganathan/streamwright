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

"""
The mysql connector on the `duckdb` engine (kind: DuckDBJob): the database attached through DuckDB's mysql extension
with its parameters in a scoped secret, read-only unless an output uses the connection. DuckDB shows each MySQL
database as a schema: a table named without one is in the connection's database.
"""

from collections.abc import Mapping
from typing import Any

from streamwright.connectors.mysql import client
from streamwright.connectors.mysql.engines import driver
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

    def open_state(self, session: DuckDBSession, name: str, connection: ResolvedConnection,
                   schema: str) -> StateStore:
        if driver.pymysql is None:
            raise AdapterError("state in MySQL needs PyMySQL: pip install \"streamwright-mysql[state]\"")
        return open_store(lambda: driver.StateConnection(driver.connect(connection, "state")), "mysql", schema)
