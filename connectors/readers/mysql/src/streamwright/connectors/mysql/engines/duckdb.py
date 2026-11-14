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

try:  # optional extra `state`: reads and writes work without it
    import pymysql
except ImportError:
    pymysql = None

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
        if pymysql is None:
            raise AdapterError("state in MySQL needs PyMySQL: pip install \"streamwright-mysql[state]\"")
        values = dict(client.parameters(connection))
        ssl_keys = {"ssl_ca": "ca", "ssl_capath": "capath", "ssl_cert": "cert", "ssl_cipher": "cipher",
                    "ssl_crl": "crl", "ssl_key": "key"}
        ssl = {target: values[key] for key, target in ssl_keys.items() if values.get(key)}

        def connect():
            arguments = {"host": values.get("host"), "port": int(values.get("port") or 3306),
                         "database": values.get("database"), "user": values.get("user"),
                         "password": values.get("password"), "connect_timeout": 15,
                         "charset": "utf8mb4", "autocommit": False}
            if values.get("socket"):
                arguments["unix_socket"] = values["socket"]
            sslmode = str(values.get("sslmode") or "disabled").lower()
            if sslmode == "disabled":
                arguments["ssl_disabled"] = True
            else:
                arguments["ssl"] = ssl
                if sslmode in ("verify_ca", "verify_identity"):
                    arguments["ssl_verify_cert"] = True
                if sslmode == "verify_identity":
                    arguments["ssl_verify_identity"] = True
            return _PyMySQLConnection(pymysql.connect(**arguments))

        return open_store(connect, "mysql", schema)


class _PyMySQLConnection:
    """Expose the small execute/transaction DB-API surface used by SqlStateStore."""

    def __init__(self, connection):
        self._connection = connection

    def execute(self, statement, parameters=()):
        cursor = self._connection.cursor()
        cursor.execute(statement, parameters)
        return cursor

    def commit(self):
        return self._connection.commit()

    def rollback(self):
        return self._connection.rollback()

    def close(self):
        return self._connection.close()
