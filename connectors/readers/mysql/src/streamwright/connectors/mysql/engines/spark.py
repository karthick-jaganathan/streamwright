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
The mysql connector on the `spark` engine (kind: SparkJob): JDBC reads and appends on the executors; merge and delete
through a staging table and one driver transaction (pymysql). Credentials go in the JDBC properties only, never in
Spark's configuration.
"""

import functools
from collections.abc import Callable, Mapping
from typing import Any

from pyspark.sql import DataFrame

from streamwright.connectors.mysql import client
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.spark.io import jdbc
from streamwright.spark.sdk import SparkContext, WriteReceipt

try:  # the extra `spark` installs it: the driver's merges and deletes
    import pymysql
except ImportError:
    pymysql = None

__all__ = ["MySQLSparkAdapter"]

CONNECT_TIMEOUT = 15
DIALECT = jdbc.JdbcDialect(
    name="mysql",
    url_template=client.JDBC_URL,
    driver=client.JDBC_DRIVER,
    default_port=client.DIALECT.default_port,
    staging_statement=client.staging_statement,
    merge_statement=client.merge_statement,
    delete_statement=client.delete_statement,
    quote="`",
    default_schema=None,             # a table named without a schema is in the connection's database
    identifier_limit=64,
    literal=client.literal,
    commit_before_cleanup=True,      # DROP and TRUNCATE commit implicitly in MySQL
)


def _target(connection: Mapping[str, Any]) -> jdbc.JdbcTarget:
    values = client.jdbc_values(connection)
    return jdbc.jdbc_target(DIALECT, host=values["host"], port=values["port"], database=values["database"],
                            user=values["user"], password=values["password"], properties=values["properties"])


def _connect(values: Mapping[str, Any]) -> Any:
    if pymysql is None:
        raise RuntimeError("the Spark driver needs pymysql: pip install \"streamwright-mysql[spark]\"")
    return pymysql.connect(host=values["host"], port=int(values["port"] or client.DIALECT.default_port),
                           database=values["database"], user=values["user"], password=values["password"],
                           connect_timeout=CONNECT_TIMEOUT, autocommit=False)


class MySQLSparkAdapter:
    """JDBC reads and writes of the catalog's database operations."""

    jars = (client.JDBC_PACKAGE,)
    packages = ("pymysql",)
    template_tokens = jdbc.STAGING_TOKENS

    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> dict[str, str]:
        return {}

    def read(self, context: SparkContext, operation: Operation, arguments: Mapping[str, Any]) -> DataFrame:
        read = jdbc.read_query if operation.method == "query" else jdbc.read_table
        return read(context, arguments, dialect=DIALECT, target=_target(context.connection), operation=operation)

    def write(self, context: SparkContext, frame: DataFrame, operation: Operation,
              arguments: Mapping[str, Any]) -> WriteReceipt:
        connect: Callable[[], Any] = functools.partial(_connect, client.jdbc_values(context.connection))
        return jdbc.write_table(context, frame, arguments, dialect=DIALECT, target=_target(context.connection),
                                connect=connect, operation=operation)

    def state_connection(self, connection: Mapping[str, Any]) -> tuple[Callable[[], Any], str]:
        """The driver's DB-API connection used by SparkJob's shared bookkeeping store."""
        if pymysql is None:
            raise RuntimeError("SparkJob MySQL state needs PyMySQL: pip install \"streamwright-mysql[spark]\"")
        values = dict(client.parameters(connection))
        ssl_names = {"ssl_ca": "ca", "ssl_capath": "capath", "ssl_cert": "cert", "ssl_cipher": "cipher",
                     "ssl_crl": "crl", "ssl_key": "key"}
        ssl = {target: values[key] for key, target in ssl_names.items() if values.get(key)}

        def connect_state():
            kwargs = {"host": values.get("host"), "port": int(values.get("port") or client.DIALECT.default_port),
                      "database": values.get("database"), "user": values.get("user"),
                      "password": values.get("password"), "connect_timeout": CONNECT_TIMEOUT,
                      "charset": "utf8mb4", "autocommit": False}
            if values.get("socket"):
                kwargs["unix_socket"] = values["socket"]
            sslmode = str(values.get("sslmode") or "disabled").lower()
            if sslmode == "disabled":
                kwargs["ssl_disabled"] = True
            else:
                kwargs["ssl"] = ssl
                if sslmode in ("verify_ca", "verify_identity"):
                    kwargs["ssl_verify_cert"] = True
                if sslmode == "verify_identity":
                    kwargs["ssl_verify_identity"] = True
            return _PyMySQLStateConnection(pymysql.connect(**kwargs))

        return connect_state, "mysql"


class _PyMySQLStateConnection:
    """Adapt PyMySQL's cursor-only API to the state store's execute surface."""

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
