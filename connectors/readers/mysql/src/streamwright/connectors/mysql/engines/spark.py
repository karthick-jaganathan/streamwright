# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

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
