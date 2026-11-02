# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
The postgres connector on the `spark` engine (kind: SparkJob): JDBC reads and appends on the executors; merge and
delete through an UNLOGGED staging table and one driver transaction (psycopg); the job's state on the driver.
Credentials go in the JDBC properties only, never in Spark's configuration.
"""

import functools
from collections.abc import Callable, Mapping
from typing import Any

from pyspark.sql import DataFrame

from streamwright.connectors.postgres import client
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.spark.io import jdbc
from streamwright.spark.sdk import SparkContext, WriteReceipt

try:  # the extra `spark` installs it: the driver's merges, deletes and state
    import psycopg
except ImportError:
    psycopg = None

__all__ = ["PostgresSparkAdapter"]

CONNECT_TIMEOUT = 15
DIALECT = jdbc.JdbcDialect(
    name="postgres",
    url_template=client.JDBC_URL,
    driver=client.JDBC_DRIVER,
    default_port=client.DIALECT.default_port,
    staging_statement=client.staging_statement,
    merge_statement=client.merge_statement,
    delete_statement=client.delete_statement,
    identifier_limit=63,
)


def _target(connection: Mapping[str, Any]) -> jdbc.JdbcTarget:
    values = client.jdbc_values(connection)
    return jdbc.jdbc_target(DIALECT, host=values["host"], port=values["port"], database=values["database"],
                            user=values["user"], password=values["password"], properties=values["properties"])


def _connect(url: str) -> Any:
    if psycopg is None:
        raise RuntimeError("the Spark driver needs psycopg: pip install \"streamwright-postgres[spark]\"")
    return psycopg.connect(url, connect_timeout=CONNECT_TIMEOUT)


def _driver(connection: Mapping[str, Any]) -> Callable[[], Any]:
    """Opens the driver's own connection (psycopg), TLS required unless the connection says otherwise."""
    return functools.partial(_connect, client.driver_url(connection, default_sslmode=client.SPARK_DRIVER_SSLMODE))


class PostgresSparkAdapter:
    """JDBC reads and writes of the catalog's database operations."""

    jars = (client.JDBC_PACKAGE,)
    packages = ("psycopg",)
    template_tokens = jdbc.STAGING_TOKENS

    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> dict[str, str]:
        return {}

    def read(self, context: SparkContext, operation: Operation, arguments: Mapping[str, Any]) -> DataFrame:
        read = jdbc.read_query if operation.method == "query" else jdbc.read_table
        return read(context, arguments, dialect=DIALECT, target=_target(context.connection), operation=operation)

    def write(self, context: SparkContext, frame: DataFrame, operation: Operation,
              arguments: Mapping[str, Any]) -> WriteReceipt:
        return jdbc.write_table(context, frame, arguments, dialect=DIALECT, target=_target(context.connection),
                                connect=_driver(context.connection), operation=operation)

    def state_connection(self, connection: Mapping[str, Any]) -> tuple[Callable[[], Any], str]:
        return _driver(connection), "postgres"
