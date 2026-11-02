# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""What the mysql connector accepts: its connection fields and the catalog's database operations (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition, EngineLimit, Field
from streamwright.core.sdk.operations import databases

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="mysql",
    title="MySQL",
    family="databases",
    summary="read-only SELECT queries and table reads, and table writes, on MySQL",
    connection=(
        Field("dsn", "string", secret=True, description="a mysql:// URL or key=value pairs (instead of the keys)"),
        Field("host", "string", description="a host name or an IP address"),
        Field("port", "integer", description="default 3306"),
        Field("database", "string", description="the database (also the schema of a table named without one)"),
        Field("user", "string", description="the user"),
        Field("password", "string", secret=True, description="the user's password"),
        Field("sslmode", "string", choices=("disabled", "preferred", "required", "verify_ca", "verify_identity"),
              description="the TLS mode"),
        Field("options", "mapping", description="more connection parameters: socket and ssl_* files"),
        Field("statement_timeout", ("string", "number"),
              description="interrupts a source statement running longer: seconds, or e.g. 500ms, 30s, 5min"),
    ),
    operations=(databases.QUERY, databases.TABLE, databases.WRITE),
    adapters={
        "source": "streamwright.connectors.mysql.engines.source:MySQLSourceAdapter",
        "duckdb": "streamwright.connectors.mysql.engines.duckdb:MySQLDuckDBAdapter",
        "spark": "streamwright.connectors.mysql.engines.spark:MySQLSparkAdapter",
    },
    validator="streamwright.connectors.mysql.client:validate_connection",
    # DuckDB's mysql extension has no MERGE / ON CONFLICT: merges run on the Spark engine
    engine_limits=(EngineLimit(engine="duckdb", operation=databases.WRITE.name,
                               choices={"mode": ("append", "overwrite")}),),
)
