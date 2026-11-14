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

"""What the postgres connector accepts: its connection fields and the catalog's database operations (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition, Field
from streamwright.core.sdk.operations import databases

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="postgres",
    title="PostgreSQL",
    family="databases",
    summary="read-only SELECT queries and table reads, and table writes, on PostgreSQL",
    connection=(
        Field("dsn", "string", secret=True,
              description="a postgresql:// URL or libpq key=value pairs (instead of the connection keys)"),
        Field("host", "string", description="a host name, an IP address or a unix socket directory"),
        Field("port", "integer", description="default 5432"),
        Field("database", "string", description="the database"),
        Field("user", "string", description="the role"),
        Field("password", "string", secret=True, description="the role's password"),
        Field("sslmode", "string", choices=("disable", "allow", "prefer", "require", "verify-ca", "verify-full"),
              description="libpq's sslmode"),
        Field("options", "mapping", description="more libpq connection parameters, e.g. {connect_timeout: \"10\"}"),
        Field("statement_timeout", ("string", "number"),
              description="the server's statement_timeout for every statement: seconds, or e.g. 500ms, 30s, 5min"),
    ),
    operations=(databases.QUERY, databases.TABLE, databases.WRITE),
    adapters={
        "source": "streamwright.connectors.postgres.engines.source:PostgresSourceAdapter",
        "duckdb": "streamwright.connectors.postgres.engines.duckdb:PostgresDuckDBAdapter",
        "spark": "streamwright.connectors.postgres.engines.spark:PostgresSparkAdapter",
    },
    validator="streamwright.connectors.postgres.client:validate_connection",
    duckdb_extensions=("postgres",),
)
