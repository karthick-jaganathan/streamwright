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
What the postgres connector knows about PostgreSQL, shared by every engine adapter: the dialect DuckDB attaches it with
(a `postgresql://` URL in a secret: DuckDB 2.0 parses only URLs there - with a keyword DSN the password would be
dropped - percent-encoded so any password works; unconstrained NUMERIC read as exact text; the server's own
statement_timeout), the rules a resolved connection must meet, and the JDBC facts and statements of Spark's writes.
No engine library is imported here.
"""

from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any, Final
from urllib.parse import quote

from streamwright.core.io.database_connections import (
    DatabaseDialect,
    Parameters,
    connection_parameters,
    connection_problems,
)
from streamwright.core.io.queries import timeout_ms

__all__ = ["DIALECT", "JDBC_DRIVER", "JDBC_PACKAGE", "JDBC_PROPERTIES", "JDBC_URL", "SPARK_DRIVER_SSLMODE",
           "SSLMODES", "delete_statement", "driver_url", "jdbc_values", "merge_statement", "parameters", "postgres_url",
           "server_parameters", "staging_statement", "validate_connection"]

SSLMODES: Final = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")
JDBC_URL: Final = "jdbc:postgresql://{host}:{port}/{database}"
JDBC_DRIVER: Final = "org.postgresql.Driver"
JDBC_PACKAGE: Final = ("org.postgresql:postgresql", "ship", "42.7.4")
SPARK_DRIVER_SSLMODE: Final = "require"  # the Spark driver's own connection, when the connection names none
# libpq parameter -> the pgjdbc property of another name (the others - sslmode, sslrootcert, options, ... - match)
JDBC_PROPERTIES: Final = MappingProxyType({
    "connect_timeout": "connectTimeout", "application_name": "ApplicationName", "gssencmode": "gssEncMode"})


def postgres_url(parameters: Parameters) -> str:
    """
    The `postgresql://` URL of (keyword, value) connection parameters (`database` is libpq's `dbname`): every one in
    the query, percent-encoded - a unix socket directory as `host`, a name with spaces, an international name or any
    password stays one value and can add no parameter. Credentials included: only ever put it in a secret.
    """
    pairs = ["%s=%s" % (quote("dbname" if key == "database" else str(key), safe=""), quote(str(value), safe=""))
             for key, value in parameters if value not in (None, "")]
    return "postgresql:///?" + "&".join(pairs)


def _secret_options(parameters: Parameters) -> Parameters:
    return [("URI", postgres_url(parameters))]


def _server_timeout(parameters: Parameters, milliseconds: int) -> Parameters:
    """The parameters with the server's own statement_timeout, set through libpq's `options` after any given."""
    setting = "-c statement_timeout=%d" % milliseconds
    given = [str(value).strip() for key, value in parameters if key == "options"]
    others = [(key, value) for key, value in parameters if key != "options"]
    return [*others, ("options", "%s %s" % (given[0], setting) if given and given[0] else setting)]


DIALECT: Final = DatabaseDialect(
    name="postgres",
    extension="postgres",
    url_schemes=("postgres", "postgresql"),
    default_port=5432,
    secret_options=_secret_options,
    identifier_limit=63,
    settings=(("pg_numeric_as_varchar", "true"),),
    server_timeout=_server_timeout,
)


def validate_connection(connection: Mapping[str, Any]) -> list[str]:
    """Why a resolved connection cannot be used: `dsn` or host, database and user; their values; the timeout."""
    return connection_problems(DIALECT, connection)


def parameters(connection: Mapping[str, Any]) -> Parameters:
    """The (keyword, value) connection parameters of a resolved connection (its DSN parsed)."""
    return connection_parameters(DIALECT, connection)


def server_parameters(connection: Mapping[str, Any]) -> Parameters:
    """The connection parameters with the server's own `statement_timeout`, when the connection sets one."""
    pairs = parameters(connection)
    if connection.get("statement_timeout") is None:
        return pairs
    return _server_timeout(pairs, timeout_ms(connection["statement_timeout"]))


def jdbc_values(connection: Mapping[str, Any]) -> dict[str, Any]:
    """
    host, port, database, user, password and the JDBC properties: the other libpq parameters (sslmode, `options` and
    a DSN's, the statement_timeout) under their pgjdbc names, then the connection's vendor keys verbatim.
    """
    vendor = dict(getattr(connection, "vendor", {}) or {})
    pairs = server_parameters(connection)
    values = dict(pairs)
    properties = {JDBC_PROPERTIES.get(key, key): str(value) for key, value in pairs
                  if key not in ("host", "port", "database", "user", "password") and key not in vendor
                  and value not in (None, "")}
    # undeclared connection keys are JDBC driver properties (vendor arguments), e.g. ApplicationName, loginTimeout
    properties.update({str(key): str(value) for key, value in vendor.items()})
    return {"host": values.get("host"), "port": values.get("port"), "database": values.get("database"),
            "user": values.get("user"), "password": values.get("password"), "properties": properties}


def driver_url(connection: Mapping[str, Any], *, default_sslmode: str | None = None) -> str:
    """The postgresql:// URL a driver-side client (psycopg) connects with (its statement_timeout too); credentials."""
    pairs = server_parameters(connection)
    if default_sslmode and not any(key == "sslmode" for key, _ in pairs):
        pairs.append(("sslmode", default_sslmode))
    return postgres_url(pairs)


def staging_statement(target: str, staging: str) -> str:
    """An UNLOGGED staging table like the target (no WAL: a bulk load that is dropped in the same batch)."""
    return "CREATE UNLOGGED TABLE %s (LIKE %s INCLUDING DEFAULTS EXCLUDING CONSTRAINTS EXCLUDING INDEXES)" % (
        staging, target)


def merge_statement(target: str, staging: str, columns: Sequence[str], key: Sequence[str],
                    update: Sequence[str]) -> str:
    names = ", ".join(columns)
    sets = ", ".join("%s = EXCLUDED.%s" % (column, column) for column in update)
    action = "DO UPDATE SET " + sets if sets else "DO NOTHING"
    return "INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) %s" % (
        target, names, names, staging, ", ".join(key), action)


def delete_statement(target: str, staging: str, key: Sequence[str]) -> str:
    matches = " AND ".join("%s.%s = %s.%s" % (target, column, staging, column) for column in key)
    return "DELETE FROM %s USING %s WHERE %s" % (target, staging, matches)
