# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the postgres connector knows about PostgreSQL, shared by every engine adapter: the dialect DuckDB attaches it with
(a `postgresql://` URL in a secret: DuckDB 2.0 parses only URLs there - with a keyword DSN the password would be
dropped - percent-encoded so any password works; unconstrained NUMERIC read as exact text; the server's own
statement_timeout), the rules a resolved connection must meet, and the JDBC facts and statements of Spark's writes.
No engine library is imported here.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Final
from urllib.parse import quote

from streamwright.core.io.database_connections import (
    DatabaseDialect,
    Parameters,
    connection_parameters,
    connection_problems,
)

__all__ = ["DIALECT", "JDBC_DRIVER", "JDBC_PACKAGE", "JDBC_URL", "SPARK_DRIVER_SSLMODE",
           "SSLMODES", "delete_statement", "driver_url", "jdbc_values", "merge_statement", "parameters", "postgres_url",
           "staging_statement", "validate_connection"]

SSLMODES: Final = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")
JDBC_URL: Final = "jdbc:postgresql://{host}:{port}/{database}"
JDBC_DRIVER: Final = "org.postgresql.Driver"
JDBC_PACKAGE: Final = ("org.postgresql:postgresql", "ship", "42.7.4")
SPARK_DRIVER_SSLMODE: Final = "require"  # the Spark driver's own connection, when the connection names none


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


def jdbc_values(connection: Mapping[str, Any]) -> dict[str, Any]:
    """host, port, database, user, password and the JDBC properties (sslmode, then the connection's vendor keys)."""
    values = dict(parameters(connection))
    properties = {"sslmode": str(values["sslmode"])} if values.get("sslmode") else {}
    # undeclared connection keys are JDBC driver properties (vendor arguments), e.g. ApplicationName, loginTimeout
    properties.update({str(key): str(value) for key, value in (getattr(connection, "vendor", {}) or {}).items()})
    return {"host": values.get("host"), "port": values.get("port"), "database": values.get("database"),
            "user": values.get("user"), "password": values.get("password"), "properties": properties}


def driver_url(connection: Mapping[str, Any], *, default_sslmode: str | None = None) -> str:
    """The postgresql:// URL a driver-side client (psycopg) connects with; credentials included."""
    pairs = parameters(connection)
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
