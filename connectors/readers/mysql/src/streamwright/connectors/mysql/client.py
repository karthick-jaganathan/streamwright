# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the mysql connector knows about MySQL, shared by every engine adapter: the dialect DuckDB attaches it with (the
connection parameters as the parameters of a `TYPE mysql` secret - the password a secret value, never in the ATTACH
text; a parameter the secret cannot take is refused by name), MySQL's system schemas, the rules a resolved connection
must meet, and the JDBC facts and statements of Spark's writes. No engine library is imported here.
"""

from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any, Final

from streamwright.core.io.database_connections import (
    DatabaseDialect,
    Parameters,
    connection_parameters,
    connection_problems,
)
from streamwright.core.io.queries import QueryPolicy
from streamwright.core.io.sandbox import ConnectError

__all__ = ["DIALECT", "JDBC_DRIVER", "JDBC_PACKAGE", "JDBC_URL", "SECRET_PARAMETERS", "delete_statement",
           "jdbc_values", "literal", "merge_statement", "parameters", "secret_options", "staging_statement",
           "validate_connection"]

# connection parameter -> DuckDB mysql secret parameter
SECRET_PARAMETERS: Final = MappingProxyType({
    "host": "HOST", "port": "PORT", "database": "DATABASE", "user": "USER", "password": "PASSWORD",
    "socket": "SOCKET", "sslmode": "SSL_MODE", "ssl_ca": "SSL_CA", "ssl_capath": "SSL_CAPATH", "ssl_cert": "SSL_CERT",
    "ssl_cipher": "SSL_CIPHER", "ssl_crl": "SSL_CRL", "ssl_crlpath": "SSL_CRLPATH", "ssl_key": "SSL_KEY"})
JDBC_URL: Final = "jdbc:mysql://{host}:{port}/{database}"
JDBC_DRIVER: Final = "com.mysql.cj.jdbc.Driver"
JDBC_PACKAGE: Final = ("com.mysql:mysql-connector-j", "ship", "8.4.0")


def secret_options(parameters: Parameters) -> Parameters:
    """
    [(secret parameter, value)] of (keyword, value) connection parameters, in the secret's order; ConnectError naming
    any parameter DuckDB's mysql secret does not take (never its value) or a port that is no number.
    """
    given = {}
    for key, value in parameters:
        if key not in SECRET_PARAMETERS:
            raise ConnectError("the connection has a parameter that DuckDB's mysql secret does not take: %s" % key)
        if value not in (None, ""):
            given[key] = value
    if "port" in given:
        if not str(given["port"]).isdigit():
            raise ConnectError("the connection's port is not a number")
        given["port"] = int(given["port"])
    return [(parameter, given[key]) for key, parameter in SECRET_PARAMETERS.items() if key in given]


DIALECT: Final = DatabaseDialect(
    name="mysql",
    extension="mysql",
    url_schemes=("mysql",),
    default_port=3306,
    secret_options=secret_options,
    key_aliases=MappingProxyType({"dbname": "database", "ssl_mode": "sslmode"}),
    connection_options=("socket", "ssl_ca", "ssl_capath", "ssl_cert", "ssl_cipher", "ssl_crl", "ssl_crlpath",
                        "ssl_key"),
    identifier_limit=64,
    policy=QueryPolicy(system_schemas=(*QueryPolicy.system_schemas, "mysql", "performance_schema", "sys")),
)


def validate_connection(connection: Mapping[str, Any]) -> list[str]:
    """Why a resolved connection cannot be used: `dsn` or host, database and user; their values; the timeout."""
    return connection_problems(DIALECT, connection)


def parameters(connection: Mapping[str, Any]) -> Parameters:
    """The (keyword, value) connection parameters of a resolved connection (its DSN parsed)."""
    return connection_parameters(DIALECT, connection)


def literal(value: str) -> str:
    """A MySQL string literal: backslashes are escapes in MySQL, so they are doubled as the quotes are."""
    return "'%s'" % str(value).replace("\\", "\\\\").replace("'", "''")


def jdbc_values(connection: Mapping[str, Any]) -> dict[str, Any]:
    """host, port, database, user, password and the JDBC properties (sslMode, then the connection's vendor keys)."""
    values = dict(parameters(connection))
    properties = {"sslMode": str(values["sslmode"]).upper()} if values.get("sslmode") else {}
    # undeclared connection keys are JDBC driver properties (vendor arguments), e.g. connectTimeout, useCompression
    properties.update({str(key): str(value) for key, value in (getattr(connection, "vendor", {}) or {}).items()})
    return {"host": values.get("host"), "port": values.get("port"), "database": values.get("database"),
            "user": values.get("user"), "password": values.get("password"), "properties": properties}


def staging_statement(target: str, staging: str) -> str:
    return "CREATE TABLE %s LIKE %s" % (staging, target)


def merge_statement(target: str, staging: str, columns: Sequence[str], key: Sequence[str],
                    update: Sequence[str]) -> str:
    names = ", ".join(columns)
    sets = ", ".join("%s = VALUES(%s)" % (column, column) for column in update or key[:1])
    return "INSERT INTO %s (%s) SELECT %s FROM %s ON DUPLICATE KEY UPDATE %s" % (target, names, names, staging, sets)


def delete_statement(target: str, staging: str, key: Sequence[str]) -> str:
    matches = " AND ".join("t.%s = s.%s" % (column, column) for column in key)
    return "DELETE t FROM %s t INNER JOIN %s s ON %s" % (target, staging, matches)
