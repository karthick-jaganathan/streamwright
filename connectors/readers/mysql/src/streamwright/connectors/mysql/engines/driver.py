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
The mysql connector's PyMySQL connections (the DuckDBJob and SparkJob state stores, Spark's driver-side merges and
deletes), with the connection's socket and TLS: `sslmode` as MySQL's client means it (`required` encrypts without
verifying, `verify_ca` checks the certificate, `verify_identity` the host name too) and the `ssl_*` files of
`options` in one SSL context, so no file is dropped and TLS is never silently optional.
"""

import ssl
from collections.abc import Mapping
from typing import Any

from streamwright.connectors.mysql import client

try:  # the extras `state` and `spark` install it
    import pymysql
except ImportError:
    pymysql = None

__all__ = ["CONNECT_TIMEOUT", "StateConnection", "connect", "connect_arguments", "ssl_context"]

CONNECT_TIMEOUT = 15


def ssl_context(values: Mapping[str, Any]) -> ssl.SSLContext:
    """The SSL context of an encrypting `sslmode` and the connection's ssl_* files (CA, client key pair, CRL)."""
    mode = str(values.get("sslmode") or "").lower()
    context = ssl.create_default_context(cafile=values.get("ssl_ca") or None, capath=values.get("ssl_capath") or None)
    context.check_hostname = mode == "verify_identity"
    context.verify_mode = ssl.CERT_REQUIRED if mode in ("verify_ca", "verify_identity") else ssl.CERT_NONE
    if values.get("ssl_crl") or values.get("ssl_crlpath"):
        context.load_verify_locations(cafile=values.get("ssl_crl") or None, capath=values.get("ssl_crlpath") or None)
        context.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
    if values.get("ssl_cert"):
        context.load_cert_chain(values["ssl_cert"], keyfile=values.get("ssl_key") or None)
    if values.get("ssl_cipher"):
        context.set_ciphers(values["ssl_cipher"])
    return context


def connect_arguments(connection: Mapping[str, Any]) -> dict[str, Any]:
    """
    pymysql.connect's keyword arguments for a resolved connection. `preferred` (the default) is PyMySQL's opportunistic
    TLS, unless ssl_* files are given: PyMySQL then requires TLS (it has no optional TLS with a client certificate).
    """
    values = dict(client.parameters(connection))
    arguments = {"host": values.get("host"), "port": int(values.get("port") or client.DIALECT.default_port),
                 "database": values.get("database"), "user": values.get("user"), "password": values.get("password"),
                 "connect_timeout": CONNECT_TIMEOUT, "charset": "utf8mb4", "autocommit": False}
    if values.get("socket"):
        arguments["unix_socket"] = values["socket"]
    mode = str(values.get("sslmode") or "preferred").lower()
    if mode == "disabled":
        arguments["ssl_disabled"] = True
    elif mode != "preferred" or any(values.get(key) for key in client.DIALECT.connection_options if key != "socket"):
        arguments["ssl"] = ssl_context(values)
    return arguments


def connect(connection: Mapping[str, Any], package: str) -> Any:
    """A PyMySQL connection (autocommit off); RuntimeError naming the extra `package` when PyMySQL is missing."""
    if pymysql is None:
        raise RuntimeError("this needs PyMySQL: pip install \"streamwright-mysql[%s]\"" % package)
    return pymysql.connect(**connect_arguments(connection))


class StateConnection:
    """PyMySQL's cursor API as the small execute/transaction surface of the shared state store."""

    def __init__(self, connection: Any):
        self._connection = connection

    def execute(self, statement: str, parameters: Any = ()) -> Any:
        cursor = self._connection.cursor()
        cursor.execute(statement, parameters)
        return cursor

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()
