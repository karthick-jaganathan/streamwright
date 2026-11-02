# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
The deltalake connector on the `source` engine (streamwright run): table reads and queries through DuckDB's delta
extension, on a sandbox of the connector's own that is fail-closed - DuckDB itself reads only inside the roots
(allowed_directories, external access off, configuration locked) and every path is checked against them first.
"""

from collections.abc import Iterator, Mapping
from typing import Any

import duckdb

from streamwright.connectors.deltalake import client
from streamwright.core.io import databases
from streamwright.core.io.sandbox import ConnectError, DuckDBSandbox, load_extension, message
from streamwright.core.sdk import quote_literal
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.core.sdk.engines.source import ConnectorContext, ConnectorError

__all__ = ["DeltaLakeSession", "DeltaLakeSourceAdapter"]

CONNECTOR = "deltalake"


def _configure_storage(sandbox: DuckDBSandbox, connection: Mapping[str, Any], roots: tuple[str, ...]) -> None:
    """Each object store's extension, and its credentials as bound parameters of a secret scoped to its roots."""
    for secret in client.storage_secrets(connection, roots):
        load_extension(sandbox.connection, secret.extension)
        names = "".join(", %s ?" % parameter for parameter, _ in secret.parameters)
        statement = "CREATE OR REPLACE TEMPORARY SECRET streamwright_%s (TYPE %s%s%s)" % (
            secret.secret_type, secret.secret_type, names, ", SCOPE ?" if secret.scopes else "")
        try:
            sandbox.connection.execute(statement, [value for _, value in secret.parameters] + (
                [list(secret.scopes)] if secret.scopes else []))
        except duckdb.Error as exc:
            raise ConnectError("cannot create the %s secret: %s" % (
                secret.secret_type, sandbox.mask(message(exc)))) from None
    settings = client.gcs_settings(connection)
    if settings:
        load_extension(sandbox.connection, "httpfs")
    for name, value in settings:
        sandbox.connection.execute("SET %s = %s" % (name, quote_literal(value)))


class DeltaLakeSession:
    """`table.read` (a scan) and `table.query` on tables inside the roots."""

    def __init__(self, reader: databases.QueryReader, roots: tuple[str, ...]):
        self.reader, self.roots = reader, roots

    def _locate(self, path: str) -> str:
        try:
            return client.resolve_path(path, self.roots)
        except ValueError as exc:
            raise ConnectorError("%s: %s" % (CONNECTOR, exc), code="ACCESS_DENIED") from None

    def read(self, operation: Operation, arguments: Mapping[str, Any],
             context: ConnectorContext) -> Iterator[list[dict[str, Any]]]:
        if operation.method == "read":
            path = self._locate(arguments["path"])
            query, parameters = client.scan_query(path, arguments.get("columns"), arguments.get("where"),
                                                  arguments.get("limit"))
            label = "scan %s" % path
        else:
            if arguments.get("path") is not None:
                self.reader.connection.execute("CREATE OR REPLACE TEMPORARY VIEW %s AS SELECT * FROM delta_scan(%s)"
                                               % (client.VIEW, quote_literal(self._locate(arguments["path"]))))
            query, parameters, label = arguments["query"], dict(arguments.get("params") or {}), "query"
        return databases.read_pages(self.reader, context, connector=CONNECTOR, call=operation.name, label=label,
                                    query=query, parameters=parameters)

    def close(self) -> None:
        self.reader.close()


class DeltaLakeSourceAdapter:
    """Opens the locked sandbox: the delta extension, the storage secrets, then reads only inside the roots."""

    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> DeltaLakeSession:
        roots = client.normalize_roots(connection["roots"])
        masked = client.masked_values(connection, connection.secret_values)
        for value in masked:
            context.secret(value)
        sandbox = DuckDBSandbox(masked=masked, provider=CONNECTOR)
        try:
            sandbox.connection.execute("SET TimeZone = 'UTC'")
            load_extension(sandbox.connection, "delta")
            _configure_storage(sandbox, connection, roots)
            sandbox.lock(allowed_directories=roots)
        except Exception as exc:
            sandbox.close()
            error = databases.database_error(CONNECTOR, exc, context.redact)
            if error is None:
                raise
            raise error from None
        return DeltaLakeSession(databases.QueryReader(sandbox, client.query_policy(roots)), roots)
