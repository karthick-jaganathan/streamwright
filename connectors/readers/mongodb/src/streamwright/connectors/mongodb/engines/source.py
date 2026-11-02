# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The mongodb connector on the `source` engine (streamwright run): read-only find and aggregate through pymongo."""

import itertools
from collections.abc import Callable, Iterator, Mapping
from typing import Any

from pymongo import MongoClient
from pymongo.errors import NetworkTimeout, PyMongoError, ServerSelectionTimeoutError

from streamwright.connectors.mongodb import client
from streamwright.core.sdk.contract import Operation, ResolvedConnection, vendor_arguments
from streamwright.core.sdk.engines.source import ConnectorContext, ConnectorError

__all__ = ["MongoDBSession", "MongoDBSourceAdapter"]

SERVER_SELECTION_MS = 10000


def _error(exc: PyMongoError, redact: Callable[[str], str]) -> ConnectorError:
    if isinstance(exc, (ServerSelectionTimeoutError, NetworkTimeout)):
        return ConnectorError("mongodb: timeout connecting to the server: %s" % redact(str(exc)), code="TIMEOUT",
                              retryable=True)
    return ConnectorError("mongodb: %s" % redact(str(exc)), code="QUERY_ERROR")


class MongoDBSession:
    """Reads a call's documents in pages of `batch_size`; the first page is read through context.call()."""

    def __init__(self, mongo: MongoClient, database: str | None):
        self.mongo, self.database = mongo, database

    def _cursor(self, operation: Operation, arguments: Mapping[str, Any]) -> Any:
        database = arguments.get("database") or self.database
        if not database:
            raise ConnectorError("mongodb: name the database in the connection or the call's `database`")
        collection = self.mongo[database][arguments["collection"]]
        # undeclared arguments are pymongo find/aggregate kwargs (vendor arguments): hint, max_time_ms, ...
        extras = {key: value for key, value in vendor_arguments(operation, arguments).items() if key != "options"}
        if operation.method == "aggregate":
            return collection.aggregate(list(arguments["pipeline"]), **extras)
        cursor = collection.find(dict(arguments.get("filter") or {}), projection=arguments.get("projection"),
                                 **extras)
        if arguments.get("sort"):
            cursor = cursor.sort([tuple(item) if isinstance(item, (list, tuple)) else (item, 1)
                                  for item in arguments["sort"]])
        return cursor.limit(int(arguments["limit"])) if arguments.get("limit") else cursor

    def read(self, operation: Operation, arguments: Mapping[str, Any],
             context: ConnectorContext) -> Iterator[list[dict[str, Any]]]:
        if arguments.get("options") is not None:
            raise ConnectorError("mongodb: `options` are the Spark connector's; the source engine takes batch_size")
        size = int(arguments.get("batch_size") or 1000)

        def start() -> tuple[list[Any], Iterator[Any]]:
            try:
                documents = iter(self._cursor(operation, arguments))
                return list(itertools.islice(documents, size)), documents
            except PyMongoError as exc:
                raise _error(exc, context.redact) from None
        batch, documents = context.call(start)
        while batch:
            yield [client.serialize(document) for document in batch]
            try:
                batch = list(itertools.islice(documents, size))
            except PyMongoError as exc:
                raise _error(exc, context.redact) from None

    def close(self) -> None:
        self.mongo.close()


class MongoDBSourceAdapter:
    """Connects with the connection's URI; its password is masked as written and decoded."""

    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> MongoDBSession:
        for value in client.masked_values(connection):
            context.secret(value)
        options = dict(getattr(connection, "vendor", {}) or {})
        mongo = MongoClient(client.uri(connection), serverSelectionTimeoutMS=SERVER_SELECTION_MS, **options)
        return MongoDBSession(mongo, client.database_name(connection))
