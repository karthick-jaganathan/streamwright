# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""What the mongodb connector accepts: its connection fields and its collection operations (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, OperationKind

__all__ = ["AGGREGATE", "CONNECTOR", "FIND", "WRITE"]

_COLLECTION = Field("collection", "string", required=True, description="the collection")
_DATABASE = Field("database", "string", description="the database (default: the connection's)")
_BATCH_SIZE = Field("batch_size", "integer", default=1000, description="source: documents in one page")
_OPTIONS = Field("options", "mapping", description="Spark: the MongoDB Spark connector's read or write options")

FIND = Operation(
    service="collection",
    method="find",
    kind=OperationKind.READ,
    description="Read the documents of a collection a filter matches.",
    arguments=(
        _COLLECTION,
        Field("filter", "mapping", description="the query filter document"),
        Field("projection", ("mapping", "list"), description="the projection document, or a list of fields"),
        Field("sort", "list", description="[field, direction] pairs (or field names: ascending)"),
        Field("limit", "integer", description="read at most this many documents"),
        _DATABASE, _BATCH_SIZE, _OPTIONS,
    ),
)

AGGREGATE = Operation(
    service="collection",
    method="aggregate",
    kind=OperationKind.READ,
    description="Read the documents an aggregation pipeline returns.",
    arguments=(
        _COLLECTION,
        Field("pipeline", "list", required=True, item_type="mapping", description="the pipeline's stages"),
        _DATABASE, _BATCH_SIZE, _OPTIONS,
    ),
)

WRITE = Operation(
    service="collection",
    method="write",
    kind=OperationKind.WRITE,
    idempotent=False,
    description="Write a step's rows into a collection, one document each.",
    arguments=(
        _COLLECTION,
        Field("mode", "string", default="append", choices=("append", "merge"),
              description="append: insert; merge: update the documents matching on primary_key, insert the rest"),
        Field("primary_key", "list", item_type="string", description="merge: the fields to match (default: _id)"),
        _DATABASE, _OPTIONS,
    ),
)

CONNECTOR = ConnectorDefinition(
    name="mongodb",
    title="MongoDB",
    family="databases",
    summary="read-only find and aggregate reads of MongoDB collections, and SparkJob collection writes",
    connection=(
        Field("uri", "string", secret=True, description="a mongodb:// or mongodb+srv:// URI (instead of the keys)"),
        Field("host", "string", description="the server's host name"),
        Field("port", "integer", default=27017, description="its port"),
        Field("database", "string", description="the database"),
        Field("username", "string", description="the user"),
        Field("password", "string", secret=True, description="the user's password"),
        Field("auth_source", "string", default="admin", description="the database that holds the user"),
    ),
    operations=(FIND, AGGREGATE, WRITE),
    adapters={
        "source": "streamwright.connectors.mongodb.engines.source:MongoDBSourceAdapter",
        "spark": "streamwright.connectors.mongodb.engines.spark:MongoDBSparkAdapter",
    },
    validator="streamwright.connectors.mongodb.client:validate_connection",
    loggers=("pymongo",),
)
