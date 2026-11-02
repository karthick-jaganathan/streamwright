# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
The mongodb connector on the `spark` engine (kind: SparkJob): the MongoDB Spark connector reads a find (as an
aggregation pipeline) or an aggregate, and writes documents - `merge` updates in place on `primary_key` (default
`_id`), on the executors, without staging. The URI reaches the connector as a read or write option, never Spark's
configuration.
"""

import json
from collections.abc import Mapping
from typing import Any

from pyspark.sql import DataFrame, Observation, functions

from streamwright.connectors.mongodb import client
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.spark.sdk import Observation as ReceiptObservation
from streamwright.spark.sdk import SparkContext, WriteReceipt

__all__ = ["MongoDBSparkAdapter"]

FORMAT = "mongodb"


def _source(context: SparkContext, arguments: Mapping[str, Any]) -> tuple[str, str]:
    """(database, collection) of a call; `batch_size` is the source engine's."""
    if arguments.get("batch_size") is not None:
        raise ValueError("mongodb: `batch_size` is the source engine's; Spark takes `options`")
    database = arguments.get("database") or client.database_name(context.connection)
    if not database:
        raise ValueError("mongodb: name the database in the connection or the call's `database`")
    return database, arguments["collection"]


class MongoDBSparkAdapter:
    """Reads and writes collections through the MongoDB Spark connector."""

    jars = (("org.mongodb.spark:mongo-spark-connector_2.12", "ship", "10.4.0"),)
    packages = ("pymongo",)

    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> dict[str, str]:
        return {}

    def read(self, context: SparkContext, operation: Operation, arguments: Mapping[str, Any]) -> DataFrame:
        database, collection = _source(context, arguments)
        pipeline = arguments["pipeline"] if operation.method == "aggregate" else client.find_pipeline(arguments)
        reader = (context.spark.read.format(FORMAT).option("spark.mongodb.read.connection.uri",
                                                           client.uri(context.connection))
                  .option("spark.mongodb.read.database", database).option("spark.mongodb.read.collection", collection))
        if pipeline:
            reader = reader.option("spark.mongodb.read.aggregation.pipeline", json.dumps(pipeline))
        for key, value in (arguments.get("options") or {}).items():
            reader = reader.option(str(key), str(value))
        return reader.load()

    def write(self, context: SparkContext, frame: DataFrame, operation: Operation,
              arguments: Mapping[str, Any]) -> WriteReceipt:
        database, collection = _source(context, arguments)
        metrics = Observation("mongodb_" + context.name)
        writer = (frame.observe(metrics, functions.count(functions.lit(1)).alias("rows")).write.format(FORMAT)
                  .option("spark.mongodb.write.connection.uri", client.uri(context.connection))
                  .option("spark.mongodb.write.database", database)
                  .option("spark.mongodb.write.collection", collection).mode("append"))
        if arguments.get("mode") == "merge":
            writer = (writer.option("spark.mongodb.write.operationType", "update")
                      .option("spark.mongodb.write.idFieldList", ",".join(arguments.get("primary_key") or ["_id"]))
                      .option("spark.mongodb.write.upsertDocument", "true"))
        for key, value in (arguments.get("options") or {}).items():
            writer = writer.option(str(key), str(value))
        writer.save()
        rows = int(metrics.get.get("rows", 0))
        return WriteReceipt(context.name, "%s.%s" % (database, collection), rows,
                            observations=[ReceiptObservation(context.name, {"rows": rows})])
