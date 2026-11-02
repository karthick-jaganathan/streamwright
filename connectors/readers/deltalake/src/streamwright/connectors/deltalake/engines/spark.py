# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
The deltalake connector on the `spark` engine (kind: SparkJob): `table.read` and `table.write` through delta-spark.
Spark reaches the tables with the credentials and settings of the object-store connection named by `storage` (s3,
gcs, azure_blob, files), so a job defines each credential once; the connection's own storage keys are not used. A
table path must be inside both the connection's roots and the storage's.
"""

from collections.abc import Mapping
from typing import Any

from pyspark.sql import DataFrame

from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.spark.io import delta
from streamwright.spark.io.files import FileLocation
from streamwright.spark.sdk import SparkContext, WriteReceipt

__all__ = ["DeltaLakeSparkAdapter"]


def _storage(context: SparkContext) -> FileLocation:
    """The FileLocation of the connection's `storage` (an object-store connection)."""
    runtime = context.runtime
    target = runtime.connections[context.connection["storage"]]
    return runtime.adapters[target.provider].adapter.location(target.auth)


def _table_path(context: SparkContext, path: str) -> str:
    return delta.resolve_table_path(context, path, tuple(context.connection["roots"]), _storage(context))


class DeltaLakeSparkAdapter:
    """Delta reads (a pinned version) and replay-safe writes (append, overwrite, merge)."""

    jars = (("io.delta:delta-spark_2.12", "provided", "3.2.1"), ("io.delta:delta-storage", "provided", "3.2.1"))
    packages = delta.PACKAGES
    local_conf = delta.LOCAL_CONF

    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> dict[str, str]:
        return {}

    def check_links(self, connection: Mapping[str, Any], connections: Mapping[str, Any]) -> list[str]:
        target = connections.get(connection.get("storage"))
        if target is None:
            return ["storage must name the object-store connection that holds the tables"]
        if "roots" not in target:
            return ["storage %r has no roots: it must be an object-store connection" % connection["storage"]]
        return []

    def read(self, context: SparkContext, operation: Operation, arguments: Mapping[str, Any]) -> DataFrame:
        return delta.read_table(context, arguments, path=_table_path(context, arguments["path"]), operation=operation)

    def write(self, context: SparkContext, frame: DataFrame, operation: Operation,
              arguments: Mapping[str, Any]) -> WriteReceipt:
        connection = context.connection
        location = arguments.get("schema_location") or connection.get("schema_location")
        return delta.write_table(
            context, frame, arguments, path=_table_path(context, arguments["path"]),
            defaults={key: connection.get(key) for key in ("schema", "schema_comment")},
            schema_location=_table_path(context, location) if location else None, operation=operation)
