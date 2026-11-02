# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The files connector on the `spark` engine (kind: SparkJob): local files and POSIX mounts through `file://`."""

from collections.abc import Mapping
from typing import Any

from pyspark.sql import DataFrame

from streamwright.connectors.files import client
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.spark.io.files import FileLocation, read_files, write_files
from streamwright.spark.sdk import SparkContext, WriteReceipt

__all__ = ["FilesSparkAdapter"]


class FilesSparkAdapter:
    """Reads and writes inside the connection's roots; Spark's native filesystem needs no jar and no setting."""

    jars = ()

    def location(self, connection: ResolvedConnection) -> FileLocation:
        return FileLocation(tuple(connection["roots"]), "", client.HADOOP_SCHEME, client.hadoop_url,
                            client.SOURCE_PATTERN)

    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> dict[str, str]:
        return {}

    def read(self, context: SparkContext, operation: Operation, arguments: Mapping[str, Any]) -> DataFrame:
        return read_files(context, arguments, location=self.location(context.connection))

    def write(self, context: SparkContext, frame: DataFrame, operation: Operation,
              arguments: Mapping[str, Any]) -> WriteReceipt:
        return write_files(context, frame, arguments, location=self.location(context.connection))
