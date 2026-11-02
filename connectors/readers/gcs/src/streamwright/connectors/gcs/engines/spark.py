# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The gcs connector on the `spark` engine (kind: SparkJob): GCS through Hadoop's S3A filesystem."""

from collections.abc import Mapping
from typing import Any

from pyspark.sql import DataFrame

from streamwright.connectors.gcs import client
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.spark.io.files import FileLocation, read_files, write_files
from streamwright.spark.sdk import SparkContext, WriteReceipt

__all__ = ["GcsSparkAdapter"]

_JARS = (("org.apache.hadoop:hadoop-aws", "provided", "3.3.4"),
         ("com.amazonaws:aws-java-sdk-bundle", "provided", "1.12.262"))


class GcsSparkAdapter:
    """Translate authorized gs:// paths into S3A paths without putting secrets in Spark configuration."""

    jars = _JARS

    def location(self, connection: ResolvedConnection) -> FileLocation:
        return FileLocation(tuple(connection["roots"]), client.SCHEME, client.HADOOP_SCHEME, client.hadoop_url)

    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> dict[str, str]:
        if client.has_credentials(connection):
            raise ValueError("GCS HMAC keys cannot be supplied to Spark; use the platform's credentials")
        settings = {}
        for bucket in client.buckets(connection):
            prefix = f"fs.s3a.bucket.{bucket}."
            settings[prefix + "endpoint"] = client.HADOOP_ENDPOINT
            settings[prefix + "path.style.access"] = "true"
        return settings

    def read(self, context: SparkContext, operation: Operation, arguments: Mapping[str, Any]) -> DataFrame:
        return read_files(context, arguments, location=self.location(context.connection))

    def write(self, context: SparkContext, frame: DataFrame, operation: Operation,
              arguments: Mapping[str, Any]) -> WriteReceipt:
        return write_files(context, frame, arguments, location=self.location(context.connection))
