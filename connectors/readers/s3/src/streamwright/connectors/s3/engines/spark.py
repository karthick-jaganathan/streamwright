"""S3's Spark adapter: client rules plus composable Spark file I/O."""

from collections.abc import Mapping
from typing import Any

from pyspark.sql import DataFrame

from streamwright.connectors.s3 import client
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.spark.io.files import FileLocation, read_files, write_files
from streamwright.spark.sdk import SparkContext, WriteReceipt

__all__ = ["S3SparkAdapter"]

_JARS = (("org.apache.hadoop:hadoop-aws", "provided", "3.3.4"),
         ("com.amazonaws:aws-java-sdk-bundle", "provided", "1.12.262"))


class S3SparkAdapter:
    """Translate authorized s3:// paths into S3A without putting secrets in Spark configuration."""

    jars = _JARS

    @staticmethod
    def location(connection: ResolvedConnection) -> FileLocation:
        return FileLocation(tuple(connection["roots"]), client.STORE.schemes[0],
                            client.HADOOP_SCHEME, client.hadoop_url)

    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> dict[str, str]:
        if client.has_credentials(connection):
            raise ValueError("S3 credentials cannot be supplied to Spark; use the platform credential chain")
        settings = {}
        for bucket in client.buckets(connection):
            prefix = f"fs.s3a.bucket.{bucket}."
            if connection.get("endpoint"):
                settings[prefix + "endpoint"] = str(connection["endpoint"])
                settings[prefix + "path.style.access"] = str(client.uses_path_style(connection)).lower()
                settings[prefix + "connection.ssl.enabled"] = str(client.uses_ssl(connection)).lower()
            if connection.get("region"):
                settings[prefix + "endpoint.region"] = str(connection["region"])
        return settings

    def read(self, context: SparkContext, operation: Operation, arguments: Mapping[str, Any]) -> DataFrame:
        return read_files(context, arguments, location=self.location(context.connection))

    def write(self, context: SparkContext, frame: DataFrame, operation: Operation,
              arguments: Mapping[str, Any]) -> WriteReceipt:
        return write_files(context, frame, arguments, location=self.location(context.connection))
