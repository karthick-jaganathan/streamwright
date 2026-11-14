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

"""The azure_blob connector on the `spark` engine (kind: SparkJob): the ABFS filesystem (abfss://)."""

from collections.abc import Mapping
from functools import partial
from typing import Any

from pyspark.sql import DataFrame

from streamwright.connectors.azure_blob import client
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.spark.io.files import FileLocation, read_files, write_files
from streamwright.spark.sdk import SparkContext, WriteReceipt

__all__ = ["AzureBlobSparkAdapter"]

_JARS = (("org.apache.hadoop:hadoop-azure", "provided", "3.3.4"),)


class AzureBlobSparkAdapter:
    """Translate authorized azure:// paths into ABFS ones; the platform's credentials sign every request."""

    jars = _JARS

    def location(self, connection: ResolvedConnection) -> FileLocation:
        account = client.account_name(connection)
        if account is None:
            raise ValueError("account_name is required for Spark")
        return FileLocation(tuple(connection["roots"]), client.SCHEMES[0], "abfss",
                            partial(client.hadoop_url, account=account), client.ABFS_PATTERN,
                            client.SOURCE_REPLACEMENT)

    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> dict[str, str]:
        if client.has_credentials(connection):
            raise ValueError("Azure credentials cannot be supplied to Spark; use the platform's credentials")
        self.location(connection)
        return {}

    def read(self, context: SparkContext, operation: Operation, arguments: Mapping[str, Any]) -> DataFrame:
        return read_files(context, arguments, location=self.location(context.connection))

    def write(self, context: SparkContext, frame: DataFrame, operation: Operation,
              arguments: Mapping[str, Any]) -> WriteReceipt:
        return write_files(context, frame, arguments, location=self.location(context.connection))
