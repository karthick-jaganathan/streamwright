#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
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
The google_sheets connector on the `spark` engine: records are fetched on the driver through the Sheets client and
pinned in the batch's manifest, so executors never call Google (no network egress, no quota use) and a replayed batch
reads the same records.
"""

from collections.abc import Mapping
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from streamwright.connectors.google_sheets import client
from streamwright.core.io.api_calls import make_caller, standalone_context, translate_pages
from streamwright.core.sdk import ConnectorError
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.spark.sdk import SparkContext, WriteReceipt

__all__ = ["GoogleSheetsSparkAdapter"]


def _fetch(connection: ResolvedConnection, operation: Operation, arguments: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The records of a read, fetched on the driver."""
    context = standalone_context(connection.redactor)
    sheets = client.connect(connection, context.secret, context.redact)
    try:
        pages = client.read_pages(sheets, operation.method, arguments, make_caller(context, client.translate_error))
        return [record for page in translate_pages(pages, client.translate_error) for record in page]
    finally:
        sheets.http.session.close()


class GoogleSheetsSparkAdapter:
    """Reads sheets driver-side; Spark's configuration never holds credentials."""

    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> Mapping[str, str]:
        return {}

    def read(self, context: SparkContext, operation: Operation, arguments: Mapping[str, Any]) -> DataFrame:
        key = "%s.*" % context.name
        pinned = getattr(context.runtime, "pinned", None)
        if pinned is not None and key in pinned:
            records = pinned[key]
        else:
            records = _fetch(context.connection, operation, arguments)
            if pinned is not None:
                pinned[key] = records
        if records:
            return context.spark.createDataFrame(records)
        return context.spark.createDataFrame([], StructType([]))

    def write(self, context: SparkContext, frame: DataFrame, operation: Operation,
              arguments: Mapping[str, Any]) -> WriteReceipt:
        raise ConnectorError("google_sheets is read-only")
