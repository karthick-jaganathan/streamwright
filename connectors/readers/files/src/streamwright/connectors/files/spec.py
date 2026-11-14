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

"""What the files connector accepts: its connection fields and the catalog's file operations (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition, Field
from streamwright.core.sdk.operations import files

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="files",
    title="Local files",
    family="files",
    summary="csv, tsv, json, jsonl and parquet files in local folders and POSIX mounts",
    connection=(
        Field("roots", "list", required=True, item_type="string", pattern=r"^(?!.*://)",
              description="the local folders the connection may read and write; relative paths use the first. No "
                          "URLs: object storage has the s3, gcs and azure_blob connectors"),
    ),
    operations=(files.READ, files.WRITE),
    adapters={
        "source": "streamwright.connectors.files.engines.source:FilesSourceAdapter",
        "duckdb": "streamwright.connectors.files.engines.duckdb:FilesDuckDBAdapter",
        "spark": "streamwright.connectors.files.engines.spark:FilesSparkAdapter",
    },
    validator="streamwright.connectors.files.client:validate_connection",
)
