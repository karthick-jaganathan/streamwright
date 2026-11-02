# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

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
