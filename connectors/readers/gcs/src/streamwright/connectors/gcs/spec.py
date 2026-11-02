# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""What the gcs connector accepts: its connection fields and the catalog's file operations (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition, Field
from streamwright.core.sdk.operations import files

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="gcs",
    title="Google Cloud Storage",
    family="object storage",
    summary="csv, tsv, json, jsonl and parquet objects in Google Cloud Storage",
    connection=(
        Field("roots", "list", required=True, item_type="string", pattern=r"^gs://",
              description="the gs:// URL prefixes the connection may read and write; relative keys use the first"),
        Field("key_id", "string", secret=True, description="HMAC access id (with secret; none: anonymous access)"),
        Field("secret", "string", secret=True, description="HMAC secret"),
    ),
    operations=(files.READ, files.WRITE),
    adapters={
        "source": "streamwright.connectors.gcs.engines.source:GcsSourceAdapter",
        "duckdb": "streamwright.connectors.gcs.engines.duckdb:GcsDuckDBAdapter",
        "spark": "streamwright.connectors.gcs.engines.spark:GcsSparkAdapter",
    },
    validator="streamwright.connectors.gcs.client:validate_connection",
)
