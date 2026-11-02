# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""What the azure_blob connector accepts: its connection fields and the catalog's file operations (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition, EngineLimit, Field
from streamwright.core.sdk.operations import files

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="azure_blob",
    title="Azure Blob Storage",
    family="object storage",
    summary="csv, tsv, json, jsonl and parquet blobs in Azure Blob Storage (Data Lake Storage Gen2 included)",
    connection=(
        Field("roots", "list", required=True, item_type="string", pattern=r"^(azure|abfss)://",
              description="the azure:// or abfss:// URL prefixes (container/prefix) the connection may read and "
                          "write; relative keys use the first"),
        Field("connection_string", "string", secret=True,
              description="the storage account's connection string (one way to sign in)"),
        Field("account_name", "string", description="the storage account (with account_key, client_secret, or alone "
                                                    "for the platform's credential chain)"),
        Field("account_key", "string", secret=True, description="the storage account key (with account_name)"),
        Field("client_secret", "string", secret=True,
              description="a service principal's secret (with account_name, tenant_id and client_id)"),
        Field("tenant_id", "string", description="the service principal's tenant"),
        Field("client_id", "string", description="the service principal's application id"),
        Field("endpoint", "string", description="the blob endpoint, for emulators: host[:port] or an http(s) URL"),
        Field("use_ssl", "boolean", default=True, description="false: plain HTTP to the endpoint"),
    ),
    operations=(files.READ, files.WRITE),
    adapters={
        "source": "streamwright.connectors.azure_blob.engines.source:AzureBlobSourceAdapter",
        "duckdb": "streamwright.connectors.azure_blob.engines.duckdb:AzureBlobDuckDBAdapter",
        "spark": "streamwright.connectors.azure_blob.engines.spark:AzureBlobSparkAdapter",
    },
    validator="streamwright.connectors.azure_blob.client:validate_connection",
    # DuckDB's azure extension cannot COPY to Azure storage
    engine_limits=(EngineLimit(engine="duckdb", operation=files.WRITE.name),),
)
