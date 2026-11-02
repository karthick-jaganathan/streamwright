# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""What the deltalake connector accepts: its connection fields, the catalog's table operations and `table.query`."""

from streamwright.core.sdk.contract import ConnectorDefinition, EngineLimit, Field, Operation, OperationKind
from streamwright.core.sdk.operations import files, tables

__all__ = ["CONNECTOR", "QUERY"]

QUERY = Operation(
    service="table",
    method="query",
    kind=OperationKind.READ,
    description="Run one read-only DuckDB SELECT over delta_scan('<a path inside the roots>') or `delta_table` "
                "(the table at `path`); `$name` parameters take the values of `params`. Source engine only (on "
                "DuckDBJob and SparkJob, read the table and use a step's sql).",
    arguments=(
        Field("query", "string", required=True, description="one SELECT, with $name for the values in params"),
        Field("params", "mapping", description="the values of the query's $name parameters"),
        Field("path", "string", pattern=files.NO_QUERY, description="the table the query reads as `delta_table`"),
    ),
)

CONNECTOR = ConnectorDefinition(
    name="deltalake",
    title="Delta Lake",
    family="table formats",
    summary="Delta Lake tables in local folders or object storage: reads, queries, and appends, overwrites and merges",
    connection=(
        Field("roots", "list", required=True, item_type="string",
              description="the folders and URL prefixes tables may be read and written in (relative paths: below "
                          "the first)"),
        Field("aws_access_key_id", "string", secret=True, description="S3: access key id"),
        Field("aws_secret_access_key", "string", secret=True, description="S3: secret access key"),
        Field("aws_session_token", "string", secret=True, description="S3: the session token of temporary credentials"),
        Field("aws_region", "string", pattern=r"^[A-Za-z0-9_\-]+$", description="S3: the buckets' region"),
        Field("s3_endpoint", "string", pattern=r"^[A-Za-z0-9._\-]+(:[0-9]+)?$",
              description="S3: an S3-compatible endpoint as host[:port]"),
        Field("s3_url_style", "string", choices=("vhost", "path"), description="S3: path for most compatible stores"),
        Field("s3_use_ssl", "boolean", description="S3: false for plain HTTP to the endpoint"),
        Field("gcs_key_file", "string", description="GCS: a service account key file"),
        Field("gcs_project_id", "string", description="GCS: the project"),
        Field("azure_connection_string", "string", secret=True, description="Azure: a connection string"),
        Field("azure_account_name", "string", description="Azure: the storage account"),
        Field("azure_account_key", "string", secret=True, description="Azure: the account key"),
        Field("storage", "string",
              description="Spark: the object-store connection whose credentials and settings reach the tables"),
        Field("schema", "string", description="Spark: the default catalog schema of registered tables"),
        Field("schema_location", "string", description="Spark: the location of a schema the writes create"),
        Field("schema_comment", "string", description="Spark: the comment of a schema the writes create"),
    ),
    operations=(tables.READ, tables.WRITE, QUERY),
    adapters={
        "source": "streamwright.connectors.deltalake.engines.source:DeltaLakeSourceAdapter",
        "duckdb": "streamwright.connectors.deltalake.engines.duckdb:DeltaLakeDuckDBAdapter",
        "spark": "streamwright.connectors.deltalake.engines.spark:DeltaLakeSparkAdapter",
    },
    validator="streamwright.connectors.deltalake.client:validate_connection",
    engine_limits=(EngineLimit(engine="duckdb", operation=QUERY.name),
                   EngineLimit(engine="spark", operation=QUERY.name)),
    duckdb_extensions=("delta", "httpfs", "azure")  # delta, and the extension of the table's object store,
)
