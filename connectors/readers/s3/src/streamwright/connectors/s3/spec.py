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

"""What the s3 connector accepts: its connection fields and the catalog's file operations (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition, Field
from streamwright.core.sdk.operations import files

__all__ = ["CONNECTOR"]

CONNECTOR = ConnectorDefinition(
    name="s3",
    title="Amazon S3",
    family="object storage",
    summary="csv, tsv, json, jsonl and parquet objects in Amazon S3 or an S3-compatible store",
    connection=(
        Field("roots", "list", required=True, item_type="string", pattern=r"^s3://",
              description="the s3:// URL prefixes the connection may read and write; relative keys use the first"),
        Field("key_id", "string", secret=True, description="access key id (with secret; none: anonymous access)"),
        Field("secret", "string", secret=True, description="secret access key"),
        Field("session_token", "string", secret=True, description="the session token of temporary credentials"),
        Field("region", "string", pattern=r"^[A-Za-z0-9_\-]+$", description="the buckets' region, e.g. us-east-1"),
        Field("endpoint", "string", pattern=r"^[A-Za-z0-9._\-]+(:[0-9]+)?$",
              description="an S3-compatible endpoint as host[:port] (no scheme: use_ssl picks it)"),
        Field("url_style", "string", choices=("vhost", "path"), description="path: most S3-compatible stores"),
        Field("use_ssl", "boolean", default=True, description="false: plain HTTP to the endpoint"),
    ),
    operations=(files.READ, files.WRITE),
    adapters={
        "source": "streamwright.connectors.s3.engines.source:S3SourceAdapter",
        "duckdb": "streamwright.connectors.s3.engines.duckdb:S3DuckDBAdapter",
        "spark": "streamwright.connectors.s3.engines.spark:S3SparkAdapter",
    },
    validator="streamwright.connectors.s3.client:validate_connection",
    duckdb_extensions=("httpfs",),
)
