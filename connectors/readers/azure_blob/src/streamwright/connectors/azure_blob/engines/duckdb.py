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

"""
The azure_blob connector on the `duckdb` engine (kind: DuckDBJob): the azure extension with one secret per root,
scoped to it. It only reads: DuckDB's azure extension cannot COPY to Azure storage (the spec's `engine_limits`).
"""

from collections.abc import Mapping
from typing import Any

from streamwright.connectors.azure_blob import client
from streamwright.core.io import files
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.core.sdk.engines.duckdb import DuckDBSession, ReadContext

__all__ = ["AzureBlobDuckDBAdapter"]


class AzureBlobDuckDBAdapter:
    """Reads blobs inside the connection's roots; two connections never share credentials."""

    def configure(self, session: DuckDBSession, name: str, connection: ResolvedConnection) -> None:
        files.configure_store_secrets(client.STORE, session, name, connection)

    def read_sql(self, context: ReadContext, operation: Operation, arguments: Mapping[str, Any]) -> str:
        location = files.FileLocation(client.STORE, tuple(context.connection["roots"]))
        return files.read_sql(context, arguments, locate=location.resolve_path)
