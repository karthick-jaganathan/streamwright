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

"""The files connector on the `duckdb` engine (kind: DuckDBJob): local files; roots are relative to the job's folder."""

import os
from collections.abc import Mapping
from functools import partial
from typing import Any

from streamwright.core.io import files
from streamwright.core.jobs.duckdb.adapter import AdapterError
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.core.sdk.engines.duckdb import DuckDBSession, ReadContext, WriteContext, WriteReceipt

__all__ = ["FilesDuckDBAdapter"]


def _locate(context: ReadContext | WriteContext):
    roots = context.connection["roots"]
    locate = partial(files.locate_local, context.session, roots)
    canonical_roots = [os.path.realpath(context.session.local_path(root)) for root in roots]

    def confined(path):
        location = locate(path)  # retain the existing lexical checks and error messages
        resolved = os.path.realpath(location)
        for root in canonical_roots:
            try:
                if os.path.commonpath((resolved, root)) == root:
                    return location
            except ValueError:  # paths on different volumes
                continue
        raise AdapterError("path %r resolves outside the connection's roots (%s)" % (path, ", ".join(roots)))

    return confined


class FilesDuckDBAdapter:
    """Reads and writes local files inside the connection's roots (writes create the folders they need)."""

    def configure(self, session: DuckDBSession, name: str, connection: ResolvedConnection) -> None:
        """Local files need no extension and no secret."""

    def read_sql(self, context: ReadContext, operation: Operation, arguments: Mapping[str, Any]) -> str:
        return files.read_sql(context, arguments, locate=_locate(context))

    def write(self, context: WriteContext, operation: Operation, arguments: Mapping[str, Any]) -> WriteReceipt:
        return files.write_files(context, arguments, locate=_locate(context), make_folders=True)
