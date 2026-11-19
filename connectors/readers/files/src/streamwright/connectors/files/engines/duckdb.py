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

import glob
import os
from collections.abc import Mapping
from functools import partial
from typing import Any

from streamwright.core.io import files
from streamwright.core.io.local import is_local_glob
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.core.sdk.engines.duckdb import AdapterError, DuckDBSession, ReadContext, WriteContext, WriteReceipt

__all__ = ["FilesDuckDBAdapter"]


def _locate(context: ReadContext | WriteContext):
    roots = context.connection["roots"]
    locate = partial(files.locate_local, context.session, roots)
    canonical_roots = [os.path.realpath(context.session.local_path(root)) for root in roots]

    def inside(found: str) -> bool:
        resolved = os.path.realpath(found)
        for root in canonical_roots:
            try:
                if os.path.commonpath((resolved, root)) == root:
                    return True
            except ValueError:  # paths on different volumes
                continue
        return False

    def confined(path):
        location = locate(path)  # retain the existing lexical checks and error messages
        # a glob is checked file by file as DuckDB expands it (hidden entries too): a link it passes may lead out
        matches = glob.glob(location, recursive=True, include_hidden=True) if is_local_glob(location) else []
        outside = next((found for found in [location, *matches] if not inside(found)), None)
        if outside is not None:
            raise AdapterError("path %r resolves outside the connection's roots (%s)%s" % (
                path, ", ".join(roots), "" if outside == location else ": %s" % outside))
        return location

    return confined


class FilesDuckDBAdapter:
    """Reads and writes local files inside the connection's roots (writes create the folders they need)."""

    def configure(self, session: DuckDBSession, name: str, connection: ResolvedConnection) -> None:
        """Local files need no extension and no secret."""

    def read_sql(self, context: ReadContext, operation: Operation, arguments: Mapping[str, Any]) -> str:
        return files.read_sql(context, arguments, locate=_locate(context))

    def write(self, context: WriteContext, operation: Operation, arguments: Mapping[str, Any]) -> WriteReceipt:
        return files.write_files(context, arguments, locate=_locate(context), make_folders=True)
