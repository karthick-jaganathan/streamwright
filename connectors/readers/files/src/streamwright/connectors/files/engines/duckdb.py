# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The files connector on the `duckdb` engine (kind: DuckDBJob): local files; roots are relative to the job's folder."""

from collections.abc import Mapping
from functools import partial
from typing import Any

from streamwright.core.io import files
from streamwright.core.sdk.contract import Operation, ResolvedConnection
from streamwright.core.sdk.engines.duckdb import DuckDBSession, ReadContext, WriteContext, WriteReceipt

__all__ = ["FilesDuckDBAdapter"]


def _locate(context: ReadContext | WriteContext):
    return partial(files.locate_local, context.session, context.connection["roots"])


class FilesDuckDBAdapter:
    """Reads and writes local files inside the connection's roots (writes create the folders they need)."""

    def configure(self, session: DuckDBSession, name: str, connection: ResolvedConnection) -> None:
        """Local files need no extension and no secret."""

    def read_sql(self, context: ReadContext, operation: Operation, arguments: Mapping[str, Any]) -> str:
        return files.read_sql(context, arguments, locate=_locate(context))

    def write(self, context: WriteContext, operation: Operation, arguments: Mapping[str, Any]) -> WriteReceipt:
        return files.write_files(context, arguments, locate=_locate(context), make_folders=True)
