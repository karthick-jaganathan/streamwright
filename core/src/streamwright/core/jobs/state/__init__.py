#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
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

"""Bookkeeping state shared by every job engine (runtime.state)."""

import sqlite3

from streamwright.core.jobs.state.dialects import (
    DEFAULT_SCHEMA,
    DIALECTS,
    DuckDBDialect,
    PostgresDialect,
    SqliteDialect,
)
from streamwright.core.jobs.state.protocol import (
    STATUSES,
    TERMINAL_STATUSES,
    LeaseHeldError,
    StaleLeaseError,
    StateError,
    StateStore,
)
from streamwright.core.jobs.state.store import SqlStateStore

__all__ = [
    "DEFAULT_SCHEMA",
    "STATUSES",
    "TERMINAL_STATUSES",
    "DuckDBDialect",
    "LeaseHeldError",
    "PostgresDialect",
    "SqlStateStore",
    "SqliteDialect",
    "StaleLeaseError",
    "StateError",
    "StateStore",
    "open_sqlite",
    "open_store",
]


def open_sqlite(path, schema=DEFAULT_SCHEMA):
    """A store in a SQLite file (tables prefixed `<schema>_`)."""
    return SqlStateStore(lambda: sqlite3.connect(path, timeout=30, isolation_level=None), SqliteDialect(), schema)


def open_store(connect, dialect, schema=DEFAULT_SCHEMA, catalog=None):
    """A store over any DB-API connect callable; dialect: sqlite, postgres or duckdb."""
    if dialect not in DIALECTS:
        raise StateError("unknown state dialect %r (known: %s)" % (dialect, ", ".join(sorted(DIALECTS))))
    return SqlStateStore(connect, DIALECTS[dialect](), schema, catalog)
