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

from streamwright.core.jobs.state.dialects import (
    DEFAULT_SCHEMA,
    DIALECTS,
    DuckDBDialect,
    MySQLDialect,
    PostgresDialect,
    SqliteDialect,
)
from streamwright.core.jobs.state.protocol import (
    STATUSES,
    TERMINAL_STATUSES,
    LeaseHeldError,
    MigrationsPendingError,
    StaleLeaseError,
    StateError,
    StateStore,
)
from streamwright.core.jobs.state.store import SqlStateStore
from streamwright.core.jobs.state.target import describe_location
from streamwright.core.jobs.state.url import open_url, resolve_state_url, sqlite_connect

__all__ = [
    "DEFAULT_SCHEMA",
    "STATUSES",
    "TERMINAL_STATUSES",
    "DuckDBDialect",
    "LeaseHeldError",
    "MigrationsPendingError",
    "MySQLDialect",
    "PostgresDialect",
    "SqlStateStore",
    "SqliteDialect",
    "StaleLeaseError",
    "StateError",
    "StateStore",
    "open_sqlite",
    "open_store",
    "open_url",
    "resolve_state_url",
]


def open_sqlite(path, schema=None):
    """A store in a SQLite file (tables with sw_ prefix); ":memory:": one in-memory database, for the store's life."""
    target = describe_location("sqlite", path=path)
    return SqlStateStore(sqlite_connect(path, target), SqliteDialect(), schema, target=target)


def open_store(connect, dialect, schema=DEFAULT_SCHEMA, catalog=None, target=None):
    """A store over any DB-API connect callable; dialect: sqlite, postgres, mysql or duckdb; `target`: which
    database it is, without secrets (see streamwright.core.jobs.state.target)."""
    if dialect not in DIALECTS:
        raise StateError("unknown state dialect %r (known: %s)" % (dialect, ", ".join(sorted(DIALECTS))))
    return SqlStateStore(connect, DIALECTS[dialect](), schema, catalog, target=target)
