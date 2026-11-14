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

import os
import sqlite3

from streamwright.core.jobs.state.dialects import (
    DEFAULT_SCHEMA,
    DIALECTS,
    DuckDBDialect,
    MySQLDialect,
    PostgresDialect,
    SqliteDialect,
)
from streamwright.core.jobs.state.memory import memory_connect
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
from streamwright.core.jobs.state.url import open_url, resolve_state_url

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
    if path == ":memory:":
        return SqlStateStore(memory_connect(), SqliteDialect(), schema, target=target)
    return SqlStateStore(_sqlite_file(path, target), SqliteDialect(), schema, target=target)


def _sqlite_file(path, target):
    """connect() of a SQLite file; its folder must exist (StateError names it, instead of sqlite's 'unable to open')."""
    def connect():
        folder = os.path.dirname(os.path.abspath(path))
        if not os.path.isdir(folder):
            raise StateError("cannot open the state database %s: its folder %s does not exist (create it, or point "
                             "runtime.state.sqlite elsewhere)" % (target, folder))
        return sqlite3.connect(path, timeout=30, isolation_level=None)
    return connect


def open_store(connect, dialect, schema=DEFAULT_SCHEMA, catalog=None, target=None):
    """A store over any DB-API connect callable; dialect: sqlite, postgres, mysql or duckdb; `target`: which
    database it is, without secrets (see streamwright.core.jobs.state.target)."""
    if dialect not in DIALECTS:
        raise StateError("unknown state dialect %r (known: %s)" % (dialect, ", ".join(sorted(DIALECTS))))
    return SqlStateStore(connect, DIALECTS[dialect](), schema, catalog, target=target)
