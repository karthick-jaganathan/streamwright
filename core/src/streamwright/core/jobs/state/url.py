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

"""
State database URLs (`sw state --url`, STREAMWRIGHT_STATE_URL):

  sqlite:///state.db            a file relative to the working directory (three slashes)
  sqlite:////var/lib/state.db   an absolute path (four slashes)
  sqlite:///:memory:            an in-memory database (lives as long as the resolved URL)
  duckdb:///state.duckdb, duckdb:////abs/state.duckdb, duckdb:///:memory:   likewise; ?catalog=NAME
  postgresql://user:pass@host:5432/dbname?schema=streamwright   (postgres:// too); other query parameters
                                                                 go to the driver (sslmode=require, ...)
  mysql://user:pass@host:3306/dbname    the tables live in database `dbname` (?schema=NAME: another one)

User names and passwords are percent-decoded. `schema` defaults to `streamwright` (MySQL: the URL's database).
"""

import importlib
import importlib.util
import os
import sqlite3
import urllib.parse
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from streamwright.core.jobs.state.dialects import (
    DEFAULT_SCHEMA,
    Dialect,
    DuckDBDialect,
    MySQLDialect,
    PostgresDialect,
    SqliteDialect,
)
from streamwright.core.jobs.state.migrations.runner import MigrationRunner
from streamwright.core.jobs.state.store import SqlStateStore
from streamwright.core.jobs.state.target import describe_url

ENV_STATE_URL = "STREAMWRIGHT_STATE_URL"

__all__ = ["DRIVERS", "ENV_STATE_URL", "ResolvedStateUrl", "migration_runner_for_url", "missing_driver", "open_url",
           "resolve_state_url"]

_MEMORY = ("", ":memory:")
_OUR_PARAMETERS = ("schema", "catalog")


class _CursorConnection:
    """`connection.execute(sql, args)` for drivers that only execute on cursors (PyMySQL, psycopg2)."""

    def __init__(self, connection):
        self._connection = connection

    def execute(self, statement, parameters=()):
        cursor = self._connection.cursor()
        cursor.execute(statement, tuple(parameters))  # always formatted: the dialect escaped literal %
        return cursor

    def commit(self):
        return self._connection.commit()

    def rollback(self):
        return self._connection.rollback()

    def close(self):
        return self._connection.close()


@dataclass(frozen=True)
class ResolvedStateUrl:
    """A state URL resolved to a dialect and a DB-API connection factory."""
    url: str
    scheme: str
    dialect: Dialect
    connect: Callable[[], Any]
    schema: str | None
    catalog: str | None = None
    keepalive: Any = field(default=None, repr=False, compare=False)   # holds an in-memory database open

    @property
    def target(self) -> str:
        """The database, without its password or query parameters."""
        return describe_url(self.url, self.dialect.name, self.schema, self.catalog)

    def make_runner(self) -> MigrationRunner:
        return MigrationRunner(self.connect, self.dialect, schema=self.schema, catalog=self.catalog,
                               target=self.target)


def _file_path(parsed):
    """sqlite:///rel.db -> rel.db, sqlite:////abs.db -> /abs.db, sqlite:///:memory: -> :memory:."""
    path = urllib.parse.unquote(parsed.netloc + parsed.path)
    path = path[1:] if path.startswith("/") and not parsed.netloc else path
    return ":memory:" if path in _MEMORY else path


def _sqlite(url, parsed, schema):
    path = _file_path(parsed)
    if path == ":memory:":
        name = "file:streamwright_%s?mode=memory&cache=shared" % uuid.uuid4().hex

        def connect():
            return sqlite3.connect(name, uri=True, timeout=30, isolation_level=None)

        return ResolvedStateUrl(url, parsed.scheme, SqliteDialect(), connect, schema, keepalive=connect())
    return ResolvedStateUrl(url, parsed.scheme, SqliteDialect(),
                            lambda: sqlite3.connect(path, timeout=30, isolation_level=None), schema)


# the DB-API drivers a database connection opens with (optional dependencies), and how to install one
DRIVERS = MappingProxyType({"postgres": (("psycopg", "psycopg2"), "pip install 'psycopg[binary]'"),
                            "mysql": (("pymysql",), "pip install pymysql")})


def missing_driver(provider):
    """None when `provider`'s connections can be opened here (a driver is importable, or none is needed), else what
    is missing and how to install it. Nothing is imported or connected."""
    names, install = DRIVERS.get(provider, ((), None))
    if not names or any(importlib.util.find_spec(name) is not None for name in names):
        return None
    return "%s is not installed (%s)" % (" or ".join(names), install)


def _driver(*names):
    """The first of these DB-API modules that is installed (drivers are optional)."""
    for name in names:
        try:
            return importlib.import_module(name)
        except ImportError:
            continue
    raise ImportError("this state URL needs %s installed" % " or ".join(names))


def _duckdb(url, parsed, schema, catalog):
    duckdb = _driver("duckdb")
    path = _file_path(parsed)
    if path == ":memory:":
        database = duckdb.connect(":memory:")
        return ResolvedStateUrl(url, parsed.scheme, DuckDBDialect(), database.cursor, schema, catalog,
                                keepalive=database)
    return ResolvedStateUrl(url, parsed.scheme, DuckDBDialect(), lambda: duckdb.connect(path).cursor(), schema,
                            catalog)


def _postgres(url, parsed, query, schema):
    driver_url = urllib.parse.urlunparse(parsed._replace(scheme="postgresql", query=urllib.parse.urlencode(
        [(key, value) for key, value in query if key not in _OUR_PARAMETERS])))

    def connect():
        driver = _driver(*DRIVERS["postgres"][0])
        connection = driver.connect(driver_url)
        return connection if hasattr(connection, "execute") else _CursorConnection(connection)  # psycopg2

    return ResolvedStateUrl(url, parsed.scheme, PostgresDialect(), connect, schema or DEFAULT_SCHEMA)


def _mysql(url, parsed, schema):
    database = urllib.parse.unquote(parsed.path.lstrip("/")) or None
    unquote = urllib.parse.unquote

    def connect():
        return _CursorConnection(_driver(*DRIVERS["mysql"][0]).connect(host=parsed.hostname, port=parsed.port or 3306,
                                          user=unquote(parsed.username) if parsed.username else None,
                                          password=unquote(parsed.password) if parsed.password else "",
                                          database=database, autocommit=False))

    return ResolvedStateUrl(url, parsed.scheme, MySQLDialect(), connect, schema or database or DEFAULT_SCHEMA)


def resolve_state_url(url: str | None = None, schema: str | None = None) -> ResolvedStateUrl:
    """A state URL (or STREAMWRIGHT_STATE_URL) resolved; `schema` overrides the URL's."""
    effective = url or os.environ.get(ENV_STATE_URL)
    if not effective:
        raise ValueError("no state database URL provided: give --url or set %s" % ENV_STATE_URL)
    parsed = urllib.parse.urlparse(effective)
    query = urllib.parse.parse_qsl(parsed.query)
    values = dict(query)
    schema = schema or values.get("schema")
    scheme = parsed.scheme.lower()
    if scheme in ("sqlite", "sqlite3"):
        return _sqlite(effective, parsed, schema)
    if scheme == "duckdb":
        return _duckdb(effective, parsed, schema or DEFAULT_SCHEMA, values.get("catalog"))
    if scheme in ("postgresql", "postgres"):
        return _postgres(effective, parsed, query, schema)
    if scheme == "mysql":
        return _mysql(effective, parsed, schema)
    raise ValueError("unsupported state database URL scheme %r (sqlite, duckdb, postgresql, mysql)" % scheme)


def migration_runner_for_url(url: str | None = None, schema: str | None = None) -> MigrationRunner:
    """A MigrationRunner for a state database URL."""
    return resolve_state_url(url=url, schema=schema).make_runner()


def open_url(url: str | None = None, schema: str | None = None):
    """A SqlStateStore for a state database URL (it keeps an in-memory database open)."""
    resolved = resolve_state_url(url=url, schema=schema)
    store = SqlStateStore(resolved.connect, resolved.dialect, schema=resolved.schema, catalog=resolved.catalog,
                          target=resolved.target)
    store.keepalive = resolved.keepalive
    return store
