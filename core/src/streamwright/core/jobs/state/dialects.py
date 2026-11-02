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
The SQL dialects the bookkeeping store runs on. A dialect is data: types, the database clock, parameter style,
locking suffixes and how a table is named (a dedicated schema where the database has schemas, a `streamwright_`
prefix on SQLite). The store never branches on a dialect's name.
"""

import datetime
import re
from types import MappingProxyType

from streamwright.core.sql.quote import quote_ident

__all__ = ["DEFAULT_SCHEMA", "DIALECTS", "Dialect", "DuckDBDialect", "PostgresDialect", "SqliteDialect"]

DEFAULT_SCHEMA = "streamwright"
_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class Dialect:
    name = None
    timestamp_type = "TIMESTAMPTZ"
    json_type = "JSON"
    json_parameter = "CAST(? AS JSON)"
    now = "now()"
    begin = None                      # statement that opens a write transaction (None: the driver opens it)
    share_lock = ""                   # on the fencing read: blocks a lease takeover until the writer commits
    update_lock = ""                  # on the lease read in acquire_lease

    def placeholders(self, text):
        return text

    def expires_in(self, parameter):
        """The database time `parameter` seconds from now."""
        raise NotImplementedError

    def namespace(self, schema, catalog=None):
        """How table names are qualified: (prefix before the table name, the CREATE SCHEMA statement or None)."""
        if not _IDENT.match(schema or ""):
            raise ValueError("invalid state schema name %r" % (schema,))
        qualified = quote_ident(schema) if catalog is None else "%s.%s" % (quote_ident(catalog), quote_ident(schema))
        return qualified + ".", "CREATE SCHEMA IF NOT EXISTS %s" % qualified

    def timestamp_parameter(self, when):
        return when

    def read_timestamp(self, value):
        """An ISO-8601 UTC text for a timestamp read back from the database."""
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=datetime.UTC)
            return value.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return str(value)


class SqliteDialect(Dialect):
    name = "sqlite"
    timestamp_type = "TEXT"           # ISO-8601 UTC text, ordered as text
    json_type = "TEXT"
    json_parameter = "?"
    now = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
    begin = "BEGIN IMMEDIATE"         # take the write lock before reading the lease

    def expires_in(self, parameter):
        return "strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ', 'now', '+' || %s || ' seconds')" % parameter

    def namespace(self, schema, catalog=None):
        if catalog is not None:
            raise ValueError("a SQLite state store has no catalog")
        if not _IDENT.match(schema or ""):
            raise ValueError("invalid state schema name %r" % (schema,))
        return schema + "_", None

    def timestamp_parameter(self, when):
        utc = when.astimezone(datetime.UTC)
        return utc.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (utc.microsecond // 1000)


class PostgresDialect(Dialect):
    name = "postgres"
    json_type = "JSONB"
    json_parameter = "CAST(? AS JSONB)"
    share_lock = " FOR SHARE"
    update_lock = " FOR UPDATE"

    def placeholders(self, text):
        return text.replace("%", "%%").replace("?", "%s")

    def expires_in(self, parameter):
        return "now() + make_interval(secs => %s)" % parameter


class DuckDBDialect(Dialect):
    name = "duckdb"
    begin = "BEGIN TRANSACTION"

    def expires_in(self, parameter):
        return "now() + to_seconds(CAST(%s AS BIGINT))" % parameter


DIALECTS = MappingProxyType({"sqlite": SqliteDialect, "postgres": PostgresDialect, "duckdb": DuckDBDialect})
