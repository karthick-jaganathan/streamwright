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

__all__ = ["DEFAULT_SCHEMA", "DIALECTS", "Dialect", "DuckDBDialect", "MySQLDialect", "PostgresDialect",
           "SqliteDialect"]

DEFAULT_SCHEMA = "streamwright"
_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class Dialect:
    name = None
    timestamp_type = "TIMESTAMPTZ"
    json_type = "JSON"
    json_parameter = "CAST(? AS JSON)"
    now = "now()"
    begin = None                      # explicit DB-API transaction statement, when the driver needs one
    share_lock = ""                   # on the fencing read: blocks a lease takeover until the writer commits
    update_lock = ""                  # on the lease read in acquire_lease
    migration_lock = ""               # transaction-scoped lock held while schema migrations are applied
    key_type = "TEXT"
    long_key_type = "TEXT"
    text_type = "TEXT"
    table_options = ""

    def migration_locked(self, row):
        """Whether the migration_lock statement's result row means the lock was taken."""
        return True

    def conflict_ignore(self, columns):
        return "ON CONFLICT (%s) DO NOTHING" % ", ".join(columns)

    def conflict_update(self, keys, values):
        return "ON CONFLICT (%s) DO UPDATE SET %s" % (
            ", ".join(keys), ", ".join("%s = excluded.%s" % (column, column) for column in values))

    def create_index(self, name, table, columns):
        return "CREATE INDEX IF NOT EXISTS %s ON %s (%s)" % (name, table, columns)

    def placeholders(self, text):
        """`text` with its `?` parameters in the driver's paramstyle (and literal `%` escaped for format styles)."""
        return text

    def expires_in(self, parameter):
        """The database time `parameter` seconds from now."""
        raise NotImplementedError

    def namespace(self, schema, catalog=None):
        """How table names are qualified: (prefix before the table name, the CREATE SCHEMA statement or None)."""
        if schema is None:
            return "", None
        if not _IDENT.match(schema):
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
        if schema is None or schema == "":
            return "", None
        if not _IDENT.match(schema):
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
    migration_lock = "SELECT pg_advisory_xact_lock(hashtext('streamwright'), hashtext(?))"

    def placeholders(self, text):
        return text.replace("%", "%%").replace("?", "%s")

    def expires_in(self, parameter):
        return "now() + make_interval(secs => %s)" % parameter


class MySQLDialect(Dialect):
    name = "mysql"
    timestamp_type = "DATETIME(6)"
    # 191 characters keeps composite utf8mb4 primary keys within InnoDB's 3072-byte limit.
    key_type = "VARCHAR(191)"
    long_key_type = "VARCHAR(500)"
    text_type = "LONGTEXT"
    now = "UTC_TIMESTAMP(6)"
    begin = "START TRANSACTION"
    share_lock = " FOR SHARE"
    update_lock = " FOR UPDATE"
    migration_lock = "SELECT GET_LOCK(CONCAT('sw:', LEFT(SHA2(CONCAT(DATABASE(), ':', ?), 256), 60)), 60)"
    table_options = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

    def migration_locked(self, row):
        return row is not None and row[0] is not None and int(row[0]) == 1  # 0: timed out, NULL: error

    def placeholders(self, text):
        return text.replace("%", "%%").replace("?", "%s")

    def namespace(self, schema, catalog=None):
        if catalog is not None:
            raise ValueError("a MySQL state store does not use a catalog")
        if not _IDENT.match(schema or ""):
            raise ValueError("invalid state schema name %r" % (schema,))
        return schema + ".", "CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET utf8mb4" % schema

    def expires_in(self, parameter):
        return "DATE_ADD(UTC_TIMESTAMP(6), INTERVAL %s SECOND)" % parameter

    def timestamp_parameter(self, when):
        return when.astimezone(datetime.UTC).replace(tzinfo=None)

    def conflict_ignore(self, columns):
        column = columns[0]
        return "ON DUPLICATE KEY UPDATE %s = %s" % (column, column)

    def conflict_update(self, keys, values):
        return "ON DUPLICATE KEY UPDATE %s" % ", ".join(
            "%s = VALUES(%s)" % (column, column) for column in values)

    def create_index(self, name, table, columns):
        return "CREATE INDEX %s ON %s (%s)" % (name, table, columns)


class DuckDBDialect(Dialect):
    name = "duckdb"
    begin = "BEGIN TRANSACTION"

    def expires_in(self, parameter):
        return "now() + to_seconds(CAST(%s AS BIGINT))" % parameter


DIALECTS = MappingProxyType({"sqlite": SqliteDialect, "postgres": PostgresDialect, "mysql": MySQLDialect,
                              "duckdb": DuckDBDialect})
