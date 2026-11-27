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
The SQL dialects the bookkeeping store runs on. A dialect is mostly data: types, the database clock, parameter style,
locking suffixes and its namespace (`namespace()`: a dedicated schema where the database has schemas, a `<schema>_`
prefix on SQLite). The store and the migration runner branch on a dialect's name in a few places only: SQLite keeps
the sw_* tables unprefixed (its `<schema>_` prefix, `streamwright_` by default, named only the tables of the store
before sw_*, which the first migration imports), DuckDB selects its `catalog`, and the DDL is compiled per dialect
name (streamwright.core.jobs.state.migrations). Writes of many rows (an apply output's outbox) take a statement per
chunk of rows: `max_parameters` and `max_statement_bytes` bound a chunk, `update_from_rows` is the dialect's UPDATE
from a list of rows.
"""

import datetime
import re
import sqlite3
from dataclasses import dataclass
from types import MappingProxyType

from streamwright.core.sql.quote import quote_ident

__all__ = ["DEFAULT_SCHEMA", "DIALECTS", "Dialect", "DuckDBDialect", "MySQLDialect", "PostgresDialect", "RowUpdate",
           "SqliteDialect"]

DEFAULT_SCHEMA = "streamwright"
_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


@dataclass(frozen=True)
class RowUpdate:
    """
    An UPDATE of `table` (alias t) from rows of values, each updating the row whose `key` matches: `columns` [(name,
    value SQL with a ? parameter)] are a row's values (`key` among them), `assignments` [(column, SQL over v.<name>)]
    what they set, and `where` (SQL over t, its parameters after the rows') bounds the rows.
    """
    table: str
    key: str
    columns: tuple[tuple[str, str], ...]
    assignments: tuple[tuple[str, str], ...]
    where: str

    def values(self, rows):
        return ", ".join(["(%s)" % ", ".join(value for _, value in self.columns)] * rows)

    def sets(self, qualifier=""):
        return ", ".join("%s%s = %s" % (qualifier, column, expression) for column, expression in self.assignments)


class Dialect:
    name = None
    timestamp_type = "TIMESTAMPTZ"
    json_type = "JSON"
    json_parameter = "CAST(? AS JSON)"
    bigint_parameter = "CAST(? AS BIGINT)"
    now = "now()"
    max_parameters = 10_000           # the most parameters one multi-row statement binds (well within the limits)
    # about the most bytes of values one multi-row statement carries: a MySQL driver writes the values into the
    # statement, which must fit max_allowed_packet (4 MB by default on MySQL 5.7, often less on managed servers)
    max_statement_bytes = 1_000_000
    begin = None                      # explicit DB-API transaction statement, when the driver needs one
    share_lock = ""                   # on the fencing read: blocks a lease takeover until the writer commits
    update_lock = ""                  # on the lease read in acquire_lease
    migration_lock = ""               # transaction-scoped lock held while schema migrations are applied
    key_type = "TEXT"
    long_key_type = "TEXT"
    text_type = "TEXT"
    table_options = ""

    def json_text(self, column, key):
        """SQL for the text of a JSON column's top-level `key` (a constant of the code, never user input)."""
        return "json_extract_string(%s, '$.%s')" % (column, key)

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

    def update_from_rows(self, update, rows):
        """
        The SQL of `update` (a RowUpdate) from `rows` rows of values, or None where the database has no UPDATE from a
        list of rows (the caller then updates a row at a time).
        """
        return "UPDATE %s AS t SET %s FROM (VALUES %s) AS v(%s) WHERE t.%s = v.%s AND %s" % (
            update.table, update.sets(), update.values(rows), ", ".join(name for name, _ in update.columns),
            update.key, update.key, update.where)

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
    max_parameters = 10_000 if sqlite3.sqlite_version_info >= (3, 32, 0) else 999    # SQLITE_MAX_VARIABLE_NUMBER
    begin = "BEGIN IMMEDIATE"         # take the write lock before reading the lease

    def expires_in(self, parameter):
        return "strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ', 'now', '+' || %s || ' seconds')" % parameter

    def json_text(self, column, key):
        return "json_extract(%s, '$.%s')" % (column, key)

    def namespace(self, schema, catalog=None):
        if catalog is not None:
            raise ValueError("a SQLite state store has no catalog")
        if schema is None or schema == "":
            return "", None
        if not _IDENT.match(schema):
            raise ValueError("invalid state schema name %r" % (schema,))
        return schema + "_", None

    def update_from_rows(self, update, rows):
        # SQLite plans an UPDATE ... FROM a list of rows target-first: each of the batch's rows scans the list (rows x
        # chunk). In-process, a statement per row costs no round trip, so a row at a time is the faster way here.
        return None

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

    def json_text(self, column, key):
        return "(%s ->> '%s')" % (column, key)

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
    bigint_parameter = "CAST(? AS SIGNED)"
    now = "UTC_TIMESTAMP(6)"
    begin = "START TRANSACTION"
    share_lock = " FOR SHARE"
    update_lock = " FOR UPDATE"
    migration_lock = "SELECT GET_LOCK(CONCAT('sw:', LEFT(SHA2(CONCAT(DATABASE(), ':', ?), 256), 60)), 60)"
    table_options = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

    def migration_locked(self, row):
        return row is not None and row[0] is not None and int(row[0]) == 1  # 0: timed out, NULL: error

    def json_text(self, column, key):
        return "JSON_UNQUOTE(JSON_EXTRACT(%s, '$.%s'))" % (column, key)

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

    def update_from_rows(self, update, rows):
        # no VALUES list with column names: a UNION of one SELECT per row; SET names t's columns (v has the same)
        first = "SELECT " + ", ".join("%s AS %s" % (value, name) for name, value in update.columns)
        rest = " UNION ALL SELECT " + ", ".join(value for _, value in update.columns)
        return "UPDATE %s AS t JOIN (%s%s) AS v ON t.%s = v.%s SET %s WHERE %s" % (
            update.table, first, rest * (rows - 1), update.key, update.key, update.sets("t."), update.where)


class DuckDBDialect(Dialect):
    name = "duckdb"
    begin = "BEGIN TRANSACTION"

    def expires_in(self, parameter):
        return "now() + to_seconds(CAST(%s AS BIGINT))" % parameter


DIALECTS = MappingProxyType({"sqlite": SqliteDialect, "postgres": PostgresDialect, "mysql": MySQLDialect,
                              "duckdb": DuckDBDialect})
