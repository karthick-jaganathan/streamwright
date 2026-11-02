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

"""
DuckDB and DuckLake warehouse outputs for `streamwright run`.

DuckLake catalogs: extensions, settings (environment variables), secrets and ATTACH are in
streamwright.core.outputs.ducklake.
"""

import datetime
import json
import logging
import os
import re

import duckdb

from streamwright.core.outputs.base import log_summary
from streamwright.core.outputs.ducklake import (
    S3_CREDENTIALS,
    attach_query,
    ducklake_settings,
    is_postgres,
    load_ducklake,
    load_extension,
    mask_values,
    postgres_secret_query,
    s3_secret_query,
)
from streamwright.core.outputs.loading import LoadingOutput
from streamwright.core.outputs.staging import Staging, checked_name, staged_table, typed_columns
from streamwright.core.outputs.staging import canonical as _canonical
from streamwright.core.outputs.staging import json_path as _json_path
from streamwright.core.outputs.staging import quote as _quote
from streamwright.core.outputs.staging import sql_string as _sql_string
from streamwright.core.runtime.templates import to_text

__all__ = ["DuckDBOutput", "DuckLakeOutput"]


LOG = logging.getLogger("streamwright.output")
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_STATE_TABLE = "_streamwright_state"
_LEGACY_STATE_TABLE = "_adapt_state"  # the state table before the StreamWright rename; read once, then superseded
_MISSING_CATALOG = "creating a new DuckLake is explicitly disabled"


def _key_value(alias, key, whole_record):
    if whole_record:
        return "json_extract_string(%s.%s, %s)" % (alias, _quote("record"), _sql_string(_json_path(key)))
    return "%s.%s" % (alias, _quote(key))


def _table_name(schema, name):
    return "%s.%s" % (_quote(schema), _quote(name))


class _WarehouseOutput(LoadingOutput):
    """Common staging and load logic for DuckDB and DuckLake."""

    kind = "warehouse"

    def __init__(self, path, schema, source):
        super().__init__(source)
        checked_name(schema, "schema")
        self.path = os.path.expanduser(path)
        self.schema = schema
        self.source_name = source["name"]
        # unkeyed exports that hold a window of their data are appended, the others replaced
        self.staging = Staging("streamwright-%s-" % self.kind)
        self.streams = self.staging.exports  # name -> dict(path, handle, schema, keys, columns, count, whole_record)
        self.status = None  # "loaded" once close() has loaded the run, "failed" if it loaded nothing

    def write_columns(self, name, columns):
        self.staging.write_columns(name, columns)

    def write_schema(self, name, schema, key_properties):
        self.staging.write_schema(name, schema, key_properties)

    def write_record(self, name, record):
        self.staging.write_record(name, record)

    def summary(self):
        """
        One entry per export (see streamwright.core.outputs.output): export, records, table (SCHEMA.EXPORT), disposition
        (merge, replace, append, or skipped: the table kept its rows), primary_key, and state
        (SCHEMA._streamwright_state) once the load saved it. No entries after a failed run or load: nothing was loaded.
        """
        if self.status == "failed":
            return []
        entries = []
        for name, stream in self.streams.items():
            entry = self._summary_entry(name, stream["count"], stream["keys"], "%s.%s" % (self.schema, name))
            if self.status == "loaded" and self.state is not None:
                entry["state"] = "%s.%s" % (self.schema, _STATE_TABLE)
            entries.append(entry)
        return entries

    def close(self, failed=False):
        self.status = "failed"
        try:
            self.staging.close()
            if failed:
                return
            self._load()
            self.status = "loaded"
        finally:
            self.staging.remove()
        log_summary(self.summary())

    def _connect(self, read_only=False):
        raise NotImplementedError

    def _destination(self):
        return self.path

    def _load(self):
        if not self.streams and self.state is None:
            return
        connection = self._connect()
        try:
            connection.execute("SET TimeZone = 'UTC'")
            connection.execute("BEGIN")
            try:
                connection.execute("CREATE SCHEMA IF NOT EXISTS %s" % _quote(self.schema))
                loaded = 0
                for index, (name, stream) in enumerate(self.streams.items()):
                    if self._load_table(connection, index, name, stream):
                        loaded += 1
                if self.state is not None:
                    self._save_state(connection)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
            LOG.debug("%s: loaded %d table(s) into %s, schema %r", self.kind, loaded, self._destination(),
                      self.schema)
        finally:
            connection.close()

    def _mask(self, exc):
        """`exc`, or (when its message shows a credential) an error with the credentials masked; see DuckLakeOutput."""
        return exc

    def _table_columns(self, connection, name):
        """The columns of the table `name` (name -> canonical type), or None when it does not exist."""
        # (looked up, not tried: a failed DESCRIBE aborts the transaction the load runs in)
        found = connection.execute("SELECT 1 FROM duckdb_tables() WHERE database_name = current_database() AND "
                                   "schema_name = ? AND table_name = ?", [self.schema, name]).fetchone()
        if found is None:
            return None
        rows = connection.execute("DESCRIBE %s" % _table_name(self.schema, name)).fetchall()
        return {row[0]: _canonical(row[1]) for row in rows}

    def _ensure_table(self, connection, name, columns):
        existing = self._table_columns(connection, name)
        target = {column: _canonical(kind) for column, kind in columns}
        if existing is None:
            connection.execute("CREATE TABLE IF NOT EXISTS %s (%s)" % (
                _table_name(self.schema, name), ", ".join("%s %s" % (_quote(column), kind)
                                                          for column, kind in columns)))
            return
        for column, kind in target.items():
            if column in existing and existing[column] != kind:
                raise ValueError("%s.%s column %r has type %s, but this run writes %s" % (
                    self.schema, name, column, existing[column], kind))
        for column, kind in columns:
            if column not in existing:
                connection.execute("ALTER TABLE %s ADD COLUMN %s %s" % (_table_name(self.schema, name),
                                                                        _quote(column), kind))

    def _stage(self, connection, index, stream, columns):
        wrapper = "_streamwright_wrapper_%d" % index
        rows = "_streamwright_rows_%d" % index
        connection.execute("DROP TABLE IF EXISTS %s" % _quote(wrapper))
        connection.execute("DROP TABLE IF EXISTS %s" % _quote(rows))
        if stream["count"] == 0:
            connection.execute("CREATE TEMP TABLE %s (%s)" % (
                _quote(rows), ", ".join(["_streamwright_ordinal BIGINT"] + ["%s %s" % (_quote(column), kind)
                                                                     for column, kind in columns])))
            return rows
        connection.execute("CREATE TEMP TABLE %s AS SELECT ordinal, record FROM %s" % (
            _quote(wrapper), staged_table(stream["path"])))
        select = typed_columns(columns, stream["whole_record"])
        statement = "CREATE TEMP TABLE %s AS SELECT %s FROM %s" % (_quote(rows), ", ".join(select), _quote(wrapper))
        connection.execute(statement)
        return rows

    def _dedupe(self, connection, index, rows, keys, whole_record=False):
        deduped = "_streamwright_dedup_%d" % index
        connection.execute("DROP TABLE IF EXISTS %s" % _quote(deduped))
        by = ", ".join(_key_value("rows", key, whole_record) for key in keys)
        connection.execute("CREATE TEMP TABLE %s AS SELECT * EXCLUDE (_streamwright_row_number) FROM ("
                           "SELECT *, row_number() OVER (PARTITION BY %s ORDER BY _streamwright_ordinal DESC) "
                           "AS _streamwright_row_number FROM %s AS rows) WHERE _streamwright_row_number = 1" % (
                               _quote(deduped), by, _quote(rows)))
        return deduped

    def _load_table(self, connection, index, name, stream):
        keys = stream["keys"]
        columns = self.staging.columns_of(name)
        disposition = self.disposition(name, keys)
        if self._kept(name, keys):
            LOG.warning("stream %r skipped partitions, so its table keeps the rows of the last complete run "
                        "(it has no primary_key to merge on)", name)
            return False
        self._ensure_table(connection, name, columns)
        rows = self._stage(connection, index, stream, columns)
        source = self._dedupe(connection, index, rows, keys, stream["whole_record"]) if keys else rows
        table = _table_name(self.schema, name)
        names = ", ".join(_quote(column) for column, _ in columns)
        if disposition == "merge":
            condition = " AND ".join("%s IS NOT DISTINCT FROM %s" % (
                _key_value("target", key, stream["whole_record"]), _key_value("source", key, stream["whole_record"]))
                for key in keys)
            connection.execute("DELETE FROM %s AS target USING %s AS source WHERE %s" % (
                table, _quote(source), condition))
        elif disposition == "replace":
            connection.execute("DELETE FROM %s" % table)
        else:
            LOG.warning("stream %r has no primary_key, so %s appends it: the days read again (lookback and today) "
                        "are loaded again", name, self.kind)
        connection.execute("INSERT INTO %s BY NAME SELECT %s FROM %s" % (table, names, _quote(source)))
        return True

    def _save_state(self, connection):
        table = _table_name(self.schema, _STATE_TABLE)
        connection.execute("CREATE TABLE IF NOT EXISTS %s (source VARCHAR, state JSON, "
                           "loaded_at TIMESTAMP WITH TIME ZONE)" % table)
        connection.execute("DELETE FROM %s WHERE source = $source" % table, {"source": self.source_name})
        connection.execute("INSERT INTO %s (source, state, loaded_at) VALUES ($source, $state::JSON, $loaded_at)" %
                           table, {"source": self.source_name, "state": json.dumps(self.state, default=to_text),
                                   "loaded_at": datetime.datetime.now(datetime.UTC)})

    def _state_connection(self):
        """A read-only connection to the destination, or None when it does not exist yet."""
        if not os.path.exists(self.path):
            return None
        return self._connect(read_only=True)

    def initial_state(self):
        connection = self._state_connection()
        if connection is None:
            return None
        try:
            connection.execute("SET TimeZone = 'UTC'")
            row = None
            for state_table in (_STATE_TABLE, _LEGACY_STATE_TABLE):
                try:
                    row = connection.execute("SELECT state FROM %s WHERE source = $source" % _table_name(
                        self.schema, state_table), {"source": self.source_name}).fetchone()
                except duckdb.CatalogException:
                    continue
                except Exception as exc:
                    masked = self._mask(exc)
                    if masked is exc:
                        raise
                    raise masked from None
                if row:
                    break
            if not row:
                return None
            return json.loads(row[0]) if isinstance(row[0], str) else row[0]
        finally:
            connection.close()


class DuckDBOutput(_WarehouseOutput):
    """Warehouse output that loads into a DuckDB database file."""

    kind = "duckdb"

    def __init__(self, path, schema, source):
        database = os.path.basename(os.path.expanduser(path))
        if os.path.splitext(database)[0].lower() == schema.lower():
            raise ValueError("schema %r has the name of the database file %s: DuckDB cannot tell them apart; "
                             "give the file another name or pass a schema (duckdb:%s:SCHEMA)" % (
                                 schema, database, path))
        super().__init__(path, schema, source)

    def _connect(self, read_only=False):
        return duckdb.connect(self.path, read_only=read_only)


class DuckLakeOutput(_WarehouseOutput):
    """
    Warehouse output that loads into a DuckLake catalog: a local file, or a Postgres database (`postgres:DSN`), with
    its data files in a local folder or on S3 (see the module's doc for the environment variables).
    """

    kind = "ducklake"

    def __init__(self, path, schema, source):
        postgres = is_postgres(path)
        self.settings = ducklake_settings(os.environ, path)
        super().__init__(path, schema, source)
        if postgres:
            self.path = path  # a connection string, not a file: no ~ to expand
        self.postgres = postgres

    def _secrets(self):
        s3 = self.settings["s3"] or {}
        return [str(s3[parameter]) for parameter in ("KEY_ID", "SECRET", "SESSION_TOKEN") if s3.get(parameter)] + [
            value for value in [self.settings["catalog_password"]] if value]

    def _mask(self, exc):
        """`exc`, or (when its message shows a credential) a RuntimeError with the credentials masked."""
        text = str(exc)
        masked = mask_values(text, self._secrets())
        return exc if masked == text else RuntimeError(masked)

    def _connect(self, read_only=False):
        connection = duckdb.connect(":memory:")
        try:
            load_ducklake(connection)
            s3 = self.settings["s3"]
            if s3 is not None:
                load_extension(connection, "httpfs")
                if s3:
                    connection.execute(*s3_secret_query(s3, self.settings["s3_scope"]))
            if self.postgres:
                load_extension(connection, "postgres")
                if self.settings["catalog_password"]:
                    connection.execute(*postgres_secret_query(self.settings["catalog_password"]))
            LOG.debug("ducklake: catalog %s, data path %s, s3 %s, data inlining row limit %s", self._destination(),
                      self.settings["data_path"] or "(DuckLake's)", "(not set up)" if s3 is None else ", ".join(
                          "%s=%s" % (key.lower(), "***" if key in S3_CREDENTIALS.values() else value)
                          for key, value in sorted(s3.items())) or "(no secret)",
                      "(DuckLake's)" if self.settings["inlining"] is None else self.settings["inlining"])
            connection.execute(attach_query(self.path, self.settings, read_only))
            connection.execute("USE %s" % _quote("streamwright_ducklake"))
            return connection
        except Exception as exc:
            connection.close()
            masked = self._mask(exc)
            if masked is exc:
                raise
            raise masked from None

    def _load(self):
        try:
            super()._load()
        except Exception as exc:
            masked = self._mask(exc)
            if masked is exc:
                raise
            raise masked from None

    def _state_connection(self):
        if not self.postgres:
            return super()._state_connection()
        try:
            return self._connect(read_only=True)
        except Exception as exc:
            if _MISSING_CATALOG in str(exc):  # no catalog in that database (schema) yet: the first load creates it
                return None
            raise
