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
Migration 0001: the sw_* state tables. A database that holds the tables of the store before them (batches,
job_leases, job_sequences, output_receipts, outbox_events, schema_signatures, store_metadata) has them copied in,
keeping the store identity, so batches, job keys (the old run_key), carried values, sequences and Delta idempotency
keys carry over.
The old tables are left in place.
"""

import json
import logging
import uuid

from sqlalchemy.schema import CreateIndex, CreateTable, SetColumnComment, SetTableComment

from streamwright.core.jobs.state.tables import get_tables, sqlalchemy_dialect

VERSION = 1
NAME = "0001_initial_schema"
DESCRIPTION = ("Create the state tables (sw_leases, sw_jobs, sw_sequences, sw_receipts, sw_events, sw_signatures, "
               "sw_migrations), importing the tables of the earlier store when present")

LOG = logging.getLogger("streamwright.jobs.state.migrations")

TABLE_ORDER = ("sw_migrations", "sw_leases", "sw_jobs", "sw_sequences", "sw_receipts", "sw_events", "sw_signatures")
_TERMINAL = ("completed", "abandoned")


def get_statements(dialect_name, schema=None):
    """The ordered DDL statements of this migration."""
    compiler = sqlalchemy_dialect(dialect_name)
    tables = get_tables(schema=schema)
    statements = []
    for name in TABLE_ORDER:
        table = tables[name]
        statements.append(str(CreateTable(table, if_not_exists=True).compile(dialect=compiler)).strip())
        # MySQL has no CREATE INDEX IF NOT EXISTS: apply() skips an index that exists (error 1061)
        statements.extend(str(CreateIndex(index, if_not_exists=dialect_name != "mysql").compile(
            dialect=compiler)).strip() for index in table.indexes)
        if dialect_name in ("postgres", "duckdb"):  # COMMENT ON
            if table.comment:
                statements.append(str(SetTableComment(table).compile(dialect=compiler)).strip())
            statements.extend(str(SetColumnComment(column).compile(dialect=compiler)).strip()
                              for column in table.columns if column.comment)
    return statements


def apply(conn, dialect, schema=None, legacy_prefix=""):
    """Creates the tables, imports the earlier store's tables when present; returns the store identity."""
    for statement in get_statements(dialect.name, schema=schema):
        try:
            conn.execute(statement)
        except Exception as exc:
            # MySQL: an index left by an interrupted run already exists
            if dialect.name == "mysql" and statement.startswith("CREATE INDEX") and exc.args[:1] == (1061,):
                continue
            raise
    target = "" if dialect.name == "sqlite" else legacy_prefix
    return _import_legacy(conn, dialect, schema, legacy_prefix, target) or str(uuid.uuid4())


def _legacy_exists(conn, dialect, schema, prefix, name):
    if dialect.name == "sqlite":
        query, args = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (prefix + name,)
    else:
        query = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"
        if dialect.name in ("postgres", "duckdb"):
            query += " AND table_catalog = current_database()"
        args = (schema, name)
    return conn.execute(dialect.placeholders(query), args).fetchone() is not None


def _json(value):
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    return json.loads(value) if isinstance(value, str) else value


def _text(value):
    return json.dumps(value, sort_keys=True, default=str)


def _import_legacy(conn, dialect, schema, prefix, target):
    """Copies the earlier store's rows into the sw_* tables; returns its store identity, or None without one."""
    if not _legacy_exists(conn, dialect, schema, prefix, "batches"):
        return None
    now, js = dialect.now, dialect.json_parameter

    def rows(name, columns):
        if not _legacy_exists(conn, dialect, schema, prefix, name):
            return []
        return conn.execute("SELECT %s FROM %s%s" % (columns, prefix, name)).fetchall()

    def insert(name, columns, values, args):
        conn.execute(dialect.placeholders("INSERT INTO %s%s (%s) VALUES (%s)" % (target, name, columns, values)),
                     tuple(args))

    identity = rows("store_metadata", "store_identity")
    for job_name, owner, epoch, acquired, expires in rows(
            "job_leases", "job_name, owner_id, lease_epoch, acquired_at, expires_at"):
        insert("sw_leases", "job_name, token, worker_id, epoch, acquired_at, expires_at, created_at, updated_at",
               "?, '', ?, ?, ?, ?, ?, %s" % now, (job_name, owner, int(epoch), acquired, expires, acquired))
    for job_name, sequence in rows("job_sequences", "job_name, batch_sequence"):
        insert("sw_sequences", "job_name, scope, last_sequence, created_at, updated_at",
               "?, '', ?, %s, %s" % (now, now), (job_name, int(sequence)))
    written = {}
    for job_name, batch_id, output, text, recorded in rows(
            "output_receipts", "job_name, batch_id, output_name, receipt_json, recorded_at"):
        receipt = _json(text)
        written[(job_name, batch_id)] = written.get((job_name, batch_id), 0) + int(receipt.get("rows") or 0)
        insert("sw_receipts", "job_name, job_id, target, rows_written, destination, files, details, committed_at, "
               "created_at, updated_at", "?, ?, ?, ?, ?, %s, %s, ?, ?, ?" % (js, js),
               (job_name, batch_id, output, receipt.get("rows"), receipt.get("destination") or "",
                _text(receipt.get("files") or []), _text(receipt), recorded, recorded, recorded))
    batches = rows("batches", "job_name, batch_id, batch_sequence, run_key, status, failed_from, manifest_json, "
                              "error_message, engine_metadata, created_at, updated_at")
    for row in batches:
        job_name, batch_id, sequence, job_key, status, failed_from, manifest, error, engine, created, updated = row
        insert("sw_jobs", "job_name, job_id, scope, sequence, job_key, status, tokens, checkpoint, failed_from, "
               "started_at, finished_at, duration_ms, rows_written, error_message, metadata, created_at, updated_at",
               "?, ?, '', ?, ?, ?, NULL, %s, ?, ?, ?, NULL, ?, ?, %s, ?, ?" % (js, js),
               (job_name, batch_id, int(sequence), job_key, status, _text(_json(manifest)), failed_from, created,
                updated if status in _TERMINAL else None, written.get((job_name, batch_id), 0), error,
                None if engine is None else _text(_json(engine)), created, updated))
    for job_name, batch_id, output, number, destination, payload, status, created, sent in rows(
            "outbox_events", "job_name, batch_id, output_name, event_sequence, destination, payload_json, status, "
                             "created_at, sent_at"):
        insert("sw_events", "job_name, job_id, target, sequence, destination, payload, status, sent_at, created_at, "
               "updated_at", "?, ?, ?, ?, ?, %s, ?, ?, ?, ?" % js,
               (job_name, batch_id, output, int(number), destination, _text(_json(payload)), status, sent, created,
                sent or created))
    for job_name, path, header_hash, signature, updated in rows(
            "schema_signatures", "job_name, path_prefix, header_hash, signature, updated_at"):
        insert("sw_signatures", "job_name, path, hash, signature, created_at, updated_at", "?, ?, ?, ?, ?, ?",
               (job_name, path, header_hash, signature, updated, updated))
    LOG.warning("state: imported %d batches of the earlier store (tables %sbatches, ...); they are kept, and can be "
                "dropped once no engine of the earlier release uses this store", len(batches), prefix)
    return str(identity[0][0]) if identity else None
