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
The bookkeeping store (runtime.state): leases with epoch fencing, batches with a status machine, pinned manifests,
write receipts, a transactional outbox and accepted file-schema signatures.

One implementation over a DB-API connection factory and a Dialect (SQLite, PostgreSQL, DuckDB). How the connection is
opened is the engine's business (a connector adapter, a DuckDB session, a SQLite file); nothing here is ever a business
write target.

Every write carries its Lease and checks the epoch inside the same transaction (`FOR SHARE` on PostgreSQL blocks a
takeover until it commits), so a slow or partitioned worker cannot overwrite the state of the worker that took over.
Lease expiry is computed by the database clock. Batch sequences come from `job_sequences`, which only ever grows: a
purge never makes a sequence (a table-format transaction version, a batch id) come back.
"""

import json
import logging
from types import MappingProxyType

from streamwright.core.jobs.state.dialects import DEFAULT_SCHEMA
from streamwright.core.jobs.state.migrations import MIGRATIONS
from streamwright.core.jobs.state.protocol import (
    STATUSES,
    TERMINAL_STATUSES,
    LeaseHeldError,
    StaleLeaseError,
    StateError,
)
from streamwright.core.jobs.types import Lease

__all__ = ["SqlStateStore"]

LOG = logging.getLogger("streamwright.jobs.state")

_NEXT = MappingProxyType({"received": ("processing", "failed", "abandoned"),
                          "processing": ("committed", "failed", "abandoned"),
                          "committed": ("acked", "failed", "abandoned"),
                          "acked": ("completed", "failed", "abandoned"),
                          "failed": ("abandoned",),                       # back to work only through retry_batch
                          "completed": (), "abandoned": ()})
_TABLES = ("job_leases", "job_sequences", "batches", "output_receipts", "outbox_events", "schema_signatures",
           "store_metadata", "schema_migrations")


def _json_text(value):
    return json.dumps(value, sort_keys=True, default=str)


def _json_value(value):
    return json.loads(value) if isinstance(value, (str, bytes)) else value


class SqlStateStore:
    def __init__(self, connect, dialect, schema=DEFAULT_SCHEMA, catalog=None):
        """`connect()` returns a new DB-API connection (closed after each operation); `catalog` is DuckDB only."""
        self._connect, self._dialect = connect, dialect
        prefix, self._create_schema = dialect.namespace(schema, catalog)
        # constraint and index names use the physical table name (prefixed on SQLite, plain inside a schema)
        label_prefix = "" if prefix.endswith(".") else prefix
        self._labels = {name: label_prefix + name for name in _TABLES}
        tokens = {name: prefix + name for name in _TABLES}
        tokens.update(now=dialect.now, json=dialect.json_parameter, expires=dialect.expires_in("?"))
        self._tokens = tokens
        self._identity = None

    # --- plumbing -------------------------------------------------------------------------------------------

    def _sql(self, text):
        return self._dialect.placeholders(text.format(**self._tokens))

    def _x(self, conn, text, args=()):
        return conn.execute(self._sql(text), tuple(args))

    def _run(self, work):
        """
        One transaction. A failed commit propagates; a failing rollback or close never replaces the original error;
        a close that fails after a successful commit is reported too.
        """
        conn = self._connect()
        try:
            if self._dialect.begin:
                conn.execute(self._dialect.begin)
            result = work(conn)
            conn.commit()
        except BaseException as error:
            for cleanup in (conn.rollback, conn.close):
                try:
                    cleanup()
                except Exception as exc:
                    LOG.warning("state store %s failed after %s: %s", cleanup.__name__, type(error).__name__, exc)
            raise
        conn.close()
        return result

    def _check(self, conn, lease):
        row = self._x(conn, "SELECT lease_epoch FROM {job_leases} WHERE job_name = ?" + self._dialect.share_lock,
                      (lease.job_name,)).fetchone()
        if row is None or int(row[0]) != lease.epoch:
            raise StaleLeaseError("lease for %s is no longer held by %s (epoch %s)" % (
                lease.job_name, lease.owner_id, lease.epoch))

    def _lease_row(self, conn, job_name, lock=""):
        return self._x(conn, "SELECT owner_id, lease_epoch, expires_at, CASE WHEN expires_at > {now} THEN 1 ELSE 0 "
                             "END FROM {job_leases} WHERE job_name = ?" + lock, (job_name,)).fetchone()

    # --- schema ---------------------------------------------------------------------------------------------

    def initialize(self):
        dialect = self._dialect

        def work(conn):
            if self._create_schema:
                conn.execute(self._create_schema)
            self._x(conn, "CREATE TABLE IF NOT EXISTS {schema_migrations} (version BIGINT NOT NULL, applied_at %s "
                          "NOT NULL, CONSTRAINT pk_%s PRIMARY KEY (version))" % (
                              dialect.timestamp_type, self._labels["schema_migrations"]))
            applied = {int(r[0]) for r in self._x(conn, "SELECT version FROM {schema_migrations}").fetchall()}
            for version, build in MIGRATIONS:
                if version in applied:
                    continue
                for statement in build(dialect, self._tokens.__getitem__, self._labels.__getitem__):
                    conn.execute(statement)
                self._x(conn, "INSERT INTO {schema_migrations} (version, applied_at) VALUES (?, {now})", (version,))
        self._run(work)

    def store_identity(self):
        """This store's identity (seeded by the migration, persisted), unique across stores for idempotency keys."""
        if self._identity is None:
            self._identity = self._run(lambda conn: self._x(
                conn, "SELECT store_identity FROM {store_metadata} WHERE store_key = ?", ("default",)).fetchone()[0])
        return self._identity

    def schema_version(self):
        return self._run(lambda conn: self._x(conn, "SELECT MAX(version) FROM {schema_migrations}").fetchone()[0])

    # --- leases ---------------------------------------------------------------------------------------------

    def acquire_lease(self, job_name, owner_id, ttl_seconds):
        def work(conn):
            self._x(conn, "INSERT INTO {job_leases} (job_name, owner_id, lease_epoch, acquired_at, expires_at) "
                          "VALUES (?, ?, 0, {now}, NULL) ON CONFLICT (job_name) DO NOTHING", (job_name, owner_id))
            current, epoch, expires_at, held = self._lease_row(conn, job_name, self._dialect.update_lock)
            if int(held) and current != owner_id:
                raise LeaseHeldError("job %s is held by %s until %s" % (
                    job_name, current, self._dialect.read_timestamp(expires_at)))
            epoch = int(epoch) + 1
            self._x(conn, "UPDATE {job_leases} SET owner_id = ?, lease_epoch = ?, acquired_at = {now}, "
                          "expires_at = {expires} WHERE job_name = ?", (owner_id, epoch, int(ttl_seconds), job_name))
            row = self._lease_row(conn, job_name)
            return Lease(job_name, owner_id, epoch, self._dialect.read_timestamp(row[2]))
        return self._run(work)

    def renew_lease(self, lease, ttl_seconds):
        def work(conn):
            row = self._lease_row(conn, lease.job_name, self._dialect.update_lock)
            if row is None or int(row[1]) != lease.epoch:
                raise StaleLeaseError("lease for %s is no longer held by %s (epoch %s)" % (
                    lease.job_name, lease.owner_id, lease.epoch))
            self._x(conn, "UPDATE {job_leases} SET expires_at = {expires} WHERE job_name = ? AND lease_epoch = ?",
                    (int(ttl_seconds), lease.job_name, lease.epoch))
            return Lease(lease.job_name, lease.owner_id, lease.epoch,
                         self._dialect.read_timestamp(self._lease_row(conn, lease.job_name)[2]))
        return self._run(work)

    def check_lease(self, lease):
        def work(conn):
            row = self._lease_row(conn, lease.job_name, self._dialect.share_lock)
            if row is None or int(row[1]) != lease.epoch or not int(row[3]):
                raise StaleLeaseError("lease for %s (epoch %s) was taken over or has expired" % (
                    lease.job_name, lease.epoch))
        self._run(work)

    def release_lease(self, lease):
        self._run(lambda conn: self._x(conn, "UPDATE {job_leases} SET expires_at = NULL WHERE job_name = ? "
                                             "AND lease_epoch = ?", (lease.job_name, lease.epoch)))

    def get_lease(self, job_name):
        def work(conn):
            row = self._lease_row(conn, job_name)
            if row is None:
                return None
            return {"job_name": job_name, "owner_id": row[0], "epoch": int(row[1]),
                    "expires_at": self._dialect.read_timestamp(row[2]), "held": bool(int(row[3]))}
        return self._run(work)

    # --- batches --------------------------------------------------------------------------------------------

    def next_batch_sequence(self, job_name):
        def work(conn):
            row = self._x(conn, "SELECT batch_sequence FROM {job_sequences} WHERE job_name = ?", (job_name,)).fetchone()
            return int(row[0]) + 1 if row else 1
        return self._run(work)

    def record_batch(self, lease, batch_id, status, manifest, *, batch_sequence=None, run_key=None, error=None,
                     engine_metadata=None):
        if status not in STATUSES:
            raise StateError("unknown status %r" % (status,))
        metadata = None if engine_metadata is None else _json_text(engine_metadata)

        def work(conn):
            self._check(conn, lease)
            row = self._x(conn, "SELECT status FROM {batches} WHERE job_name = ? AND batch_id = ?",
                          (lease.job_name, batch_id)).fetchone()
            if row is None:
                self._insert_batch(conn, lease, batch_id, status, manifest, batch_sequence=batch_sequence,
                                   run_key=run_key, error=error, metadata=metadata)
                return
            self._move_batch(conn, lease, batch_id, row[0], status, manifest=manifest, error=error, metadata=metadata)
        self._run(work)

    def _insert_batch(self, conn, lease, batch_id, status, manifest, *, batch_sequence, run_key, error, metadata):
        if batch_sequence is None:
            raise StateError("a new batch needs a batch_sequence")
        if status not in ("received", "processing"):
            raise StateError("a new batch starts as received or processing, not %s" % status)
        high = self._x(conn, "SELECT batch_sequence FROM {job_sequences} WHERE job_name = ?",
                       (lease.job_name,)).fetchone()
        if high and int(batch_sequence) <= int(high[0]):
            raise StateError("batch sequence %s must exceed the last sequence %s of %s" % (
                batch_sequence, high[0], lease.job_name))
        self._x(conn, "INSERT INTO {batches} (job_name, batch_id, batch_sequence, run_key, status, failed_from, "
                      "manifest_json, error_message, engine_metadata, created_at, updated_at) "
                      "VALUES (?, ?, ?, ?, ?, NULL, {json}, ?, {json}, {now}, {now})",
                (lease.job_name, batch_id, int(batch_sequence), run_key, status, _json_text(manifest), error,
                 metadata))
        self._x(conn, "INSERT INTO {job_sequences} (job_name, batch_sequence) VALUES (?, ?) ON CONFLICT (job_name) "
                      "DO UPDATE SET batch_sequence = excluded.batch_sequence", (lease.job_name, int(batch_sequence)))

    def _move_batch(self, conn, lease, batch_id, current, status, *, manifest, error, metadata):
        if current in TERMINAL_STATUSES and status != current:
            raise StateError("batch %s is %s and cannot become %s" % (batch_id, current, status))
        if status != current and status not in _NEXT[current]:
            raise StateError("batch %s cannot go from %s to %s" % (batch_id, current, status))
        failed_from = current if status == "failed" and current != "failed" else None
        self._x(conn, "UPDATE {batches} SET status = ?, manifest_json = {json}, error_message = ?, "
                      "failed_from = COALESCE(?, failed_from), engine_metadata = COALESCE({json}, engine_metadata), "
                      "updated_at = {now} WHERE job_name = ? AND batch_id = ?",
                (status, _json_text(manifest), error, failed_from, metadata, lease.job_name, batch_id))

    def record_receipt(self, lease, batch_id, receipt, manifest=None):
        def work(conn):
            self._check(conn, lease)
            self._x(conn, "INSERT INTO {output_receipts} (job_name, batch_id, output_name, receipt_json, recorded_at) "
                          "VALUES (?, ?, ?, {json}, {now}) ON CONFLICT (job_name, batch_id, output_name) "
                          "DO UPDATE SET receipt_json = excluded.receipt_json, recorded_at = excluded.recorded_at",
                    (lease.job_name, batch_id, receipt.output_name, _json_text(receipt.to_dict())))
            if manifest is not None:
                self._x(conn, "UPDATE {batches} SET manifest_json = {json}, updated_at = {now} "
                              "WHERE job_name = ? AND batch_id = ?", (_json_text(manifest), lease.job_name, batch_id))
        self._run(work)

    def commit_batch(self, lease, batch_id, manifest, events, schema_signatures=()):
        for output_name, (destination, _) in events.items():
            if not destination:
                raise StateError("output %s has no event destination" % output_name)

        def work(conn):
            self._check(conn, lease)
            row = self._x(conn, "SELECT status FROM {batches} WHERE job_name = ? AND batch_id = ?",
                          (lease.job_name, batch_id)).fetchone()
            if row is None:
                raise StateError("batch %s does not exist" % batch_id)
            self._move_batch(conn, lease, batch_id, row[0], "committed", manifest=manifest, error=None, metadata=None)
            for output_name, (destination, rows) in sorted(events.items()):
                for number, payload in enumerate(rows):
                    self._x(conn, "INSERT INTO {outbox_events} (job_name, batch_id, output_name, event_sequence, "
                                  "destination, payload_json, status, created_at, sent_at) "
                                  "VALUES (?, ?, ?, ?, ?, {json}, 'pending', {now}, NULL) "
                                  "ON CONFLICT (job_name, batch_id, output_name, event_sequence) DO NOTHING",
                            (lease.job_name, batch_id, output_name, number, destination, _json_text(payload)))
            self._store_signatures(conn, lease, schema_signatures)
        self._run(work)

    def retry_batch(self, lease, batch_id):
        def work(conn):
            self._check(conn, lease)
            row = self._x(conn, "SELECT status, failed_from FROM {batches} WHERE job_name = ? AND batch_id = ?",
                          (lease.job_name, batch_id)).fetchone()
            if row is None or row[0] != "failed":
                raise StateError("batch %s is not failed" % batch_id)
            if not row[1]:
                raise StateError("batch %s has no status to resume from" % batch_id)
            self._x(conn, "UPDATE {batches} SET status = ?, failed_from = NULL, error_message = NULL, "
                          "updated_at = {now} WHERE job_name = ? AND batch_id = ?", (row[1], lease.job_name, batch_id))
            return row[1]
        return self._run(work)

    def get_receipts(self, job_name, batch_id):
        def work(conn):
            rows = self._x(conn, "SELECT output_name, receipt_json FROM {output_receipts} WHERE job_name = ? "
                                 "AND batch_id = ? ORDER BY output_name", (job_name, batch_id)).fetchall()
            return {r[0]: _json_value(r[1]) for r in rows}
        return self._run(work)

    _BATCH_COLUMNS = ("batch_id, batch_sequence, run_key, status, failed_from, manifest_json, error_message, "
                      "engine_metadata, created_at, updated_at")

    def _batch(self, row):
        read = self._dialect.read_timestamp
        return {"batch_id": row[0], "batch_sequence": int(row[1]), "run_key": row[2], "status": row[3],
                "failed_from": row[4], "manifest": _json_value(row[5]), "error": row[6],
                "engine_metadata": _json_value(row[7]) if row[7] is not None else None,
                "created_at": read(row[8]), "updated_at": read(row[9])}

    def _batches(self, where, args, suffix=""):
        def work(conn):
            rows = self._x(conn, "SELECT %s FROM {batches} WHERE %s%s" % (self._BATCH_COLUMNS, where, suffix),
                           args).fetchall()
            return [self._batch(row) for row in rows]
        return self._run(work)

    def get_batch(self, job_name, batch_id):
        found = self._batches("job_name = ? AND batch_id = ?", (job_name, batch_id))
        return found[0] if found else None

    def find_batch_by_run_key(self, job_name, run_key):
        found = self._batches("job_name = ? AND run_key = ?", (job_name, run_key))
        return found[0] if found else None

    def find_latest_completed_batch(self, job_name):
        found = self._batches("job_name = ? AND status = 'completed'", (job_name,),
                              " ORDER BY batch_sequence DESC LIMIT 1")
        return found[0] if found else None

    def list_incomplete_batches(self, job_name):
        return self._batches("job_name = ? AND status NOT IN ('completed', 'abandoned')", (job_name,),
                             " ORDER BY batch_sequence")

    def list_batches(self, job_name, limit=20):
        return self._batches("job_name = ?", (job_name, int(limit)), " ORDER BY batch_sequence DESC LIMIT ?")

    # --- outbox ---------------------------------------------------------------------------------------------

    def list_pending_events(self, job_name):
        def work(conn):
            rows = self._x(conn, "SELECT batch_id, output_name, event_sequence, destination, payload_json "
                                 "FROM {outbox_events} WHERE job_name = ? AND status = 'pending' "
                                 "ORDER BY batch_id, output_name, event_sequence", (job_name,)).fetchall()
            return [{"batch_id": r[0], "output_name": r[1], "event_sequence": int(r[2]), "destination": r[3],
                     "payload": _json_value(r[4])} for r in rows]
        return self._run(work)

    def mark_events_sent(self, lease, batch_id, output_name, event_sequences):
        def work(conn):
            self._check(conn, lease)
            for number in event_sequences:
                self._x(conn, "UPDATE {outbox_events} SET status = 'sent', sent_at = {now} WHERE job_name = ? "
                              "AND batch_id = ? AND output_name = ? AND event_sequence = ?",
                        (lease.job_name, batch_id, output_name, int(number)))
        self._run(work)

    def purge_batches(self, job_name, older_than):
        cutoff = self._dialect.timestamp_parameter(older_than)

        def work(conn):
            ids = [r[0] for r in self._x(conn, "SELECT batch_id FROM {batches} WHERE job_name = ? AND status IN "
                                               "('completed', 'abandoned') AND updated_at < ?",
                                         (job_name, cutoff)).fetchall()]
            for batch_id in ids:
                self._x(conn, "DELETE FROM {output_receipts} WHERE job_name = ? AND batch_id = ?", (job_name, batch_id))
                self._x(conn, "DELETE FROM {outbox_events} WHERE job_name = ? AND batch_id = ? AND status = 'sent'",
                        (job_name, batch_id))
                self._x(conn, "DELETE FROM {batches} WHERE job_name = ? AND batch_id = ?", (job_name, batch_id))
            return len(ids)
        return self._run(work)

    # --- file schema signatures -----------------------------------------------------------------------------

    def get_schema_signatures(self, job_name):
        def work(conn):
            rows = self._x(conn, "SELECT path_prefix, header_hash FROM {schema_signatures} WHERE job_name = ?",
                           (job_name,)).fetchall()
            return {r[0]: r[1] for r in rows}
        return self._run(work)

    def _store_signatures(self, conn, lease, signatures):
        for prefix, header_hash, signature in signatures:
            self._x(conn, "INSERT INTO {schema_signatures} (job_name, path_prefix, header_hash, signature, "
                          "updated_at) VALUES (?, ?, ?, ?, {now}) ON CONFLICT (job_name, path_prefix) DO UPDATE "
                          "SET header_hash = excluded.header_hash, signature = excluded.signature, "
                          "updated_at = excluded.updated_at", (lease.job_name, prefix, header_hash, signature))
