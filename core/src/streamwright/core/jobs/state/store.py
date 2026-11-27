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
The StreamWright bookkeeping state store (runtime.state), on the `sw_*` tables:

- sw_leases: fencing leases, one row per lock token (the job's scope token and any `runtime.state.lock` tokens),
  each with the epoch of the run that holds it and a database-clock expiry.
- sw_jobs: the runs (batches) of each job and scope: lifecycle, pinned manifest, lock tokens, metrics.
- sw_sequences: the run counter of each job and scope; Delta transaction versions come from it, so it never goes
  back, even when runs are purged.
- sw_receipts: output write receipts; sw_events: the transactional outbox; sw_signatures: input schema hashes.
- sw_migrations: the applied migrations and the store's persistent identity.

Every write runs in one transaction and is fenced: it is rejected (StaleLeaseError) once any token of the writer's
lease has a newer epoch. The Python API speaks of batches (batch_id, manifest); the columns are job_id,
checkpoint and job_key.
"""

import datetime
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any

from streamwright.core.jobs.state.events import EventOperations
from streamwright.core.jobs.state.leases import LOCK_POLICIES, LeaseOperations
from streamwright.core.jobs.state.protocol import STATUSES, TERMINAL_STATUSES, StateError
from streamwright.core.jobs.state.requests import RequestOperations
from streamwright.core.jobs.state.sql import json_text as _json_text
from streamwright.core.jobs.state.sql import json_value as _json_value
from streamwright.core.jobs.types import Lease, WriteReceipt

__all__ = ["LOCK_POLICIES", "SqlStateStore"]

_NEXT = MappingProxyType({
    "received": ("processing", "failed", "abandoned"),
    "processing": ("committed", "failed", "abandoned"),
    "committed": ("acked", "failed", "abandoned"),
    "acked": ("completed", "failed", "abandoned"),
    "failed": ("abandoned",),  # back to work only through retry_batch
    "completed": (),
    "abandoned": (),
    "skipped": (),
})
_BATCH_COLUMNS = ("job_id, scope, sequence, job_key, status, tokens, checkpoint, failed_from, started_at, "
                  "finished_at, duration_ms, rows_written, error_message, metadata, created_at, updated_at, attempts")


def _parse_time(text: str | None) -> datetime.datetime | None:
    if not text:
        return None
    return datetime.datetime.fromisoformat(str(text).replace("Z", "+00:00"))


class SqlStateStore(EventOperations, LeaseOperations, RequestOperations):
    """ACID state store for StreamWright jobs: leases, runs, receipts, outbox, signatures and the serve run queue."""

    # --- sequences (sw_sequences) ---------------------------------------------------------------------------

    def _next_sequence(self, conn: Any, job_name: str, scope: str) -> int:
        self._x(conn, "INSERT INTO {sw_sequences} (job_name, scope, last_sequence, created_at, updated_at) "
                      "VALUES (?, ?, 0, {now}, {now}) " + self._dialect.conflict_ignore(("job_name", "scope")),
                (job_name, scope))
        self._x(conn, "UPDATE {sw_sequences} SET last_sequence = last_sequence + 1, updated_at = {now} "
                      "WHERE job_name = ? AND scope = ?", (job_name, scope))
        return int(self._x(conn, "SELECT last_sequence FROM {sw_sequences} WHERE job_name = ? AND scope = ?",
                           (job_name, scope)).fetchone()[0])

    def next_batch_sequence(self, lease: Lease, scope: str = "") -> int:
        """
        Issues the next run sequence of the job and scope (fenced): one more than any issued before, never
        reused, even after purges. Delta outputs use it as the transaction version.
        """
        def work(conn):
            self._check(conn, lease)
            return self._next_sequence(conn, lease.job_name, scope)

        return self._run(work)

    # --- batches (sw_jobs) ----------------------------------------------------------------------------------

    def record_batch(self, lease: Lease, batch_id: str, status: str, manifest: Mapping[str, Any], *,
                     batch_sequence: int | None = None, scope: str = "", job_key: str | None = None,
                     tokens: Mapping[str, Any] | None = None, error: str | None = None,
                     engine_metadata: Mapping[str, Any] | None = None) -> None:
        """
        Creates a batch (received or processing; needs batch_sequence from next_batch_sequence) or moves it along
        the status machine, storing its manifest; fenced by the lease. `tokens` defaults to the lease's.
        """
        if status not in STATUSES:
            raise StateError("unknown status %r" % (status,))

        def work(conn):
            self._check(conn, lease)
            row = self._x(conn, "SELECT status FROM {sw_jobs} WHERE job_name = ? AND job_id = ?",
                          (lease.job_name, batch_id)).fetchone()
            if row is not None:
                self._move(conn, lease.job_name, batch_id, row[0], status, manifest=manifest, error=error,
                           engine_metadata=engine_metadata)
                return
            if status not in ("received", "processing"):
                raise StateError("a new batch starts as received or processing, not %s" % status)
            if batch_sequence is None:
                raise StateError("a new batch needs its batch_sequence")
            admitted = tokens if tokens is not None else {
                "accepted": [token for token in lease.tokens if token], "skipped": list(lease.pruned_tokens)}
            self._insert(conn, lease.job_name, batch_id, scope=scope, sequence=batch_sequence, job_key=job_key,
                         status=status, tokens=admitted, manifest=manifest, error=error,
                         engine_metadata=engine_metadata)

        self._run(work)

    def record_skipped(self, job_name: str, batch_id: str, manifest: Mapping[str, Any], *, scope: str = "",
                       tokens: Mapping[str, Any] | None = None) -> int:
        """
        Records a run that did nothing because the lock policy pruned all it needed (status skipped); not fenced,
        as the run holds no lease. Returns its sequence.
        """
        def work(conn):
            sequence = self._next_sequence(conn, job_name, scope)
            self._insert(conn, job_name, batch_id, scope=scope, sequence=sequence, job_key=None, status="skipped",
                         tokens=tokens or {}, manifest=manifest)
            self._finish(conn, job_name, batch_id)
            return sequence

        return self._run(work)

    def _insert(self, conn: Any, job_name: str, batch_id: str, *, scope: str, sequence: int,
                job_key: str | None, status: str, tokens: Mapping[str, Any], manifest: Mapping[str, Any],
                error: str | None = None, engine_metadata: Mapping[str, Any] | None = None) -> None:
        self._x(conn, "INSERT INTO {sw_jobs} (job_name, job_id, scope, sequence, job_key, status, tokens, checkpoint, "
                      "failed_from, started_at, finished_at, duration_ms, rows_written, error_message, metadata, "
                      "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, {json}, {json}, NULL, {now}, NULL, NULL, 0, "
                      "?, {json}, {now}, {now})",
                (job_name, batch_id, scope, int(sequence), job_key, status, _json_text(tokens),
                 _json_text(manifest), error, None if engine_metadata is None else _json_text(engine_metadata)))

    def _move(self, conn: Any, job_name: str, batch_id: str, current: str, status: str, *,
              manifest: Mapping[str, Any], error: str | None = None,
              engine_metadata: Mapping[str, Any] | None = None) -> None:
        if status != current and status not in _NEXT[current]:
            raise StateError("batch %s cannot go from %s to %s" % (batch_id, current, status))
        failed_from = current if status == "failed" and current != "failed" else None
        self._x(conn, "UPDATE {sw_jobs} SET status = ?, checkpoint = {json}, error_message = ?, "
                      "failed_from = COALESCE(?, failed_from), metadata = COALESCE({json}, metadata), "
                      "rows_written = COALESCE((SELECT SUM(r.rows_written) FROM {sw_receipts} r "
                      "WHERE r.job_name = ? AND r.job_id = ?), 0), updated_at = {now} "
                      "WHERE job_name = ? AND job_id = ?",
                (status, _json_text(manifest), error, failed_from,
                 None if engine_metadata is None else _json_text(engine_metadata), job_name, batch_id, job_name,
                 batch_id))
        if status in TERMINAL_STATUSES and status != current:
            self._finish(conn, job_name, batch_id)

    def _finish(self, conn: Any, job_name: str, batch_id: str) -> None:
        """Stamps finished_at (database clock) and duration_ms of a batch reaching a terminal status."""
        self._x(conn, "UPDATE {sw_jobs} SET finished_at = {now} WHERE job_name = ? AND job_id = ?",
                (job_name, batch_id))
        started, finished = self._x(conn, "SELECT started_at, finished_at FROM {sw_jobs} WHERE job_name = ? AND "
                                          "job_id = ?", (job_name, batch_id)).fetchone()
        started, finished = (_parse_time(self._dialect.read_timestamp(value)) for value in (started, finished))
        duration = max(0, round((finished - started).total_seconds() * 1000))
        self._x(conn, "UPDATE {sw_jobs} SET duration_ms = ? WHERE job_name = ? AND job_id = ?",
                (duration, job_name, batch_id))

    def retry_batch(self, lease: Lease, batch_id: str, *, resend: Sequence[str] | None = None) -> str:
        """
        Moves a failed batch back to the status it failed from, counting one more attempt; returns that status. In the
        same transaction its failed apply rows of the `resend` outputs (None: every output) go back to pending, so the
        relay sends them again; they keep their attempts and last error until then. Applied rows, in-flight rows and
        rows the operator skipped (UNKNOWN_OUTCOME: they may have been applied) are left as they are.
        """
        def work(conn):
            self._check(conn, lease)
            row = self._x(conn, "SELECT status, failed_from FROM {sw_jobs} WHERE job_name = ? AND job_id = ?",
                          (lease.job_name, batch_id)).fetchone()
            if row is None or row[0] != "failed":
                raise StateError("batch %s is not failed" % batch_id)
            if not row[1]:
                raise StateError("batch %s has no status to resume from" % batch_id)
            self._x(conn, "UPDATE {sw_jobs} SET status = ?, failed_from = NULL, error_message = NULL, "
                          "attempts = attempts + 1, updated_at = {now} WHERE job_name = ? AND job_id = ?",
                    (row[1], lease.job_name, batch_id))
            self._resend_failed(conn, lease.job_name, batch_id, resend)
            return row[1]

        return self._run(work)

    # --- batch queries --------------------------------------------------------------------------------------

    def _batch(self, row: Sequence[Any]) -> dict[str, Any]:
        read = self._dialect.read_timestamp
        return {
            "batch_id": row[0], "scope": row[1] or "", "batch_sequence": int(row[2]), "job_key": row[3],
            "status": row[4], "tokens": _json_value(row[5]) if row[5] is not None else None,
            "manifest": _json_value(row[6]), "failed_from": row[7], "started_at": read(row[8]),
            "finished_at": read(row[9]), "duration_ms": None if row[10] is None else int(row[10]),
            "rows_written": int(row[11] or 0), "error": row[12],
            "engine_metadata": _json_value(row[13]) if row[13] is not None else None,
            "created_at": read(row[14]), "updated_at": read(row[15]), "attempts": int(row[16] or 1),
        }

    def _batches(self, where: str, args: Sequence[Any], suffix: str = "") -> list[dict[str, Any]]:
        rows = self._run(lambda conn: self._x(
            conn, "SELECT " + _BATCH_COLUMNS + " FROM {sw_jobs} WHERE " + where + suffix, args).fetchall())
        return [self._batch(row) for row in rows]

    def get_batch(self, job_name: str, batch_id: str) -> dict[str, Any] | None:
        found = self._batches("job_name = ? AND job_id = ?", (job_name, batch_id))
        return found[0] if found else None

    def find_batch_by_job_key(self, job_name: str, job_key: str, scope: str = "") -> dict[str, Any] | None:
        found = self._batches("job_name = ? AND scope = ? AND job_key = ?", (job_name, scope, job_key))
        return found[0] if found else None

    def find_latest_completed_batch(self, job_name: str, scope: str = "") -> dict[str, Any] | None:
        found = self._batches("job_name = ? AND scope = ? AND status = 'completed'", (job_name, scope),
                              " ORDER BY sequence DESC LIMIT 1")
        return found[0] if found else None

    def list_incomplete_batches(self, job_name: str, scope: str = "") -> list[dict[str, Any]]:
        """The scope's batches not yet completed, abandoned or skipped, oldest first."""
        return self._batches("job_name = ? AND scope = ? AND status NOT IN (%s)" % self._marks(TERMINAL_STATUSES),
                             (job_name, scope, *TERMINAL_STATUSES), " ORDER BY sequence")

    def list_batches(self, job_name: str, limit: int = 20, scope: str | None = None) -> list[dict[str, Any]]:
        """The newest batches of the job (of one scope, or all when scope is None), newest first."""
        where, args = ("job_name = ?", [job_name]) if scope is None else ("job_name = ? AND scope = ?",
                                                                          [job_name, scope])
        return self._batches(where, (*args, int(limit)), " ORDER BY created_at DESC, sequence DESC LIMIT ?")

    def find_batches(self, *, job_name: str | None = None, batch_id: str | None = None, scope: str | None = None,
                     status: str | None = None, request_id: str | None = None,
                     limit: int = 20) -> list[dict[str, Any]]:
        """
        The newest batches of any job (read-only, for `streamwright state list|show --url`): by job name, job id, scope,
        status or the request id of the run that made them; each with its `job_name`.
        """
        where, args = ["1 = 1"], []
        for column, value in (("job_name", job_name), ("job_id", batch_id), ("scope", scope), ("status", status),
                              (self._dialect.json_text("metadata", "request_id"), request_id)):
            if value is not None:
                where.append("%s = ?" % column)
                args.append(value)
        rows = self._run(lambda conn: self._x(
            conn, "SELECT job_name, " + _BATCH_COLUMNS + " FROM {sw_jobs} WHERE " + " AND ".join(where) +
            " ORDER BY created_at DESC, sequence DESC LIMIT ?", (*args, int(limit))).fetchall())
        return [dict(self._batch(row[1:]), job_name=row[0]) for row in rows]

    def purge_batches(self, job_name: str, older_than: Any, scope: str | None = None, *, chunk: int = 1000) -> int:
        """
        Deletes the completed, abandoned and skipped batches last updated before `older_than` (an aware datetime),
        with their receipts and outbox rows (events and apply rows, whatever their status), `chunk` batches per
        transaction; sequences are kept.
        """
        cutoff = self._dialect.timestamp_parameter(older_than)
        where = "job_name = ? AND status IN (%s) AND updated_at < ?" % self._marks(TERMINAL_STATUSES)
        args = [job_name, *TERMINAL_STATUSES, cutoff]
        if scope is not None:
            where, args = where + " AND scope = ?", [*args, scope]

        def work(conn):
            ids = [row[0] for row in self._x(conn, "SELECT job_id FROM {sw_jobs} WHERE " + where +
                                             " LIMIT %d" % int(chunk), args).fetchall()]
            for table in ("sw_receipts", "sw_events", "sw_jobs"):
                if ids:
                    self._x(conn, "DELETE FROM {%s} WHERE job_name = ? AND job_id IN (%s)" % (
                        table, self._marks(ids)), (job_name, *ids))
            return len(ids)

        total = 0
        while True:
            deleted = self._run(work)
            total += deleted
            if deleted < chunk:
                return total

    # --- receipts (sw_receipts) -----------------------------------------------------------------------------

    def record_receipt(self, lease: Lease, batch_id: str, receipt: WriteReceipt,
                       manifest: Mapping[str, Any] | None = None) -> None:
        """Stores an output's receipt (and the batch manifest, in the same transaction); fenced."""
        details = receipt.to_dict()

        def work(conn):
            self._check(conn, lease)
            self._x(conn, "INSERT INTO {sw_receipts} (job_name, job_id, target, rows_written, destination, files, "
                          "details, committed_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, {json}, {json}, "
                          "{now}, {now}, {now}) " + self._dialect.conflict_update(
                              ("job_name", "job_id", "target"),
                              ("rows_written", "destination", "files", "details", "committed_at", "updated_at")),
                    (lease.job_name, batch_id, receipt.output_name, receipt.rows, receipt.destination,
                     _json_text(list(receipt.files)), _json_text(details)))
            if manifest is not None:
                self._x(conn, "UPDATE {sw_jobs} SET checkpoint = {json}, updated_at = {now} "
                              "WHERE job_name = ? AND job_id = ?", (_json_text(manifest), lease.job_name, batch_id))

        self._run(work)

    def get_receipts(self, job_name: str, batch_id: str) -> dict[str, dict[str, Any]]:
        rows = self._run(lambda conn: self._x(
            conn, "SELECT target, details FROM {sw_receipts} WHERE job_name = ? AND job_id = ? ORDER BY target",
            (job_name, batch_id)).fetchall())
        return {row[0]: _json_value(row[1]) for row in rows}

    # --- commit & outbox (sw_events) ------------------------------------------------------------------------

    def commit_batch(self, lease: Lease, batch_id: str, manifest: Mapping[str, Any],
                     events: Mapping[str, tuple[str, Sequence[Mapping[str, Any]]]],
                     schema_signatures: Sequence[tuple[str, str, str]] = ()) -> None:
        """
        Marks the batch committed and stores its outbox events ({output: (destination, rows)}) and the accepted
        schema signatures ([(path prefix, header hash, signature)]) atomically.
        """
        for output, (destination, _) in events.items():
            if not destination:
                raise StateError("output %s has no event destination" % output)

        def work(conn):
            self._check(conn, lease)
            row = self._x(conn, "SELECT status FROM {sw_jobs} WHERE job_name = ? AND job_id = ?",
                          (lease.job_name, batch_id)).fetchone()
            if row is None:
                raise StateError("batch %s does not exist" % batch_id)
            self._move(conn, lease.job_name, batch_id, row[0], "committed", manifest=manifest)
            # a batch's outbox can be large (an apply output's rows): multi-row INSERTs, not a statement per row
            self._insert_rows(
                conn, "INSERT INTO {sw_events} (job_name, job_id, target, sequence, destination, payload, status, "
                      "sent_at, created_at, updated_at)", "(?, ?, ?, ?, ?, {json}, 'pending', NULL, {now}, {now})",
                [(lease.job_name, batch_id, output, number, destination, _json_text(payload))
                 for output, (destination, rows) in sorted(events.items()) for number, payload in enumerate(rows)],
                self._dialect.conflict_ignore(("job_name", "job_id", "target", "sequence")))
            # the last signature of a path wins, as it would a statement at a time (an upsert takes a key once)
            latest = {path: (header_hash, signature) for path, header_hash, signature in schema_signatures}
            self._insert_rows(
                conn, "INSERT INTO {sw_signatures} (job_name, path, hash, signature, created_at, updated_at)",
                "(?, ?, ?, ?, {now}, {now})",
                [(lease.job_name, path, header_hash, signature) for path, (header_hash, signature) in latest.items()],
                self._dialect.conflict_update(("job_name", "path"), ("hash", "signature", "updated_at")))

        self._run(work)

    # --- schema signatures (sw_signatures) ------------------------------------------------------------------

    def get_schema_signatures(self, job_name: str) -> dict[str, str]:
        rows = self._run(lambda conn: self._x(
            conn, "SELECT path, hash FROM {sw_signatures} WHERE job_name = ?", (job_name,)).fetchall())
        return {row[0]: row[1] for row in rows}
