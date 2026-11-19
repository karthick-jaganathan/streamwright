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
The run queue of `streamwright serve` in the state store (sw_requests): `serve` enqueues a run per accepted request,
workers claim them under an expiring claim (renewed while they run) and record how they ended. A worker that dies
leaves its claim to expire; another takes the run over, and the job's own batches resume (the run's job key is the
caller and its Idempotency-Key, so a completed batch is never run twice). Claims take turns on a named lock, so
concurrent workers count each other's runs against a scope and limits.concurrency.
"""

import time
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from streamwright.core.jobs.state.protocol import StateError
from streamwright.core.jobs.state.sql import SqlBase, json_text, json_value

__all__ = ["ACTIVE_REQUEST_STATUSES", "REQUEST_STATUSES", "RateLimitedError", "RequestOperations", "new_run_id"]

REQUEST_STATUSES = ("queued", "running", "succeeded", "failed")
ACTIVE_REQUEST_STATUSES = ("queued", "running")
_COLUMNS = ("run_id, job_name, principal, idempotency_key, body_sha256, scope_key, status, inputs, payload, "
            "request_id, attempts, worker_id, claim_expires_at, result, error_code, error_message, started_at, "
            "finished_at, created_at, updated_at")
# a run another worker may take: queued (and not deferred past now), or running under a claim that ended
_CLAIMABLE = ("((status = 'queued' AND (claim_expires_at IS NULL OR claim_expires_at < {now})) "
              "OR (status = 'running' AND claim_expires_at < {now}))")
_LIVE = "status = 'running' AND claim_expires_at >= {now}"
# no live run of the same job and scope (a claim query's runs are `r`)
_SCOPE_FREE = ("(r.scope_key = '' OR NOT EXISTS (SELECT 1 FROM {sw_requests} o WHERE o.job_name = r.job_name AND "
               "o.scope_key = r.scope_key AND o.run_id <> r.run_id AND o.status = 'running' AND "
               "o.claim_expires_at >= {now}))")
_ENDED = "status IN ('succeeded', 'failed')"
_CLAIM_LOCK = "sw_requests.claim:"               # + the schema: the named lock claims serialize on
_RATE_LOCK = "sw_requests.rate:"                 # + the caller and job: requests counted against a rate take turns


class RateLimitedError(StateError):
    """A caller's rate for a job is used up; `oldest` is when its oldest counted run was queued (ISO-8601)."""

    def __init__(self, oldest):
        super().__init__("the rate limit is reached")
        self.oldest = oldest


def new_run_id():
    """
    A run id: `run_`, the time in microseconds (13 hex digits) and 13 random hex digits. Ids sort in the order they
    were made, so runs queued within one timestamp (MySQL keeps whole seconds) are still claimed in order.
    """
    return "run_%013x%s" % (time.time_ns() // 1000, uuid.uuid4().hex[:13])


class RequestOperations(SqlBase):
    """sw_requests: enqueue, claim, renew, finish and read the runs requested over HTTP."""

    def _request(self, row: Sequence[Any]) -> dict[str, Any]:
        read = self._dialect.read_timestamp
        names = [name.strip() for name in _COLUMNS.split(",")]
        record = dict(zip(names, row, strict=True))
        for name in ("inputs", "payload", "result"):
            record[name] = None if record[name] is None else json_value(record[name])
        for name in ("claim_expires_at", "started_at", "finished_at", "created_at", "updated_at"):
            record[name] = read(record[name])
        record["attempts"] = int(record["attempts"] or 0)
        return record

    def _requests(self, conn: Any, where: str, args: Sequence[Any], suffix: str = "") -> list[dict[str, Any]]:
        rows = self._x(conn, "SELECT " + _COLUMNS + " FROM {sw_requests} WHERE " + where + suffix, args).fetchall()
        return [self._request(row) for row in rows]

    def enqueue_request(self, *, job_name: str, principal: str, body_sha256: str, inputs: Mapping[str, Any],
                        request_id: str, idempotency_key: str | None = None, scope_key: str = "",
                        payload: Sequence[Mapping[str, Any]] | None = None,
                        rate: tuple[int, Any] | None = None) -> tuple[dict[str, Any], bool]:
        """
        Queues a run, or returns the run the same Idempotency-Key already queued: (run, created). The caller compares
        the body fingerprints of an existing run. `rate` (requests, since: an aware datetime): RateLimitedError when
        the caller queued that many runs of the job since then - counted and queued in one transaction, the caller's
        and job's requests taking turns on a named lock, so the limit holds for requests at once on every host.
        """
        def work(conn):
            if idempotency_key is not None:
                found = self._requests(conn, "job_name = ? AND principal = ? AND idempotency_key = ?",
                                       (job_name, principal, idempotency_key))
                if found:
                    return found[0], False
            if rate is not None:
                self._named_lock(conn, "%s%s:%s:%s" % (_RATE_LOCK, self._schema or "", principal, job_name))
                count, oldest = self._recent(conn, principal, job_name, rate[1])
                if count >= rate[0]:
                    raise RateLimitedError(oldest)
            run_id = new_run_id()
            self._x(conn, "INSERT INTO {sw_requests} (run_id, job_name, principal, idempotency_key, body_sha256, "
                          "scope_key, status, inputs, payload, request_id, attempts, created_at, updated_at) VALUES "
                          "(?, ?, ?, ?, ?, ?, 'queued', {json}, {json}, ?, 0, {now}, {now})",
                    (run_id, job_name, principal, idempotency_key, body_sha256, scope_key, json_text(dict(inputs)),
                     None if payload is None else json_text(list(payload)), request_id))
            return self._requests(conn, "run_id = ?", (run_id,))[0], True

        try:
            return self._run(work)
        except RateLimitedError:
            raise
        except Exception:
            # the same key queued at once by another request: the unique key refused this one; that run is the run
            found = None if idempotency_key is None else self.find_request(job_name, principal, idempotency_key)
            if found is None:
                raise
            return found, False

    def find_request(self, job_name: str, principal: str, idempotency_key: str) -> dict[str, Any] | None:
        """The run a caller queued with this Idempotency-Key, or None."""
        found = self._run(lambda conn: self._requests(
            conn, "job_name = ? AND principal = ? AND idempotency_key = ?", (job_name, principal, idempotency_key)))
        return found[0] if found else None

    def get_request(self, run_id: str) -> dict[str, Any] | None:
        found = self._run(lambda conn: self._requests(conn, "run_id = ?", (run_id,)))
        return found[0] if found else None

    def list_requests(self, *, job_name: str | None = None, principal: str | None = None,
                      statuses: Sequence[str] = (), limit: int = 50) -> list[dict[str, Any]]:
        """Runs, newest first, narrowed to a job, a caller and statuses."""
        where, args = ["1 = 1"], []
        for column, value in (("job_name", job_name), ("principal", principal)):
            if value is not None:
                where.append("%s = ?" % column)
                args.append(value)
        if statuses:
            where.append("status IN (%s)" % self._marks(statuses))
            args.extend(statuses)
        return self._run(lambda conn: self._requests(conn, " AND ".join(where), (*args, int(limit)),
                                                     " ORDER BY created_at DESC, run_id DESC LIMIT ?"))

    def active_request(self, job_name: str, scope_key: str) -> dict[str, Any] | None:
        """The oldest queued or running run of the job's scope, or None."""
        found = self._run(lambda conn: self._requests(
            conn, "job_name = ? AND scope_key = ? AND status IN ('queued', 'running')", (job_name, scope_key),
            " ORDER BY created_at, run_id LIMIT 1"))
        return found[0] if found else None

    def count_requests(self, statuses: Sequence[str], job_name: str | None = None) -> int:
        where, args = "status IN (%s)" % self._marks(statuses), list(statuses)
        if job_name is not None:
            where, args = where + " AND job_name = ?", [*args, job_name]
        return int(self._run(lambda conn: self._x(conn, "SELECT COUNT(*) FROM {sw_requests} WHERE " + where,
                                                  args).fetchone()[0]))

    def claim_request(self, worker_id: str, ttl_seconds: int, *,
                      concurrency: Mapping[str, int] | None = None) -> dict[str, Any] | None:
        """
        Claims the oldest run a worker may take, or returns None: one whose scope has no live run, and whose job runs
        fewer live runs than `concurrency[job]` (no limit when it is absent). One query finds it, however many older
        runs wait on a busy scope. Claims take turns (a named lock on Postgres and MySQL, the write lock on SQLite), so
        each counts the runs the others claimed.
        """
        limits = dict(concurrency or {})

        def work(conn):
            self._claim_lock(conn)
            where, args = _CLAIMABLE + " AND " + _SCOPE_FREE, []
            capped = self._capped_jobs(conn, limits)
            if capped:
                where, args = where + " AND r.job_name NOT IN (%s)" % self._marks(capped), capped
            row = self._x(conn, "SELECT r.run_id FROM {sw_requests} r WHERE " + where +
                          " ORDER BY r.created_at, r.run_id LIMIT 1", args).fetchone()
            if row is None:
                return None
            self._x(conn, "UPDATE {sw_requests} SET status = 'running', worker_id = ?, claim_expires_at = {expires}, "
                          "attempts = attempts + 1, started_at = COALESCE(started_at, {now}), updated_at = {now} "
                          "WHERE run_id = ?", (worker_id, int(ttl_seconds), row[0]))
            return self._requests(conn, "run_id = ?", (row[0],))[0]

        return self._run(work)

    def _capped_jobs(self, conn: Any, limits: Mapping[str, int]) -> list[str]:
        """The jobs of `limits` that run as many live runs as their limit allows."""
        if not limits:
            return []
        names = sorted(limits)
        rows = self._x(conn, "SELECT job_name, COUNT(*) FROM {sw_requests} WHERE job_name IN (%s) AND %s "
                             "GROUP BY job_name" % (self._marks(names), _LIVE), names).fetchall()
        return sorted(name for name, live in rows if int(live) >= int(limits[name]))

    def _claim_lock(self, conn: Any) -> None:
        self._named_lock(conn, _CLAIM_LOCK + (self._schema or ""))

    def _named_lock(self, conn: Any, key: str) -> None:
        """A transaction-long named lock (Postgres, MySQL; SQLite's write lock serializes already)."""
        lock = self._dialect.migration_lock
        if lock:
            row = self._x(conn, lock, (key,)).fetchone()
            if not self._dialect.migration_locked(row):
                raise StateError("could not take the run queue's lock %s in %s" % (key, self.target))

    def renew_request(self, run_id: str, worker_id: str, ttl_seconds: int) -> bool:
        """Extends the worker's claim; False when the run is no longer the worker's (taken over or ended)."""
        def work(conn):
            self._x(conn, "UPDATE {sw_requests} SET claim_expires_at = {expires}, updated_at = {now} "
                          "WHERE run_id = ? AND worker_id = ? AND status = 'running'",
                    (int(ttl_seconds), run_id, worker_id))
            return bool(self._requests(conn, "run_id = ? AND worker_id = ? AND status = 'running'",
                                       (run_id, worker_id)))

        return self._run(work)

    def defer_request(self, run_id: str, worker_id: str, seconds: int) -> bool:
        """Puts the worker's run back in the queue, to be claimed again after `seconds`; False when it is not its."""
        def work(conn):
            if not self._requests(conn, "run_id = ? AND worker_id = ? AND status = 'running'", (run_id, worker_id)):
                return False
            self._x(conn, "UPDATE {sw_requests} SET status = 'queued', worker_id = NULL, claim_expires_at = {expires}, "
                          "updated_at = {now} WHERE run_id = ?", (int(seconds), run_id))
            return True

        return self._run(work)

    def finish_request(self, run_id: str, worker_id: str, *, succeeded: bool, result: Any = None,
                       error_code: str | None = None, error_message: str | None = None) -> bool:
        """
        Records how the worker's run ended and removes its input rows; False when the run is no longer the worker's
        (another worker took it over: its outcome is that worker's to record).
        """
        def work(conn):
            if not self._requests(conn, "run_id = ? AND worker_id = ? AND status = 'running'", (run_id, worker_id)):
                return False
            self._x(conn, "UPDATE {sw_requests} SET status = ?, result = {json}, error_code = ?, error_message = ?, "
                          "payload = NULL, claim_expires_at = NULL, finished_at = {now}, updated_at = {now} "
                          "WHERE run_id = ?",
                    ("succeeded" if succeeded else "failed", None if result is None else json_text(result),
                     error_code, error_message, run_id))
            return True

        return self._run(work)

    def purge_requests(self, older_than: Any, *, dry_run: bool = False, chunk: int = 10000) -> int:
        """
        Deletes the ended runs that finished before `older_than` (an aware datetime), `chunk` rows per transaction (a
        large queue is never locked for long); returns how many (`dry_run`: how many would be, deleting none).
        """
        when = self._dialect.timestamp_parameter(older_than)
        where = _ENDED + " AND finished_at < ?"
        if dry_run:
            return int(self._run(lambda conn: self._x(conn, "SELECT COUNT(*) FROM {sw_requests} WHERE " + where,
                                                      (when,)).fetchone()[0]))

        def work(conn):
            ids = [row[0] for row in self._x(conn, "SELECT run_id FROM {sw_requests} WHERE " + where +
                                             " ORDER BY finished_at LIMIT %d" % int(chunk), (when,)).fetchall()]
            if ids:
                self._x(conn, "DELETE FROM {sw_requests} WHERE run_id IN (%s)" % self._marks(ids), ids)
            return len(ids)

        total = 0
        while True:
            deleted = self._run(work)
            total += deleted
            if deleted < chunk:
                return total

    def recent_requests(self, principal: str, job_name: str, since: Any) -> tuple[int, str | None]:
        """(the runs a caller queued for a job since `since`, an aware datetime; the oldest one's created_at)."""
        return self._run(lambda conn: self._recent(conn, principal, job_name, since))

    def _recent(self, conn: Any, principal: str, job_name: str, since: Any) -> tuple[int, str | None]:
        row = self._x(conn, "SELECT COUNT(*), MIN(created_at) FROM {sw_requests} WHERE principal = ? AND job_name = ? "
                            "AND created_at >= ?", (principal, job_name, self._dialect.timestamp_parameter(since)))
        row = row.fetchone()
        return int(row[0]), self._dialect.read_timestamp(row[1])

    def queue_stats(self, since: Any) -> dict[str, list]:
        """
        The queue now and lately: {"active": [(job, status, runs, oldest created_at)] of queued and running runs,
        "finished": [(job, status, created_at, started_at, finished_at)] of the runs that ended since `since`}.
        """
        def work(conn):
            read = self._dialect.read_timestamp
            active = self._x(conn, "SELECT job_name, status, COUNT(*), MIN(created_at) FROM {sw_requests} WHERE "
                                   "status IN ('queued', 'running') GROUP BY job_name, status").fetchall()
            finished = self._x(conn, "SELECT job_name, status, created_at, started_at, finished_at FROM {sw_requests} "
                                     "WHERE finished_at >= ? AND " + _ENDED,
                               (self._dialect.timestamp_parameter(since),)).fetchall()
            return {"active": [(job, status, int(runs), read(oldest)) for job, status, runs, oldest in active],
                    "finished": [(job, status, read(created), read(started), read(ended))
                                 for job, status, created, started, ended in finished]}

        return self._run(work)
