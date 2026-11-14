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

"""Leases (sw_leases): one row per token, taken together under one epoch, with database-clock expiry."""

import random
import time
from collections.abc import Mapping, Sequence
from typing import Any

from streamwright.core.jobs.state.protocol import LeaseHeldError, StateError
from streamwright.core.jobs.state.sql import LOG, SqlBase
from streamwright.core.jobs.types import Lease

__all__ = ["LOCK_POLICIES", "LeaseOperations"]

LOCK_POLICIES = ("fail", "wait", "prune")
# waiting for a lease (policy wait): the first pause, its growth and its cap, in seconds
_WAIT_FIRST, _WAIT_FACTOR, _WAIT_MAX = 0.2, 1.6, 5.0


def _held_message(job_name: str, conflicts: Mapping[str, Mapping[str, Any]], what: str = "is held") -> str:
    if set(conflicts) == {""}:
        info = conflicts[""]
        return "job %s is held by %s until %s" % (job_name, info["owner_id"], info["expires_at"])
    return "job %s lease %s: %s" % (job_name, what, ", ".join(
        "%s (held by %s until %s)" % (token or "<job>", info["owner_id"], info["expires_at"])
        for token, info in sorted(conflicts.items())))


class LeaseOperations(SqlBase):
    # --- leases (sw_leases) ---------------------------------------------------------------------------------

    def acquire_lease(self, job_name: str, owner_id: str, ttl_seconds: int, *, tokens: Sequence[str] = ("",),
                      optional: Sequence[str] = (), policy: str = "fail", timeout: float = 0.0) -> Lease:
        """
        Takes `tokens` (all needed) and `optional` (wanted) under one new epoch. When another owner holds one:
        `fail` raises LeaseHeldError, `wait` retries (backing off, with jitter) until `timeout` seconds pass, and
        `prune` drops the held optional tokens (Lease.pruned_tokens) or, when a needed token is held, takes
        nothing and returns a lease with no tokens and epoch 0 (the run is skipped).
        """
        if policy not in LOCK_POLICIES:
            raise StateError("unknown lock policy %r (known: %s)" % (policy, ", ".join(LOCK_POLICIES)))
        needed = tuple(sorted(set(tokens or ("",))))
        wanted = tuple(sorted(set(optional) - set(needed)))
        everything = tuple(sorted(needed + wanted))      # one lock order for every worker: no deadlock
        deadline = time.monotonic() + max(0.0, float(timeout or 0.0))
        pause = _WAIT_FIRST

        def work(conn):
            for token in everything:
                self._x(conn, "INSERT INTO {sw_leases} (job_name, token, worker_id, epoch, acquired_at, expires_at, "
                              "created_at, updated_at) VALUES (?, ?, ?, 0, {now}, NULL, {now}, {now}) "
                        + self._dialect.conflict_ignore(("job_name", "token")), (job_name, token, owner_id))
            rows = self._lease_rows(conn, job_name, everything, self._dialect.update_lock)
            conflicts = {token: {"owner_id": row[0], "expires_at": self._dialect.read_timestamp(row[2])}
                         for token, row in rows.items() if row[3] and row[0] != owner_id}
            if conflicts and (policy != "prune" or any(token in conflicts for token in needed)):
                if policy == "prune":
                    return Lease(job_name, owner_id, 0, "", tokens=(), pruned_tokens=everything), conflicts
                return None, conflicts
            taken = tuple(token for token in everything if token not in conflicts)
            epoch = max(row[1] for row in rows.values()) + 1
            for token in taken:
                self._x(conn, "UPDATE {sw_leases} SET worker_id = ?, epoch = ?, acquired_at = {now}, "
                              "expires_at = {expires}, updated_at = {now} WHERE job_name = ? AND token = ?",
                        (owner_id, epoch, int(ttl_seconds), job_name, token))
            expires = self._lease_rows(conn, job_name, taken[:1])[taken[0]][2]
            return Lease(job_name, owner_id, epoch, self._dialect.read_timestamp(expires), tokens=taken,
                         pruned_tokens=tuple(token for token in wanted if token in conflicts)), conflicts

        while True:
            lease, conflicts = self._run(work)
            if lease is not None:
                if lease.epoch == 0:
                    LOG.warning("%s; skipping the run (lock policy prune)",
                                _held_message(job_name, {t: c for t, c in conflicts.items() if t in needed}))
                elif lease.pruned_tokens:
                    LOG.warning("%s; pruned (lock policy prune)",
                                _held_message(job_name, {t: conflicts[t] for t in lease.pruned_tokens}))
                return lease
            remaining = deadline - time.monotonic()
            if policy != "wait" or remaining <= 0:
                raise LeaseHeldError(_held_message(job_name, conflicts, "wait timed out" if policy == "wait"
                                                   else "is held"), conflicts=conflicts)
            self._sleep(min(remaining, pause * random.uniform(0.5, 1.0)))
            pause = min(_WAIT_MAX, pause * _WAIT_FACTOR)

    def renew_lease(self, lease: Lease, ttl_seconds: int) -> Lease:
        def work(conn):
            self._check(conn, lease, lock=self._dialect.update_lock)
            for token in sorted(lease.tokens or ("",)):
                self._x(conn, "UPDATE {sw_leases} SET expires_at = {expires}, updated_at = {now} "
                              "WHERE job_name = ? AND token = ? AND epoch = ?",
                        (int(ttl_seconds), lease.job_name, token, lease.epoch))
            expires = self._lease_rows(conn, lease.job_name, (lease.tokens or ("",))[:1])
            return Lease(lease.job_name, lease.owner_id, lease.epoch,
                         self._dialect.read_timestamp(next(iter(expires.values()))[2]),
                         tokens=lease.tokens, pruned_tokens=lease.pruned_tokens)

        return self._run(work)

    def check_lease(self, lease: Lease) -> None:
        """Raises StaleLeaseError unless every token of `lease` is still held, unexpired, at its epoch."""
        self._run(lambda conn: self._check(conn, lease, unexpired=True))

    def release_lease(self, lease: Lease) -> None:
        def work(conn):
            for token in sorted(lease.tokens or ("",)):
                self._x(conn, "UPDATE {sw_leases} SET expires_at = NULL, updated_at = {now} "
                              "WHERE job_name = ? AND token = ? AND epoch = ? AND worker_id = ?",
                        (lease.job_name, token, lease.epoch, lease.owner_id))

        if lease.epoch:
            self._run(work)

    def _lease_dict(self, job_name: str, token: str, row: tuple) -> dict[str, Any]:
        return {"job_name": job_name, "token": token, "owner_id": row[0], "epoch": row[1],
                "expires_at": self._dialect.read_timestamp(row[2]), "held": row[3]}

    def get_lease(self, job_name: str, token: str = "") -> dict[str, Any] | None:
        rows = self._run(lambda conn: self._lease_rows(conn, job_name, (token,)))
        return self._lease_dict(job_name, token, rows[token]) if token in rows else None

    def list_leases(self, job_name: str) -> list[dict[str, Any]]:
        """Every lease token of the job (held or not), by token."""
        def work(conn):
            return self._x(conn, "SELECT token, worker_id, epoch, expires_at, CASE WHEN expires_at > {now} THEN 1 "
                                 "ELSE 0 END FROM {sw_leases} WHERE job_name = ? ORDER BY token",
                           (job_name,)).fetchall()

        return [self._lease_dict(job_name, row[0], (row[1], int(row[2]), row[3], bool(int(row[4]))))
                for row in self._run(work)]
