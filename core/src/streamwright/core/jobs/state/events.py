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
The outbox rows (sw_events) after their batch's commit: listing them, recording apply rows' calls and outcomes, settling
held rows and sending rejected rows again when their batch is retried.
"""

from collections.abc import Sequence
from typing import Any

from streamwright.core.jobs.state.dialects import RowUpdate
from streamwright.core.jobs.state.sql import SqlBase
from streamwright.core.jobs.state.sql import json_text as _json_text
from streamwright.core.jobs.state.sql import json_value as _json_value
from streamwright.core.jobs.types import Lease

__all__ = ["EventOperations"]

# apply rows' outcomes, a statement per chunk of rows (record_event_outcomes): (sequence, status, result, code, message)
_OUTCOMES = RowUpdate(
    table="{sw_events}", key="sequence",
    columns=(("sequence", "{bigint}"), ("status", "?"), ("result", "{json}"), ("error_code", "?"),
             ("error_message", "?")),
    assignments=(("status", "v.status"), ("result", "v.result"), ("error_code", "v.error_code"),
                 ("error_message", "v.error_message"), ("sent_at", "{now}"), ("updated_at", "{now}")),
    where="t.job_name = ? AND t.job_id = ? AND t.target = ?")


class EventOperations(SqlBase):
    # --- outbox (sw_events) ---------------------------------------------------------------------------------

    def _resend_failed(self, conn: Any, job_name: str, batch_id: str, outputs: Sequence[str] | None) -> None:
        """
        A retried batch's failed apply rows of `outputs` (None: every output) back to pending, keeping their attempts
        and last error until they are sent again; rows the operator skipped (UNKNOWN_OUTCOME) stay failed.
        """
        outputs = None if outputs is None else tuple(outputs)
        if outputs == ():
            return
        target = "" if outputs is None else " AND target IN (%s)" % self._marks(outputs)
        self._x(conn, "UPDATE {sw_events} SET status = 'pending', updated_at = {now} WHERE job_name = ? AND job_id = ? "
                      "AND status = 'failed' AND COALESCE(error_code, '') <> 'UNKNOWN_OUTCOME'" + target,
                (job_name, batch_id, *(outputs or ())))

    def list_pending_events(self, job_name: str, scope: str = "") -> list[dict[str, Any]]:
        """
        The scope's outbox rows not yet settled - events not sent, apply rows pending or in flight (a call whose
        outcome is unknown) - in batch, output and row order.
        """
        rows = self._run(lambda conn: self._x(
            conn, "SELECT e.job_id, e.target, e.sequence, e.destination, e.payload, e.status, e.attempts "
                  "FROM {sw_events} e JOIN {sw_jobs} j ON j.job_name = e.job_name AND j.job_id = e.job_id "
                  "WHERE e.job_name = ? AND j.scope = ? AND e.status IN ('pending', 'in_flight') "
                  "ORDER BY j.sequence, e.target, e.sequence", (job_name, scope)).fetchall())
        return [{"batch_id": row[0], "output_name": row[1], "event_sequence": int(row[2]), "destination": row[3],
                 "payload": _json_value(row[4]), "status": row[5], "attempts": int(row[6] or 0)} for row in rows]

    def list_events(self, job_name: str, batch_id: str) -> list[dict[str, Any]]:
        """A batch's outbox rows with their status and outcome (`streamwright state show`)."""
        rows = self._run(lambda conn: self._x(
            conn, "SELECT target, sequence, status, attempts, result, error_code, error_message FROM {sw_events} "
                  "WHERE job_name = ? AND job_id = ? ORDER BY target, sequence", (job_name, batch_id)).fetchall())
        return [{"output_name": row[0], "event_sequence": int(row[1]), "status": row[2], "attempts": int(row[3] or 0),
                 "result": _json_value(row[4]), "error_code": row[5], "error_message": row[6]} for row in rows]

    def count_events(self, job_name: str, batch_ids: Sequence[str]) -> dict[str, dict[str, int]]:
        """{job id: {status: rows}} of batches' outbox rows (apply rows and events), for `streamwright state list`."""
        if not batch_ids:
            return {}
        rows = self._run(lambda conn: self._x(
            conn, "SELECT job_id, status, COUNT(*) FROM {sw_events} WHERE job_name = ? AND job_id IN (%s) "
                  "GROUP BY job_id, status" % self._marks(batch_ids), (job_name, *batch_ids)).fetchall())
        found: dict[str, dict[str, int]] = {}
        for batch_id, status, count in rows:
            found.setdefault(batch_id, {})[status] = int(count)
        return found

    def mark_events_in_flight(self, lease: Lease, batch_id: str, output_name: str,
                              event_sequences: Sequence[int]) -> None:
        """Records, before the call, that these apply rows are being sent (one more attempt each)."""
        self._update_events(lease, batch_id, output_name, event_sequences,
                            assignments="status = 'in_flight', attempts = attempts + 1, updated_at = {now}")

    def record_event_outcomes(self, lease: Lease, batch_id: str, output_name: str,
                              outcomes: Sequence[tuple[int, str, Any, str | None, str | None]]) -> None:
        """Stores apply rows' outcomes: (sequence, status applied|failed, result, error code, error message)."""
        def work(conn):
            self._check(conn, lease)
            rows = [(int(number), status, None if result is None else _json_text(result), code, message)
                    for number, status, result, code, message in outcomes]
            if self._update_rows(conn, _OUTCOMES, (lease.job_name, batch_id, output_name), rows):
                return
            for number, status, result, code, message in outcomes:     # a database without UPDATE ... FROM
                self._x(conn, "UPDATE {sw_events} SET status = ?, result = {json}, error_code = ?, error_message = ?, "
                              "sent_at = {now}, updated_at = {now} "
                              "WHERE job_name = ? AND job_id = ? AND target = ? AND sequence = ?",
                        (status, None if result is None else _json_text(result), code, message, lease.job_name,
                         batch_id, output_name, int(number)))

        self._run(work)

    def settle_in_flight_events(self, lease: Lease, batch_id: str, output_name: str | None, resend: bool) -> int:
        """
        Settles a batch's in-flight apply rows (an unknown outcome held for the operator): back to pending (`resend`)
        or failed with the code UNKNOWN_OUTCOME; returns how many rows changed.
        """
        target = "" if output_name is None else " AND target = ?"
        args = (lease.job_name, batch_id) + (() if output_name is None else (output_name,))
        change = ("status = 'pending'" if resend else
                  "status = 'failed', error_code = 'UNKNOWN_OUTCOME', error_message = 'skipped by the operator: "
                  "the call may or may not have been applied'")

        where = "WHERE job_name = ? AND job_id = ? AND status = 'in_flight'" + target

        def work(conn):
            self._check(conn, lease)
            count = int(self._x(conn, "SELECT COUNT(*) FROM {sw_events} " + where, args).fetchone()[0])
            self._x(conn, "UPDATE {sw_events} SET %s, updated_at = {now} %s" % (change, where), args)
            return count

        return self._run(work)

    def _update_events(self, lease, batch_id, output_name, event_sequences, *, assignments, args=()):
        def work(conn):
            self._check(conn, lease)
            self._update_in(conn, "UPDATE {sw_events} SET %s WHERE job_name = ? AND job_id = ? AND target = ?"
                            % assignments, (*args, lease.job_name, batch_id, output_name), "sequence",
                            [int(number) for number in event_sequences])

        self._run(work)

    def mark_events_sent(self, lease: Lease, batch_id: str, output_name: str, event_sequences: Sequence[int]) -> None:
        self._update_events(lease, batch_id, output_name, event_sequences,
                            assignments="status = 'sent', sent_at = {now}, updated_at = {now}")
