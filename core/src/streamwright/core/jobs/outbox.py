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
The outbox (sw_events) of a job's batches: what a batch's publish and apply outputs commit with it, and the relay that
delivers them after the batch is committed - events to their broker (at least once, stable dedupe keys), apply rows
through their connector (streamwright.core.jobs.apply.delivery).
"""

from streamwright.core.jobs.apply.errors import ApplyError, RowsRejectedError
from streamwright.core.runtime.logs.context import log_context

__all__ = ["OutboxError", "entries", "relay"]


class OutboxError(Exception):
    """The outbox cannot be committed or relayed as the job declares it."""


def entries(events, publishers, applier, outputs):
    """
    {output: (destination, rows)} to commit with a batch: `events` are the executor's rows per output, `outputs` the
    batch's (pinned) output items, whose rendered arguments (or request) an apply output's destination pins.
    """
    found = {}
    for name, rows in events.items():
        if applier is not None and name in applier.calls:
            found[name] = (applier.destination(name, outputs.get(name) or {}), list(rows))
        elif name in publishers:
            call = publishers[name]
            found[name] = (call.broker.destination(call.arguments), list(rows))
        else:
            raise OutboxError("output %s produced events but publishes to no broker" % name)
    return found


def relay(state, lease, *, job_name, scope, publishers, applier, log):
    """
    Delivers the scope's unsettled outbox rows, batch by batch and output by output. Rows an apply output rejected
    under on_row_error: fail make their batch fail (runtime.state.failed_job_policy decides what next: a retry sends
    those rows again, abandon leaves them failed).
    """
    groups = {}
    for row in state.list_pending_events(job_name, scope):
        groups.setdefault((row["batch_id"], row["output_name"]), []).append(row)
    for (batch_id, output_name), rows in sorted(groups.items()):
        with log_context(output=output_name):           # its lines, the vendor's and the connector's
            if applier is not None and output_name in applier.calls:
                try:
                    applier.deliver(state, lease, batch_id, output_name, rows=rows, log=log)
                except RowsRejectedError as exc:
                    batch = state.get_batch(job_name, exc.batch_id)
                    state.record_batch(lease, exc.batch_id, "failed", batch["manifest"], error=str(exc))
                    raise OutboxError(str(exc)) from None
                except ApplyError as exc:
                    raise OutboxError(str(exc)) from None
                continue
            call = publishers.get(output_name)
            if call is None:
                raise OutboxError("the outbox holds rows for output %r, which neither publishes to a broker nor "
                                  "applies operations" % output_name)
            try:
                call.broker.publish(call.arguments, [r["payload"] for r in rows], "%s:%s:%s" % (
                    job_name, batch_id, output_name))
            except Exception as exc:
                raise OutboxError("relaying %d events of %s/%s failed: %s: %s" % (
                    len(rows), batch_id, output_name, type(exc).__name__, exc)) from None
            state.mark_events_sent(lease, batch_id, output_name, [r["event_sequence"] for r in rows])
            log("relayed", job_id=batch_id, output=output_name, events=len(rows))
