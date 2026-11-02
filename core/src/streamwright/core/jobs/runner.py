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
The batch loop and its delivery guarantees, shared by every job engine. No engine import: the engine's work is behind
`Executor.execute`.

One batch:
  1. poll messages (if the job consumes a broker) and pin them with the batch time in the manifest   received
  2. execute: build the steps, save the manifest (pinned values) before the first write, then write each output
     that has no receipt; every write is fenced by the lease and recorded with its receipt              processing
  3. all outputs written: the status and the outbox events are stored in ONE transaction               committed
  4. the messages are acknowledged (only now: the outbox is durable)                                   acked
  5. the outbox is relayed (at least once, stable dedupe keys)                                         completed

A batch's manifest `carry` section is copied into the next batch (an executor keeps bookmarks there).
A crash anywhere is repaired by the next start: unfinished batches resume from their manifest (outputs with a
receipt are not rerun; a committed batch is never re-executed), then the outbox is relayed, then new work starts. A
failed batch stops the job until it is retried (it resumes from the status it failed from) or abandoned. A relay
failure is reported on its own and never marks a batch failed. The lease is renewed by a heartbeat while a batch
runs; a worker that lost it stops at its next fenced write (StaleLeaseError).
"""

import datetime
import logging
import os
import socket
import threading
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from streamwright.core.jobs.state import StaleLeaseError
from streamwright.core.jobs.types import BatchContext, Broker, ExecutionHooks, Message

__all__ = ["ERROR_TEXT_LIMIT", "BrokerCall", "RelayError", "Runner", "RunnerError", "owner_id"]

LOG = logging.getLogger("streamwright.jobs.runner")
ERROR_TEXT_LIMIT = 2000


class RunnerError(Exception):
    """The job stopped: a batch failed, or a failed batch waits for retry or abandon."""


class RelayError(RunnerError):
    """Publishing the outbox failed; the batches stay acked and are relayed by the next run."""


@dataclass(frozen=True)
class BrokerCall:
    """A broker with the arguments of the step (consume) or output (publish) that uses it."""
    broker: Broker
    arguments: Mapping[str, Any]


def owner_id():
    """A lease owner unique to this runner: host, process and a random part (two runs in one process differ)."""
    return "%s:%d:%s" % (socket.gethostname(), os.getpid(), uuid.uuid4().hex[:12])


def _now_iso():
    return datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class _Heartbeat:
    """
    Renews the lease in the background while a batch runs; remembers why renewal stopped. Only a lost lease (epoch or
    owner changed) or a lease that expired while renewals failed stops it: a failed renewal (e.g. a lock conflict
    with a contending acquire) is retried soon.
    """

    RETRY_SECONDS = 0.2

    def __init__(self, runner, interval):
        self.runner, self.interval = runner, interval
        self.lost = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._beat, name="streamwright-lease-heartbeat", daemon=True)

    def _beat(self):
        runner = self.runner
        valid_until = time.monotonic() + runner.lease_seconds
        wait = self.interval
        while not self._stop.wait(wait):
            try:
                runner.lease = runner.state.renew_lease(runner.lease, runner.lease_seconds)
            except StaleLeaseError as exc:
                self.lost = exc
                return
            except Exception as exc:
                if time.monotonic() >= valid_until:
                    self.lost = exc
                    return
                runner.log("lease_renewal_retried", error=runner.redact(str(exc)))
                wait = min(self.interval, self.RETRY_SECONDS)
                continue
            valid_until = time.monotonic() + runner.lease_seconds
            wait = self.interval

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        self._stop.set()
        self._thread.join()
        return False


class Runner:
    def __init__(self, job_name, state, executor, consumer=None, publishers=None, *, redact=None, lease_seconds=300,
                 heartbeat_seconds=None, engine_metadata=None, owner=None, log=None, clock=_now_iso,
                 sleep=time.sleep):
        """
        consumer:   BrokerCall of the job's consume step, or None
        publishers: {output name: BrokerCall} for the outputs that publish events
        redact:     callable applied to every error text that is logged or stored
        """
        self.job_name, self.state, self.executor = job_name, state, executor
        self.consumer, self.publishers = consumer, dict(publishers or {})
        self.redact = redact or (lambda text: text)
        self.lease_seconds = int(lease_seconds)
        self.heartbeat_seconds = heartbeat_seconds if heartbeat_seconds is not None else max(1, self.lease_seconds // 3)
        self.engine_metadata = dict(engine_metadata or {})
        self.owner = owner or owner_id()
        self.log = log or (lambda event, **fields: LOG.info("%s %s", event, fields))
        self.clock, self.sleep = clock, sleep
        self.lease = None
        self.processed = []                  # batch ids this run processed (new and resumed)
        self.error = None                    # the (redacted) reason the run stopped

    # --- public ---------------------------------------------------------------------------------------------

    def run(self, mode="once", interval=10, max_messages=500, run_key=None, max_wait=30):
        """
        mode: once | drain | available_now | follow. max_messages 0 = recover and relay only. Returns an exit code.
        """
        self.lease = self.state.acquire_lease(self.job_name, self.owner, self.lease_seconds)
        try:
            self._recover()
            self._relay()
            if max_messages == 0:
                return 0
            watermarks = self._watermarks() if mode == "available_now" else None
            while True:
                self.lease = self.state.renew_lease(self.lease, self.lease_seconds)
                processed = self._next_batch(max_messages, run_key, max_wait, watermarks)
                if mode == "once" or (mode in ("drain", "available_now") and not processed):
                    return 0
                if not processed:
                    self.sleep(interval)
        except StaleLeaseError as exc:
            return self._stop("lease_lost", exc)
        except RunnerError as exc:
            return self._stop("stopped", exc)
        except KeyboardInterrupt:
            self.log("interrupted")
            return 130
        finally:
            self._release()

    def _stop(self, event, exc):
        self.error = self.redact(str(exc))
        self.log(event, error=self.error)
        return 1

    def _release(self):
        try:
            self.state.release_lease(self.lease)
        except StaleLeaseError:
            pass                                    # taken over: nothing of ours to release
        except Exception as exc:
            # the lease expires on its own; the run's own outcome is what the caller needs
            self.log("release_failed", error=self.redact(str(exc)))

    def _watermarks(self):
        if self.consumer is None or not self.consumer.broker.supports_watermarks:
            return None
        found = self.consumer.broker.watermarks(self.consumer.arguments)
        self.log("watermarks", watermarks=str(found))
        return found

    # --- recovery -------------------------------------------------------------------------------------------

    def _recover(self):
        for batch in self.state.list_incomplete_batches(self.job_name):
            if batch["status"] == "failed":
                raise RunnerError("batch %s failed earlier (%s); retry or abandon it" % (
                    batch["batch_id"], batch.get("error") or "no detail"))
            self.log("resume", batch=batch["batch_id"], status=batch["status"])
            self._process(batch["batch_id"], batch["batch_sequence"], batch["manifest"], batch["status"])

    # --- one new batch --------------------------------------------------------------------------------------

    def _next_batch(self, max_messages, run_key, max_wait, watermarks):
        messages = []
        if self.consumer is not None:
            broker, arguments = self.consumer.broker, self.consumer.arguments
            messages = broker.poll(arguments, int(broker.poll_limit(arguments, max_messages)),
                                   broker.poll_wait_seconds(arguments, max_wait), watermarks=watermarks)
            if not messages:
                return False
            run_key = None
        elif run_key is not None and self.state.find_batch_by_run_key(self.job_name, run_key) is not None:
            self.log("skip", run_key=run_key, reason="a batch with this run key already completed")
            return False
        sequence = self.state.next_batch_sequence(self.job_name)
        batch_id = "%s-%08d" % (self.job_name, sequence)
        manifest = {"batch_ts": self.clock(), "pinned": {}, "carry": self._carried(),
                    "messages": [m.to_dict() for m in messages]}
        self.state.record_batch(self.lease, batch_id, "received", manifest, batch_sequence=sequence, run_key=run_key,
                                engine_metadata=self.engine_metadata or None)
        self._process(batch_id, sequence, manifest, "received")
        return True

    def _carried(self):
        """What the last completed batch carries to the next one (e.g. source bookmarks), from its manifest."""
        batch = self.state.find_latest_completed_batch(self.job_name)
        return dict(batch["manifest"].get("carry") or {}) if batch else {}

    def _process(self, batch_id, sequence, manifest, status):
        self.processed.append(batch_id)
        try:
            if status in ("received", "processing"):
                self._execute(batch_id, sequence, manifest, status)
                status = "committed"
            if status == "committed":
                messages = [Message(**m) for m in manifest.get("messages") or ()]
                if messages:
                    self.consumer.broker.ack(messages, self.consumer.arguments)
                self.state.record_batch(self.lease, batch_id, "acked", manifest)
        except (StaleLeaseError, KeyboardInterrupt):
            raise
        except Exception as exc:
            text = self.redact("%s: %s" % (type(exc).__name__, exc))[:ERROR_TEXT_LIMIT]
            self.log("batch_failed", batch=batch_id, error=text)
            self.state.record_batch(self.lease, batch_id, "failed", manifest, error=text)
            raise RunnerError("batch %s failed: %s" % (batch_id, text)) from None
        self._relay()
        self.state.record_batch(self.lease, batch_id, "completed", manifest)
        self.log("completed", batch=batch_id)

    def _execute(self, batch_id, sequence, manifest, status):
        if status == "received":
            self.state.record_batch(self.lease, batch_id, "processing", manifest)
        context = BatchContext(batch_id, sequence, manifest["batch_ts"], self.job_name, self.lease.epoch,
                               self.state.store_identity(), self.engine_metadata)
        done = self.state.get_receipts(self.job_name, batch_id)
        with _Heartbeat(self, self.heartbeat_seconds) as heartbeat:
            def fence():
                if heartbeat.lost is not None:
                    raise StaleLeaseError("lease renewal failed: %s" % heartbeat.lost)
                self.state.check_lease(self.lease)

            hooks = ExecutionHooks(
                fence=fence,
                save_manifest=lambda pinned: self.state.record_batch(self.lease, batch_id, "processing", pinned),
                record_receipt=lambda receipt, pinned: self.state.record_receipt(self.lease, batch_id, receipt,
                                                                                 pinned))
            result = self.executor.execute(context, manifest, done, hooks)
            fence()
        events = {}
        for output_name, rows in result.events.items():
            if output_name not in self.publishers:
                raise RunnerError("output %s produced events but publishes to no broker" % output_name)
            call = self.publishers[output_name]
            events[output_name] = (call.broker.destination(call.arguments), list(rows))
        self.state.commit_batch(self.lease, batch_id, manifest, events, result.schema_signatures)
        if result.schema_reports:
            rejected = sum(1 for r in result.schema_reports if r.status == "rejected")
            self.log("batch_files", batch=batch_id, read=len(result.schema_reports) - rejected, rejected=rejected,
                     drifted=sum(1 for r in result.schema_reports if r.status == "drift"))

    # --- outbox ---------------------------------------------------------------------------------------------

    def _relay(self):
        groups = {}
        for row in self.state.list_pending_events(self.job_name):
            groups.setdefault((row["batch_id"], row["output_name"]), []).append(row)
        for (batch_id, output_name), rows in sorted(groups.items()):
            call = self.publishers.get(output_name)
            if call is None:
                raise RelayError("the outbox holds events for output %r, which publishes to no broker" % output_name)
            try:
                call.broker.publish(call.arguments, [r["payload"] for r in rows],
                                    "%s:%s:%s" % (self.job_name, batch_id, output_name))
            except Exception as exc:
                raise RelayError("relaying %d events of %s/%s failed: %s: %s" % (
                    len(rows), batch_id, output_name, type(exc).__name__, exc)) from None
            self.state.mark_events_sent(self.lease, batch_id, output_name, [r["event_sequence"] for r in rows])
            self.log("relayed", batch=batch_id, output=output_name, events=len(rows))
