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
Everything a run sees is its job's and scope's (runtime.state.scope): batches, job keys, carried values, sequences and
the outbox. The lease holds the scope's token, the lock tokens and the tokens pinned by the scope's unfinished
batches; a batch's manifest pins its inputs, and resuming it rebuilds the work from them (`prepare`).
A crash anywhere is repaired by the next start: unfinished batches resume from their manifest (outputs with a
receipt are not rerun; a committed batch is never re-executed), then the outbox is relayed, then new work starts. A
failed batch is dealt with by the scope's next run, before new work, as runtime.state.failed_job_policy says: abandon
(the default) gives it up; retry resumes it from the status it failed from while it has been started fewer than
max_job_attempts times; block (and retry after the last attempt) stops the job until it is retried (`state retry`) or
abandoned (`state abandon`). A relay
failure is reported on its own and never marks a batch failed. The lease is renewed by a heartbeat while a batch
runs; a worker that lost it stops at its next fenced write (StaleLeaseError).
"""

import datetime
import hashlib
import json
import logging
import os
import socket
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from streamwright.core.jobs.heartbeat import Heartbeat
from streamwright.core.jobs.locking import FailedJobPolicy, check_key, scope_token
from streamwright.core.jobs.state import LeaseHeldError, StaleLeaseError
from streamwright.core.jobs.types import BatchContext, Broker, ExecutionHooks, Message
from streamwright.core.runtime.logs import fields, log_context

__all__ = [
    "DEFAULT_MAX_MESSAGES",
    "ERROR_TEXT_LIMIT",
    "MODES",
    "BrokerCall",
    "RelayError",
    "Runner",
    "RunnerError",
    "batch_id",
    "log_event",
    "owner_id",
]

LOG = logging.getLogger("streamwright.jobs.runner")
ERROR_TEXT_LIMIT = 2000
MODES = ("once", "drain", "available_now", "follow")
DEFAULT_MAX_MESSAGES = 500


class RunnerError(Exception):
    """The job stopped: a batch failed, or a failed batch waits for retry or abandon."""


class RelayError(RunnerError):
    """Publishing the outbox failed; the batches stay acked and are relayed by the next run."""


@dataclass(frozen=True)
class BrokerCall:
    """A broker with the arguments of the step (consume) or output (publish) that uses it."""
    broker: Broker
    arguments: Mapping[str, Any]


_WARNING_EVENTS = frozenset({"batch_failed", "lease_held", "lease_lost", "stopped", "release_failed", "interrupted",
                             "retry", "abandoned"})


def log_event(event, **values):
    """A runner event as a log line: `completed: job_id=...`, with `event` and `job_id` as fields (JSON logs)."""
    detail = ", ".join("%s=%s" % (name, value) for name, value in values.items())
    LOG.log(logging.WARNING if event in _WARNING_EVENTS else logging.INFO, "%s%s", event,
            ": " + detail if detail else "", extra=fields(event=event, job_id=values.get("job_id")))


def owner_id():
    """A lease owner unique to this runner: host, process and a random part (two runs in one process differ)."""
    return "%s:%d:%s" % (socket.gethostname(), os.getpid(), uuid.uuid4().hex[:12])


def batch_id(job_name, scope, sequence):
    """A batch's id: `<job>-<sequence>`, with a short hash of the scope between them when there is one."""
    if not scope:
        return "%s-%08d" % (job_name, sequence)
    return "%s-%s-%08d" % (job_name, hashlib.sha256(scope.encode()).hexdigest()[:16], sequence)


def _canonical(value):
    return json.dumps(value, sort_keys=True, default=str)


def _now_iso():
    return datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Runner:
    _PIN_ATTEMPTS = 3

    def __init__(self, job_name, state, executor=None, consumer=None, publishers=None, *, redact=None,
                 lease_seconds=300, heartbeat_seconds=None, engine_metadata=None, owner=None, log=None,
                 clock=_now_iso, sleep=time.sleep, scope="", lock=None, prepare=None, failed_jobs=None):
        """
        consumer:   BrokerCall of the job's consume step, or None
        publishers: {output name: BrokerCall} for the outputs that publish events
        redact:     callable applied to every error text that is logged or stored
        scope:      runtime.state.scope ("" for none)
        lock:       a LockPlan (tokens, item tokens, policy, timeout; inputs(lease) -> a new batch's pinned inputs)
        prepare:    inputs -> the Executor for a batch with those pinned inputs (else `executor` runs every batch)
        failed_jobs: a FailedJobPolicy (default: abandon)
        """
        self.job_name, self.state, self.executor, self.scope = job_name, state, executor, scope or ""
        self.consumer, self.publishers = consumer, dict(publishers or {})
        self.redact = redact or (lambda text: text)
        self.lease_seconds = int(lease_seconds)
        self.heartbeat_seconds = heartbeat_seconds if heartbeat_seconds is not None else max(1, self.lease_seconds // 3)
        self.engine_metadata = dict(engine_metadata or {})
        self.owner = owner or owner_id()
        self.log = log or log_event
        self.clock, self.sleep = clock, sleep
        self.lock, self.prepare = lock, prepare
        self.failed_jobs = failed_jobs or FailedJobPolicy()
        self.lease = None
        self.inputs = None                   # the pinned inputs of this run's new batches
        self._executors = {}
        self.processed = []                  # batch ids this run processed (new and resumed)
        self.skipped = False                 # the lock policy pruned all this run's work
        self.error = None                    # the (redacted) reason the run stopped

    # --- public ---------------------------------------------------------------------------------------------

    def acquire(self):
        """
        Takes the lease: the scope's token, the lock's tokens and the tokens the scope's unfinished batches pinned
        (needed), and the lock's item tokens (prunable). Raises LeaseHeldError (policy fail, or wait timed out).
        """
        lock = self.lock
        policy, timeout = (lock.policy, lock.timeout) if lock else ("fail", 0.0)
        for _ in range(self._PIN_ATTEMPTS):
            pinned = self._pinned_tokens()
            needed = tuple(dict.fromkeys((*(lock.tokens if lock else (scope_token(self.scope),)), *pinned)))
            self.lease = self.state.acquire_lease(self.job_name, self.owner, self.lease_seconds, tokens=needed,
                                                  optional=lock.item_tokens if lock else (), policy=policy,
                                                  timeout=timeout)
            if not self.lease.epoch or self._pinned_tokens() <= set(self.lease.tokens):
                return self.lease
            self._release()                 # an unfinished batch appeared while we waited: take its tokens too
        raise LeaseHeldError("job %s: the tokens of its unfinished batches keep changing" % self.job_name)

    def _pinned_tokens(self):
        pinned = set()
        for batch in self.state.list_incomplete_batches(self.job_name, self.scope):
            pinned.update(((batch.get("tokens") or {}).get("accepted")) or ())
        return pinned

    def run(self, mode="once", interval=10, max_messages=DEFAULT_MAX_MESSAGES, job_key=None, max_wait=30):
        """
        mode: once | drain | available_now | follow (all but once need a consumer: without one every batch is new
        work). max_messages 0 = recover and relay only. Returns an exit code.
        """
        self._check_run(mode, max_messages, job_key)
        try:
            if self.lease is None:
                self.acquire()
        except LeaseHeldError as exc:
            return self._stop("lease_held", exc)
        if not self.lease.epoch:
            return self._skip()
        try:
            if self.lease.pruned_tokens:
                self.log("lock_pruned", accepted=[t for t in self.lease.tokens if t],
                         pruned=list(self.lease.pruned_tokens))
            self.inputs = self.lock.inputs(self.lease) if self.lock else None
            self._recover()
            self._relay()
            if max_messages == 0:
                return 0
            if self.lock and self.inputs is None:
                return self._skip()
            watermarks = self._watermarks() if mode == "available_now" else None
            while True:
                self.lease = self.state.renew_lease(self.lease, self.lease_seconds)
                processed = self._next_batch(max_messages, job_key, max_wait, watermarks)
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

    def _check_run(self, mode, max_messages, job_key):
        if mode not in MODES:
            raise ValueError("unknown run mode %r (%s)" % (mode, ", ".join(MODES)))
        if mode != "once" and self.consumer is None and max_messages:
            raise ValueError("run mode %s needs a broker consumer" % mode)
        if job_key is not None:
            check_key("the job key", job_key)

    def _skip(self):
        """Records a run the lock policy pruned entirely (status skipped) and returns 0."""
        self.skipped = True
        accepted = [t for t in self.lease.tokens if t] if self.lease.epoch else []
        skipped = list(self.lease.pruned_tokens)
        name = "%s-skipped-%s" % (batch_id(self.job_name, self.scope, 0)[:-9], uuid.uuid4().hex[:12])
        self.state.record_skipped(self.job_name, name, {"batch_ts": self.clock()}, scope=self.scope,
                                  tokens={"accepted": accepted, "skipped": skipped})
        self.log("skipped", job_id=name, pruned=skipped)
        return 0

    def _stop(self, event, exc):
        self.error = self.redact(str(exc))
        self.log(event, error=self.error)
        return 1

    def _release(self):
        try:
            if self.lease is not None and self.lease.epoch:
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
        for batch in self.state.list_incomplete_batches(self.job_name, self.scope):
            status = batch["status"]
            if status == "failed":
                status = self._failed(batch)
                if status is None:
                    continue
            else:
                self.log("resume", job_id=batch["batch_id"], status=status)
            self._process(batch["batch_id"], batch["batch_sequence"], batch["manifest"], status)

    def _failed(self, batch):
        """
        A failed batch under the failed-job policy: abandoned (None), moved back to work (its status), or RunnerError.
        """
        name, error, policy = batch["batch_id"], batch.get("error") or "no detail", self.failed_jobs
        if policy.policy == "abandon":
            self.state.record_batch(self.lease, name, "abandoned", batch["manifest"], error=batch.get("error"))
            key = batch["job_key"] and {"job_key": "%s (a later run with this key does nothing)" % batch["job_key"]}
            self.log("abandoned", job_id=name, reason="failed job policy abandon", **(key or {}), error=error)
            return None
        reason = policy.refusal(batch)
        if reason:
            raise RunnerError("batch %s failed earlier and is not retried (%s): retry or abandon it (`state retry` or "
                              "`state abandon` with its job id); its error: %s" % (name, reason, error))
        status = self.state.retry_batch(self.lease, name)
        attempt = "%d of %d" % (int(batch.get("attempts") or 1) + 1, policy.max_attempts)
        self.log("retry", job_id=name, attempt=attempt, resumes_from=status, error=error)
        return status

    def _executor_for(self, manifest):
        """The executor of a batch: built from its pinned inputs (else this run's) when the engine prepares them."""
        if self.prepare is None:
            return self.executor
        inputs = manifest.get("inputs") or self.inputs
        key = _canonical(inputs)
        if key not in self._executors:
            self._executors[key] = self.prepare(inputs)
        return self._executors[key]

    # --- one new batch --------------------------------------------------------------------------------------

    def _next_batch(self, max_messages, job_key, max_wait, watermarks):
        messages = []
        if self.consumer is not None:
            broker, arguments = self.consumer.broker, self.consumer.arguments
            messages = broker.poll(arguments, int(broker.poll_limit(arguments, max_messages)),
                                   broker.poll_wait_seconds(arguments, max_wait), watermarks=watermarks)
            if not messages:
                return False
            job_key = None
        elif job_key is not None and self.state.find_batch_by_job_key(self.job_name, job_key, self.scope):
            self.log("skip", job_key=job_key, reason="a batch with this job key already ran")
            return False
        sequence = self.state.next_batch_sequence(self.lease, self.scope)
        name = batch_id(self.job_name, self.scope, sequence)
        manifest = {"batch_ts": self.clock(), "pinned": {}, "carry": self._carried(),
                    "messages": [m.to_dict() for m in messages]}
        if self.inputs is not None:
            manifest["inputs"] = json.loads(_canonical(self.inputs))
        fixed = [t for t in (self.lock.tokens if self.lock else ()) if t]
        tokens = {"accepted": fixed + list((self.inputs or {}).get("tokens") or ()),
                  "skipped": list(self.lease.pruned_tokens)}
        self.state.record_batch(self.lease, name, "received", manifest, batch_sequence=sequence, scope=self.scope,
                                job_key=job_key, tokens=tokens, engine_metadata=self.engine_metadata or None)
        self._process(name, sequence, manifest, "received")
        return True

    def _carried(self):
        """What the scope's last completed batch carries to the next one (e.g. source bookmarks)."""
        batch = self.state.find_latest_completed_batch(self.job_name, self.scope)
        return dict(batch["manifest"].get("carry") or {}) if batch else {}

    def _process(self, batch_id, sequence, manifest, status):
        with log_context(job_id=batch_id, scope=self.scope):
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
                self.log("batch_failed", job_id=batch_id, error=text)
                self.state.record_batch(self.lease, batch_id, "failed", manifest, error=text)
                raise RunnerError("batch %s failed: %s" % (batch_id, text)) from None
            self._relay()
            self.state.record_batch(self.lease, batch_id, "completed", manifest)
            self.log("completed", job_id=batch_id)

    def _execute(self, batch_id, sequence, manifest, status):
        if status == "received":
            self.state.record_batch(self.lease, batch_id, "processing", manifest)
        context = BatchContext(batch_id, sequence, manifest["batch_ts"], self.job_name, self.lease.epoch,
                               self.state.store_identity(), self.engine_metadata, self.scope)
        executor = self._executor_for(manifest)
        done = self.state.get_receipts(self.job_name, batch_id)
        with Heartbeat(self, self.heartbeat_seconds) as heartbeat:
            def fence():
                if heartbeat.lost is not None:
                    raise StaleLeaseError("lease renewal failed: %s" % heartbeat.lost)
                self.state.check_lease(self.lease)

            hooks = ExecutionHooks(
                fence=fence,
                save_manifest=lambda pinned: self.state.record_batch(self.lease, batch_id, "processing", pinned),
                record_receipt=lambda receipt, pinned: self.state.record_receipt(self.lease, batch_id, receipt,
                                                                                 pinned))
            result = executor.execute(context, manifest, done, hooks)
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
            self.log("batch_files", job_id=batch_id, read=len(result.schema_reports) - rejected, rejected=rejected,
                     drifted=sum(1 for r in result.schema_reports if r.status == "drift"))

    # --- outbox ---------------------------------------------------------------------------------------------

    def _relay(self):
        groups = {}
        for row in self.state.list_pending_events(self.job_name, self.scope):
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
            self.log("relayed", job_id=batch_id, output=output_name, events=len(rows))
