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
`streamwright worker`: runs the runs `serve` queued (sw_requests), as `streamwright duckdb run` would, one at a time
per Worker (`--processes N`: N Workers, each on a thread of its own, their runs in child processes).

A worker claims the oldest run it may take (one per scope at once; at most limits.concurrency of a job across all
workers), renews its claim while the run goes on, and records the outcome. Each run executes in a child process,
killed when it outlasts limits.timeout, or when the worker loses its claim (the state store did not answer before
the claim would end, or another worker took the run over): the run is then left, unrecorded, to the worker that
takes it. The child ignores SIGINT and SIGTERM (Ctrl-C, a service manager's stop): the worker finishes the run under
way, then stops. A worker that stops (crash, restart) leaves its claim to expire; another worker then takes the run
over, and the job's own state resumes it: the run's job key is the caller and its Idempotency-Key (or the run id),
so batches already done are not done again.
Callers see an error code (`run-failed`, `invalid-input`, `invalid-job`, `timeout`); the redacted error goes to the
log and the run's row (`error_message`), for the operator.
"""

import datetime
import hashlib
import logging
import multiprocessing
import signal
import threading
import time

from streamwright.core.config.inputs import InputError
from streamwright.core.jobs.duckdb.engine import JobInvalidError, run_job
from streamwright.core.jobs.locking import MAX_KEY_LENGTH
from streamwright.core.jobs.runner import owner_id
from streamwright.core.runtime.logs import LogSetup, log_context
from streamwright.core.runtime.logs.lines import fields, milliseconds

__all__ = ["CLAIM_LOST", "CLAIM_SECONDS", "Worker", "default_worker_id", "execute_run", "job_key", "run_summary"]

LOG = logging.getLogger("streamwright.serve.worker")
CLAIM_SECONDS = 60
POLL_SECONDS = 1.0
MAX_ATTEMPTS = 10                                 # claims of a run whose job's lease another run holds
_DEFER_SECONDS = 30
_WATCH_SECONDS = 0.25                             # how often a running child's worker checks its claim
_STOP_SIGNALS = frozenset({signal.SIGINT, signal.SIGTERM})      # for the worker, not its child
CLAIM_LOST = "claim-lost"


def run_summary(result):
    """What a caller is told about a finished run: its status, batches and the rows each output wrote."""
    summary = {"status": "failed" if result.exit_code else ("skipped" if result.lock_skipped else "ok"),
               "job": result.job_name, "job_ids": list(result.batches),
               "outputs": [{"name": name, "rows": rows, "already_committed": name in result.skipped,
                            **({"rejected": result.rejected[name]} if result.rejected.get(name) else {})}
                           for name, rows in sorted(result.outputs.items())]}
    if result.fan_out:
        summary.update(fan_out=result.fan_out, passes=result.passes)
    return summary


def default_worker_id():
    """This process's worker id (its host and process id)."""
    return "worker:" + owner_id()


def _milliseconds(start, end):
    """The milliseconds between two of a run's ISO-8601 timestamps (None when either is missing)."""
    if not start or not end:
        return None
    return round((datetime.datetime.fromisoformat(end) - datetime.datetime.fromisoformat(start)).total_seconds()
                 * 1000)


def job_key(request):
    """
    The run's job key: its caller's Idempotency-Key (or its run id), scoped to the caller as the key is in the queue -
    two callers' runs with the same key are two runs. A digest when the text is longer than the key columns.
    """
    text = "serve:%s:%s" % (request["principal"], request["idempotency_key"] or request["run_id"])
    if len(text) <= MAX_KEY_LENGTH:
        return text
    return "serve:" + hashlib.sha256(text.encode()).hexdigest()


def _options(served, request):
    options = dict(served.entry.server_options)
    return {**options, "config": dict(request["inputs"] or {}), "input_rows": request["payload"],
            "request_id": request["request_id"], "job_key": job_key(request) if served.persistent else None,
            "log_row_values": False}          # serve never logs row values: a step's `log:` logs its row count


def execute_run(manifest, options):
    """Runs the job (in this process): (succeeded, summary, error code, error message)."""
    try:
        result = run_job(manifest, **options)
    except InputError as exc:
        return False, None, "invalid-input", str(exc)
    except JobInvalidError as exc:
        return False, None, "invalid-job", str(exc)
    except Exception as exc:                      # run_job redacts what it raises
        return False, None, "run-failed", "%s: %s" % (type(exc).__name__, exc)
    summary = run_summary(result)
    if result.exit_code:
        return False, summary, "lease-held" if result.lease_held else "run-failed", result.error or "the run failed"
    return True, summary, None, None


def _child(conn, manifest, options, log_settings):
    for signum in _STOP_SIGNALS:                  # blocked since the fork (see _in_child): ignored, then unblocked
        signal.signal(signum, signal.SIG_IGN)
    _mask(signal.SIG_UNBLOCK)
    setup = LogSetup(**log_settings)              # the worker's logging options: the run logs as `duckdb run` does
    try:
        conn.send(execute_run(manifest, options))
    finally:
        conn.close()
        setup.close()


def _mask(how):
    """Blocks or unblocks the stop signals in this thread (where the platform has signal masks)."""
    if hasattr(signal, "pthread_sigmask"):
        return signal.pthread_sigmask(how, _STOP_SIGNALS)
    return None


def _in_child(manifest, options, timeout, log_settings=None, *, lost=None):
    """
    execute_run in a child process, logging with `log_settings` (LogSetup's arguments); (False, None, "timeout", ...)
    when it outlasts `timeout` seconds, (False, None, CLAIM_LOST, ...) once `lost` (a threading.Event) is set: the
    child is killed. The child ignores SIGINT and SIGTERM - they are the worker's, which finishes the run first.
    """
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    process = context.Process(target=_child, args=(send, manifest, options, dict(log_settings or {})), daemon=True)
    blocked = _mask(signal.SIG_BLOCK)             # inherited by the child until it ignores them
    try:
        process.start()
    finally:
        if blocked is not None:
            signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
    send.close()
    deadline = time.monotonic() + timeout
    try:
        while not receive.poll(max(0.0, min(_WATCH_SECONDS, deadline - time.monotonic()))):
            if lost is not None and lost.is_set():
                return False, None, CLAIM_LOST, "the worker lost its claim on the run; the run was stopped"
            if time.monotonic() >= deadline:
                return False, None, "timeout", "the run did not end within %ds; it was stopped" % timeout
        return receive.recv()
    except EOFError:
        return False, None, "run-failed", "the run's process ended without an outcome (exit code %s)" % (
            process.exitcode,)
    finally:
        receive.close()
        if process.is_alive():                    # SIGKILL: it ignores SIGTERM
            process.kill()
        process.join()


class _Renewal(threading.Thread):
    """
    Renews a claim every third of its length until stopped. `lost` (an Event) is set when the run is no longer the
    worker's, or when the state store has not answered and the claim would end before the next renewal.
    """

    def __init__(self, store, run_id, worker_id, seconds):
        super().__init__(name="streamwright-claim-%s" % run_id, daemon=True)
        self.store, self.run_id, self.worker_id, self.seconds = store, run_id, worker_id, seconds
        self.stopped, self.lost = threading.Event(), threading.Event()

    def run(self):
        interval = max(self.seconds / 3, 0.5)
        renewed = time.monotonic()                # the claim was just taken
        while not self.stopped.wait(interval):
            started = time.monotonic()
            try:
                if not self.store.renew_request(self.run_id, self.worker_id, self.seconds):
                    LOG.warning("run %s is no longer this worker's: it is stopped", self.run_id)
                    self.lost.set()
                    return
                renewed = started
            except Exception as exc:              # a database blip: the claim stays until it expires
                LOG.warning("could not renew the claim of run %s: %s", self.run_id, exc)
                if time.monotonic() + interval - renewed >= self.seconds:
                    LOG.warning("run %s: its claim ends before it can be renewed: the run is stopped", self.run_id)
                    self.lost.set()
                    return


class Worker:
    """Claims and runs queued runs of `jobs` ({name: ServedJob}) from `store` (a SqlStateStore)."""

    def __init__(self, jobs, store, *, worker_id=None, claim_seconds=CLAIM_SECONDS, poll_seconds=POLL_SECONDS,
                 execute=_in_child, log_settings=None):
        """
        `execute(manifest, options, timeout, log_settings, lost=event)` runs one run (default: in a child process),
        stopping it once `lost` is set.
        """
        self.jobs, self.store, self.log_settings = dict(jobs), store, dict(log_settings or {})
        self.worker_id = worker_id or default_worker_id()
        self.claim_seconds, self.poll_seconds, self.execute = claim_seconds, poll_seconds, execute
        self.concurrency = {name: served.entry.concurrency for name, served in self.jobs.items()
                            if served.entry.concurrency}

    def run_one(self):
        """Claims one run and runs it; returns its run id, or None when there was nothing to claim."""
        request = self.store.claim_request(self.worker_id, self.claim_seconds, concurrency=self.concurrency)
        if request is None:
            return None
        served = self.jobs.get(request["job_name"])
        with log_context(request_id=request["request_id"], job=request["job_name"]):
            if served is None:
                self._finish(request, (False, None, "unknown-job", "the service no longer lists job %r" %
                                       request["job_name"]))
                return request["run_id"]
            LOG.info("run %s: job %s, attempt %d", request["run_id"], request["job_name"], request["attempts"])
            renewal = _Renewal(self.store, request["run_id"], self.worker_id, self.claim_seconds)
            renewal.start()
            started = time.monotonic()
            try:
                outcome = self.execute(served.entry.manifest, _options(served, request), served.entry.timeout_seconds,
                                       self.log_settings, lost=renewal.lost)
            finally:
                renewal.stopped.set()
                renewal.join()
            if outcome[2] == CLAIM_LOST:
                LOG.warning("run %s: the claim was lost; the run was stopped and is left to the worker that takes it "
                            "over", request["run_id"])
            else:
                self._finish(request, outcome, time.monotonic() - started)
        return request["run_id"]

    def _finish(self, request, outcome, seconds=None):
        succeeded, summary, code, message = outcome
        if code == "lease-held" and request["attempts"] < MAX_ATTEMPTS:
            # another run (or a stopped worker's, until its lease ends) holds the job's lease: wait, not fail
            seconds = min(_DEFER_SECONDS * request["attempts"], 300)
            if self.store.defer_request(request["run_id"], self.worker_id, seconds):
                LOG.info("run %s: the job's lease is held (%s); retrying in %ds", request["run_id"], message, seconds)
                return
        if message:
            LOG.warning("run %s failed (%s): %s", request["run_id"], code, message)
        recorded = self.store.finish_request(request["run_id"], self.worker_id, succeeded=succeeded,
                                             result=summary, error_code=code, error_message=message)
        if not recorded:
            LOG.warning("run %s was taken over by another worker: its outcome is that worker's", request["run_id"])
            return
        status = "succeeded" if succeeded else "failed"
        wait = _milliseconds(request["created_at"], request["started_at"])
        LOG.info("run %s %s (attempt %d, queued %s ms, ran %s ms)", request["run_id"], status, request["attempts"],
                 wait, None if seconds is None else milliseconds(seconds),
                 extra=fields(event="serve_run", run_id=request["run_id"], status=status, code=code,
                              attempt=request["attempts"], queue_wait_ms=wait,
                              duration_ms=None if seconds is None else milliseconds(seconds)))

    def run(self, stop):
        """Runs queued runs until `stop` (a threading.Event) is set; a run under way is finished first."""
        LOG.info("worker %s: waiting for runs of %s", self.worker_id, ", ".join(sorted(self.jobs)) or "no job")
        while not stop.is_set():
            try:
                ran = self.run_one()
            except Exception as exc:              # the state store is unreachable: wait and try again
                LOG.warning("worker %s: %s", self.worker_id, exc)
                ran = None
            if ran is None:
                stop.wait(self.poll_seconds)
        LOG.info("worker %s: stopped", self.worker_id)
