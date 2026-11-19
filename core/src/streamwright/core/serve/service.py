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
The service behind the HTTP API, without the HTTP: the jobs serve.yaml lists, checked once at start; who may call
which; and the CLI commands as operations - run (queue it, then wait or not), validate, plan, status, a run's state.
Every operation takes the caller (a Principal) and raises RequestError with the HTTP status a refusal maps to.
"""

import asyncio
import datetime
import logging
import re
import time
from types import MappingProxyType

from streamwright.core.jobs.duckdb.engine import validate_job
from streamwright.core.jobs.state.requests import RateLimitedError
from streamwright.core.serve.config import ServiceError
from streamwright.core.serve.envelope import RequestError, read_envelope, scope_key
from streamwright.core.serve.rules import served_job

__all__ = ["IDEMPOTENCY_KEY", "MAX_QUEUED", "Service"]

LOG = logging.getLogger("streamwright.serve")
IDEMPOTENCY_KEY = re.compile(r"^[\x21-\x7e]{1,128}$")
MAX_QUEUED = 1000                                 # queued runs (all jobs): beyond, 503 and Retry-After
_RETRY_AFTER_SECONDS = 30
_POLL_SECONDS = 0.25
_SET_ONLY_BYTES = 64 * 1024                       # a body without input rows
_ENVELOPE_BYTES = 64 * 1024                       # what `set` and `options` add to the rows


def _issue(issue):
    return {"severity": issue.severity, "code": issue.code, "path": issue.path, "message": issue.message}


class Service:
    """A loaded serve.yaml (config.Service) with its jobs checked, and its state store (the run queue)."""

    def __init__(self, config, store, *, clock=time.monotonic):
        self.config, self.store, self._clock = config, store, clock
        jobs, problems = {}, []
        for name, entry in config.jobs.items():
            served, found, warnings = served_job(entry)
            problems.extend(found)
            for warning in warnings:
                LOG.warning("%s", warning)
            if served is not None:
                jobs[name] = served
        if problems:
            raise ServiceError(problems)
        self.jobs = MappingProxyType(jobs)
        keys = {key.id for key in config.api_keys}
        # a `principals` entry naming an API key grants that key; any other names a JWT subject
        self._grantees = MappingProxyType({name: frozenset(("key:" if listed in keys else "jwt:") + listed
                                                           for listed in served.entry.principals)
                                           for name, served in jobs.items()})

    # --- who may call what ------------------------------------------------------------------------------------

    def granted(self, principal):
        return [name for name in sorted(self.jobs) if self._may(principal, name)]

    def _may(self, principal, name):
        return not self.config.has_auth or principal.id in self._grantees[name]

    def job(self, name, principal):
        """The ServedJob `name` for `principal`: 404 when it is not listed, 403 when it is not granted."""
        served = self.jobs.get(name)
        if served is None:
            raise RequestError(404, "not-found", "no job %r" % name)
        if not self._may(principal, name):
            raise RequestError(403, "forbidden", "the caller may not use job %r" % name)
        return served

    def body_limit(self, served):
        """The largest request body the job takes, in bytes."""
        if served.input is None:
            return _SET_ONLY_BYTES
        rows = served.entry.payload_max_bytes or int(served.input["max_bytes"])
        return min(rows, int(served.input["max_bytes"])) + _ENVELOPE_BYTES

    def describe(self, served):
        """A job as the caller sees it: what it may set and whether it takes rows."""
        settable = {name: {key: value for key, value in (served.config[name] or {}).items()
                           if key in ("type", "items", "required", "default", "description", "enum", "pattern",
                                      "minimum", "maximum", "max_length")}
                    for name in sorted(served.settable)}
        return {"name": served.entry.name, "set": settable, "takes_input": served.input is not None,
                "input_columns": sorted((served.input or {}).get("columns") or ()),
                "idempotency_key": "required" if served.applies else "optional"}

    # --- the commands -----------------------------------------------------------------------------------------

    def submit(self, name, principal, body, *, idempotency_key=None, request_id):
        """Queues a run (`duckdb run`): (run, created). Same key and body: the existing run; another body: 422."""
        served = self.job(name, principal)
        if idempotency_key is not None and not IDEMPOTENCY_KEY.match(idempotency_key):
            raise RequestError(422, "invalid-idempotency-key", "Idempotency-Key is 1 to 128 printable ASCII "
                                                                "characters")
        if served.applies and idempotency_key is None:
            raise RequestError(428, "idempotency-key-required", "job %r applies operations to a vendor: send an "
                                                                 "Idempotency-Key header" % name)
        envelope = read_envelope(body, served, principal, served.secrets, self.body_limit(served))
        scope = scope_key(served, envelope.config)
        if idempotency_key is not None:
            existing = self._existing(name, principal, idempotency_key, envelope.fingerprint)
            if existing is not None:
                return existing, False, envelope
        if served.entry.on_busy == "reject":
            active = self.store.active_request(name, scope)
            if active is not None and active["principal"] == principal.id:
                raise RequestError(409, "busy", "a run of the same scope is %s: %s" % (
                    active["status"], active["run_id"]), {"Location": "/v1/runs/%s" % active["run_id"]})
            if active is not None:                # another caller's run: never named
                raise RequestError(409, "busy", "a run of the same scope is active: retry later")
        if self.store.count_requests(("queued",)) >= MAX_QUEUED:
            raise RequestError(503, "queue-full", "the run queue is full: retry later",
                               {"Retry-After": str(_RETRY_AFTER_SECONDS)})
        now = datetime.datetime.now(datetime.UTC)
        rate = served.entry.rate
        try:
            run, created = self.store.enqueue_request(
                job_name=name, principal=principal.id, body_sha256=envelope.fingerprint, inputs=envelope.config,
                request_id=request_id, idempotency_key=idempotency_key, scope_key=scope,
                payload=None if envelope.rows is None else list(envelope.rows),
                rate=None if rate is None else (rate[0], now - datetime.timedelta(seconds=rate[1])))
        except RateLimitedError as exc:
            raise self._rate_limited(name, exc.oldest, rate[1], now) from None
        if not created and run["body_sha256"] != envelope.fingerprint:
            raise self._conflict()
        LOG.info("queued run %s of job %s for %s", run["run_id"], name, principal.id)
        return run, created, envelope

    @staticmethod
    def _rate_limited(name, oldest, seconds, now):
        """
        429: the caller queued limits.rate runs of the job in the window. They are counted in the queue, with the new
        run, in one transaction, so the limit holds across every host that serves the job (a replayed
        Idempotency-Key is its run, not a new one). Retry-After: when the oldest counted run leaves the window.
        """
        ends = datetime.datetime.fromisoformat(oldest) + datetime.timedelta(seconds=seconds) if oldest else now
        return RequestError(429, "rate-limited", "too many requests for job %r: retry later" % name,
                            {"Retry-After": str(max(1, int((ends - now).total_seconds()) + 1))})

    def _existing(self, name, principal, key, fingerprint):
        found = self.store.find_request(name, principal.id, key)
        if found is not None and found["body_sha256"] != fingerprint:
            raise self._conflict()
        return found

    @staticmethod
    def _conflict():
        return RequestError(422, "idempotency-key-reused", "this Idempotency-Key was used with another request body")

    async def wait(self, run, seconds):
        """
        `run` once it ends, or as it is after `seconds`: a coroutine, which sleeps on the event loop and holds a
        thread only to read the run from the store.
        """
        deadline = self._clock() + seconds
        while run is not None and run["status"] in ("queued", "running") and self._clock() < deadline:
            await asyncio.sleep(min(_POLL_SECONDS, max(0.0, deadline - self._clock())))
            run = await asyncio.to_thread(self.store.get_request, run["run_id"])
        return run

    def run_state(self, run_id, principal):
        """A run of the caller (`GET /v1/runs/ID`); 404 for another caller's."""
        run = self.store.get_request(run_id)
        if run is None or (self.config.has_auth and run["principal"] != principal.id):
            raise RequestError(404, "not-found", "no run %r" % run_id)
        return run

    def validate(self, name, principal, body):
        """`duckdb validate --set ...`: the request checked, then the job with its values."""
        served = self.job(name, principal)
        envelope = read_envelope(body, served, principal, served.secrets, self.body_limit(served))
        report = validate_job(served.entry.manifest, config=envelope.config)
        return {"valid": report.valid, "issues": [_issue(issue) for issue in report.issues]}

    def plan(self, name, principal):
        """`duckdb plan`."""
        served = self.job(name, principal)
        report = validate_job(served.entry.manifest)
        return {"valid": report.valid, "plan": report.plan, "issues": [_issue(issue) for issue in report.problems]}

    def status(self, name, principal, limit=20):
        """`duckdb status`: the caller's latest runs of the job."""
        self.job(name, principal)
        return [view(run) for run in self.store.list_requests(job_name=name, principal=principal.id, limit=limit)]


def view(run):
    """A run as its caller sees it: never its inputs, rows or the operator's error message."""
    shown = {key: run[key] for key in ("run_id", "status", "attempts", "created_at", "started_at", "finished_at",
                                       "request_id")}
    shown["job"] = run["job_name"]
    if run["status"] in ("succeeded", "failed"):
        shown["result"] = run["result"]
    if run["status"] == "failed":
        shown["error"] = {"code": run["error_code"] or "run-failed",
                          "message": "the run failed; the operator's log has the details (request id %s)" %
                                     run["request_id"]}
    return shown
