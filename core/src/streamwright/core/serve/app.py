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
The HTTP API of `streamwright serve` (plan §5.10): the job CLI's commands as endpoints.

    GET  /v1/jobs                          the caller's jobs: what each lets it set, and whether it takes rows
    POST /v1/jobs/{job}/runs               duckdb run: {"set": {...}, "input": [...], "options": {"async": false}}
    POST /v1/jobs/{job}/validate           duckdb validate --set ...
    GET  /v1/jobs/{job}/plan               duckdb plan
    GET  /v1/jobs/{job}/status             the caller's latest runs of the job
    GET  /v1/runs/{run_id}                 a run: queued, running, succeeded or failed, with its summary
    GET  /healthz, /readyz, /openapi.json  liveness, readiness (the state store answers), the caller's API
    GET  /docs                             Swagger UI over the caller's API, with `docs: {enabled: true}` (a page
                                           without data; otherwise 404 like any unknown path)
    GET  /metrics                          Prometheus metrics of the caller's jobs, with `metrics: {enabled: true}`

A run is queued, then the request waits up to limits.wait_seconds for it (200 with the run when it ends, else 202
with its Location); `options.async` answers 202 at once. Every response carries X-Request-ID (the caller's when it
is 1-128 of A-Z a-z 0-9 . _ -, else a new one). Bodies are JSON, read up to the job's limit (413 beyond); query
strings are refused (400). Errors are {"status": "error", "code", "message", "request_id"}: nothing internal.
"""

import logging
import re
import time
from types import MappingProxyType

from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from streamwright.core.runtime.logs import log_context
from streamwright.core.runtime.logs.context import new_request_id
from streamwright.core.serve.auth import Authenticator
from streamwright.core.serve.docs import docs_page
from streamwright.core.serve.envelope import RequestError
from streamwright.core.serve.metrics import CONTENT_TYPE, HttpMetrics, render
from streamwright.core.serve.openapi import openapi
from streamwright.core.serve.service import view

__all__ = ["REQUEST_ID", "create_app"]

LOG = logging.getLogger("streamwright.serve")
ACCESS = logging.getLogger("streamwright.serve.access")
REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_HEADERS = MappingProxyType({"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"})


def _json(status, body, request_id, headers=None):
    return JSONResponse(body, status_code=status, headers={**_HEADERS, **(headers or {}), "X-Request-ID": request_id})


async def _body(request, limit):
    """The request body, at most `limit` bytes (413 beyond, before reading the rest); JSON only."""
    length = request.headers.get("content-length")
    if length is not None and (not length.isdigit() or int(length) > limit):
        raise RequestError(413, "too-large", "the request body is larger than %d bytes" % limit)
    chunks, size = [], 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise RequestError(413, "too-large", "the request body is larger than %d bytes" % limit)
        chunks.append(chunk)
    body = b"".join(chunks)
    kind = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if body.strip() and kind != "application/json":
        raise RequestError(415, "unsupported-media-type", "the request body is JSON (Content-Type: application/json)")
    return body


class _Api:
    def __init__(self, service, authenticator):
        self.service, self.auth, self.http = service, authenticator, HttpMetrics()

    async def handle(self, request, operation, *, authenticate=True, route="-"):
        started = time.monotonic()
        given = request.headers.get("x-request-id", "")
        request_id = given if REQUEST_ID.match(given) else new_request_id()
        principal_id, status = "-", 500
        with log_context(request_id=request_id):
            try:
                if request.url.query:
                    raise RequestError(400, "query-not-allowed", "query strings are not accepted; send JSON bodies")
                principal = None
                if authenticate:                  # in a thread: a JWT may need the issuer's keys (JWKS) fetched
                    principal = await run_in_threadpool(self.auth.authenticate, request.headers.get("authorization"))
                principal_id = principal.id if principal is not None else "-"
                result = await operation(request, principal, request_id)
                if isinstance(result, Response):       # a page (/docs), not a JSON body
                    response, status = result, result.status_code
                    response.headers["X-Request-ID"] = request_id
                else:
                    status, body, headers = result
                    response = _json(status, body, request_id, headers)
            except RequestError as exc:
                status = exc.status
                response = _json(exc.status, {"status": "error", "code": exc.code, "message": exc.message,
                                              "request_id": request_id}, request_id, exc.headers)
            except Exception:
                LOG.exception("request %s failed", request_id)
                status = 500
                response = _json(500, {"status": "error", "code": "internal", "message": "the request failed; the "
                                       "operator's log has the details", "request_id": request_id}, request_id)
            seconds = time.monotonic() - started
            self.http.record(route, request.method, status, seconds)
            ACCESS.info("%s %s %s %d %.0fms", request.method, request.url.path, principal_id, status, seconds * 1000,
                        extra={"principal": principal_id, "status": status})
        return response

    # --- endpoints --------------------------------------------------------------------------------------------

    async def jobs(self, request, principal, request_id):
        granted = [self.service.describe(self.service.jobs[name]) for name in self.service.granted(principal)]
        return 200, {"jobs": granted}, None

    async def runs(self, request, principal, request_id):
        name = request.path_params["job"]
        served = self.service.job(name, principal)
        body = await _body(request, self.service.body_limit(served))
        run, _, envelope = await run_in_threadpool(
            self.service.submit, name, principal, body, idempotency_key=request.headers.get("idempotency-key"),
            request_id=request_id)
        if not envelope.run_async and served.entry.wait_seconds:
            run = await self.service.wait(run, served.entry.wait_seconds)
        ended = run["status"] in ("succeeded", "failed")
        return (200 if ended else 202), view(run), None if ended else {"Location": "/v1/runs/%s" % run["run_id"]}

    async def validate(self, request, principal, request_id):
        name = request.path_params["job"]
        body = await _body(request, self.service.body_limit(self.service.job(name, principal)))
        return 200, await run_in_threadpool(self.service.validate, name, principal, body), None

    async def plan(self, request, principal, request_id):
        return 200, await run_in_threadpool(self.service.plan, request.path_params["job"], principal), None

    async def status(self, request, principal, request_id):
        runs = await run_in_threadpool(self.service.status, request.path_params["job"], principal)
        return 200, {"runs": runs}, None

    async def run(self, request, principal, request_id):
        run = await run_in_threadpool(self.service.run_state, request.path_params["run_id"], principal)
        return 200, view(run), None

    async def openapi(self, request, principal, request_id):
        granted = [self.service.jobs[name] for name in self.service.granted(principal)]
        return 200, openapi(granted, bool(self.service.config.has_auth)), None

    async def docs(self, request, principal, request_id):
        return docs_page(bool(self.service.config.has_auth))

    async def metrics(self, request, principal, request_id):
        try:
            text = await run_in_threadpool(render, self.service.store, self.service.granted(principal), self.http)
        except Exception as exc:
            LOG.warning("metrics: the state store does not answer: %s", exc)
            raise RequestError(503, "not-ready", "the state store does not answer") from None
        return Response(text, media_type=CONTENT_TYPE, headers=dict(_HEADERS))

    async def healthz(self, request, principal, request_id):
        return 200, {"status": "ok"}, None

    async def readyz(self, request, principal, request_id):
        try:
            await run_in_threadpool(self.service.store.count_requests, ("queued",))
        except Exception as exc:
            LOG.warning("not ready: the state store does not answer: %s", exc)
            raise RequestError(503, "not-ready", "the state store does not answer") from None
        return 200, {"status": "ready"}, None


def create_app(service, authenticator=None):
    """The Starlette app of a serve.Service."""
    api = _Api(service, authenticator or Authenticator(service.config))

    def route(path, operation, method, authenticate=True):
        async def endpoint(request):
            return await api.handle(request, operation, authenticate=authenticate, route=path)
        return Route(path, endpoint, methods=[method])

    async def not_found(request, exc):
        request_id = new_request_id()
        status = getattr(exc, "status_code", 404)
        return _json(status, {"status": "error", "code": "not-found" if status == 404 else "method-not-allowed",
                              "message": "no such endpoint" if status == 404 else "method not allowed",
                              "request_id": request_id}, request_id)

    return Starlette(exception_handlers={404: not_found, 405: not_found}, routes=[
        route("/healthz", api.healthz, "GET", authenticate=False),
        route("/readyz", api.readyz, "GET", authenticate=False),
        route("/openapi.json", api.openapi, "GET"),
        *([route("/docs", api.docs, "GET", authenticate=False)] if service.config.docs else []),
        *([route("/metrics", api.metrics, "GET")] if service.config.metrics else []),
        route("/v1/jobs", api.jobs, "GET"),
        route("/v1/jobs/{job}/runs", api.runs, "POST"),
        route("/v1/jobs/{job}/validate", api.validate, "POST"),
        route("/v1/jobs/{job}/plan", api.plan, "GET"),
        route("/v1/jobs/{job}/status", api.status, "GET"),
        route("/v1/runs/{run_id}", api.run, "GET"),
    ])
