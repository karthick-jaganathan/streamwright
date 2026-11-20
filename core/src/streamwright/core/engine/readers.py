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

"""Transport selection by request kind for source runs."""

import copy
import logging
import tempfile
from collections.abc import Iterator
from functools import partial
from types import MappingProxyType
from typing import Protocol
from urllib.parse import urlsplit

from streamwright.core.config.inputs import parse_duration
from streamwright.core.engine.errors import SourceError
from streamwright.core.engine.responses import as_scope, first_response, lookup, matches, summarize
from streamwright.core.engine.rules import transport_problem
from streamwright.core.io import downloads
from streamwright.core.net.encoding import encode_params
from streamwright.core.net.http import merge_headers, paginate
from streamwright.core.net.paginators import set_dotted_path
from streamwright.core.runtime import components, logs
from streamwright.core.runtime.templates import get_path, render, to_text

LOG = logging.getLogger("streamwright.source")


class RequestReader(Protocol):
    def pages(self, runner, stream, request, body, *, client, context, scopes, select, where) -> Iterator[list]:
        """Yield one list of records per transport response."""


def select_records(request, response):
    """Records at the configured path of one response, preserving missing-path diagnostics."""
    path = (request.get("records") or {}).get("path")
    if not path:
        data = response
    else:
        try:
            data = get_path(response, path)
        except (KeyError, TypeError, IndexError):
            if isinstance(response, dict) and response and path.split(".")[0] not in response:
                raise SourceError("records path %r not found in the response" % (path,)) from None
            return []
    if data is None:
        return []
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise SourceError("records at %r are %s, expected a list" % (path, type(data).__name__))


def explode_records(records, field):
    for record in records:
        if not field:
            yield record
            continue
        try:
            children = get_path(record, field)
        except (KeyError, TypeError, IndexError):
            continue
        if not isinstance(children, list):
            continue
        parent = {key: value for key, value in record.items() if key != field}
        for child in children:
            merged = dict(parent)
            merged.update(child if isinstance(child, dict) else {field: child})
            yield merged


def read_pages(runner, stream, request, client, context, *, scopes, where=None, read=None):
    """Select the reader from the request body and count the pages of this read."""
    body = components.request_body(request)
    effective_records = request["records"] if "records" in request else runner.http.get("records")
    explode = (effective_records or {}).get("explode")
    select = partial(select_records, {"records": effective_records})
    kind = next(kind for kind in READERS if kind in body)
    pages = READERS[kind].pages(runner, stream, request, body, client=client, context=context, scopes=scopes,
                                select=select, where=where)
    if explode:
        pages = (list(explode_records(page, explode)) for page in pages)
    return pages if read is None else read.count(pages)


class HttpReader:
    def pages(self, runner, stream, request, body, *, client, context, scopes, select, where):
        return self.http_pages(runner, body["http"], request, client=client, scopes=scopes, where=where)

    @staticmethod
    def prepared(runner, request, scopes):
        """(the rendered request, its headers merged onto the source's, its params encoding)."""
        encoding = request.get("params_encoding") or runner.http.get("params_encoding", "plain")
        unrendered = dict(request)
        headers_spec = unrendered.pop("headers", None)
        http = runner.query_builders.build(unrendered, render(unrendered, scopes))
        headers = render(headers_spec, dict(scopes, secrets=runner.secrets)) if headers_spec else {}
        return http, merge_headers(runner.http.get("headers"), headers), encoding

    def response(self, runner, request, *, client, scopes, where=None):
        """The decoded body of one request, unpaginated (an async job's submit or poll)."""
        http, headers, encoding = self.prepared(runner, request, scopes)
        return client.request(http.get("method", "GET"), http["path"],
                              params=encode_params(dict(http.get("params") or {}), encoding), headers=headers,
                              json_body=http.get("json"), fields=where)

    def http_pages(self, runner, request, item, *, client, scopes, where=None):
        effective_paginator = item["paginator"] if "paginator" in item else runner.http.get("paginator")
        records = item["records"] if "records" in item else runner.http.get("records")
        http, merged_headers, encoding = self.prepared(runner, request, scopes)

        def send(patch):
            if patch.url:
                url = client.follow(patch.url)
                response = client.request(http.get("method", "GET"), url, headers=merged_headers, fields=where)
            else:
                params = dict(http.get("params") or {})
                if patch.params:
                    params.update(patch.params)
                json_body = http.get("json")
                if patch.body:
                    json_body = copy.deepcopy(json_body) if json_body is not None else {}
                    for path, value in patch.body.items():
                        set_dotted_path(json_body, path, value)
                response = client.request(http.get("method", "GET"), http["path"],
                                          params=encode_params(params, encoding), headers=merged_headers,
                                          json_body=json_body, fields=where)
            send.last_headers = client.last_headers
            return response

        def paginator_for_response(response):
            return render(effective_paginator, dict(scopes, response=response or {})) \
                if effective_paginator is not None else None

        select_item = {"records": records}
        for _, page in paginate(send, paginator_for_response,
                                lambda response: select_records(select_item, response)):
            yield page

class SdkReader:
    def pages(self, runner, stream, request, body, *, client, context, scopes, select, where):
        if runner.transport == "http":
            raise SourceError(transport_problem("sdk", runner.transport, runner.provider))
        return self.responses(runner, body, context, scopes, where=where, select=select)

    def responses(self, runner, request, context, scopes, *, where=None, select=None):
        unrendered = components.sdk_call(request)
        call = runner.query_builders.build(unrendered, render(unrendered, scopes))
        call["arguments"] = call.get("arguments") or {}
        name = "%s.%s" % (call.get("service"), call.get("method"))
        responses, number = None, 0
        try:
            while True:
                context.where = where
                started = runner.clock()
                try:
                    if responses is None:
                        responses = iter(runner.connector_session.read(call, context))
                    response = next(responses)
                except StopIteration:
                    return
                number += 1
                page = response if select is None else select(response)
                logs.sdk_summary(runner.provider, name, where, number, None if select is None else len(page),
                                 seconds=runner.clock() - started, attempt=context.attempt or 1)
                yield page
        finally:
            close = getattr(responses, "close", None)
            if close is not None:
                close()


class AsyncJobReader:
    """
    submit -> poll until done -> results (read like a request) or a download. Each part is what its operation is: a
    connector call, or an HTTP request through the source's client (REST connectors).
    """

    def pages(self, runner, stream, request, body, *, client, context, scopes, select, where):
        return self.job_pages(runner, stream, body["async_job"], context=context, scopes=scopes, select=select,
                              where=where, client=client, item=request)

    @staticmethod
    def _one(runner, part, *, client, context, scopes, where):
        """
        The one response of a submit or poll part. A connector that reads in pages of records gives one object as a
        page of one record: the record is the response.
        """
        if "http" in part:
            return READERS["http"].response(runner, part["http"], client=client, scopes=scopes, where=where)
        response = first_response(runner.connector_session.responses(part, context, scopes, where=where))
        if isinstance(response, list) and len(response) == 1 and isinstance(response[0], dict):
            return response[0]
        return response

    def job_pages(self, runner, stream, job, *, context, scopes, select, where=None, client=None, item=None):
        name = stream["name"]

        def step(label):                # a call's where, naming its step of the job (logged on its network line)
            return dict(where or {"stream": name}, job_step=label)
        submit = job["submit"]
        submitted = self._one(runner, submit, client=client, context=context, scopes=scopes, where=step("submit"))
        scopes = dict(scopes, submit=as_scope(submitted))
        job_text = runner.redact(to_text(submitted)) if submitted is not None and not isinstance(
            submitted, (dict, list)) else summarize(submitted, runner.redact)
        LOG.info("stream %r: submitted the job: %s", name, job_text,
                 extra=logs.fields(event="job_submitted", job_request=job_text, **(where or {"stream": name})))
        scopes["poll"] = self.poll(runner, name, job["poll"], submit, context=context, scopes=scopes, where=where,
                                   client=client)
        results = job.get("results")
        if results is not None and "http" in results:
            paging = {key: value for key, value in ((item or {}) | results).items() if key in ("records", "paginator")}
            yield from READERS["http"].http_pages(runner, results["http"], paging, client=client, scopes=scopes,
                                                  where=step("results"))
            return
        if results is not None:
            yield from runner.connector_session.responses(results, context, scopes, where=step("results"),
                                                          select=select)
            return
        download = render(job["download"], scopes)
        url = download.get("url")
        if url in (None, ""):
            window = scopes.get("window")
            LOG.info("stream %r: the job finished without a file to download, which usually means no data%s", name,
                     " from %s to %s" % (window["start"], window["end"]) if window else "",
                     extra=logs.fields(event="no_file", **(where or {"stream": name})))
            return
        if not isinstance(url, str):
            raise SourceError("the download url is %s, expected text" % type(url).__name__)
        runner.redact.add(urlsplit(url).query)
        file_format = download["format"]
        with tempfile.TemporaryFile() as handle:
            size = runner.clients.download(stream).download(url, handle, step("download"))
            LOG.info("stream %r: downloaded %d bytes", name, size,
                     extra=logs.fields(event="download", bytes=size, **(where or {"stream": name})))
            for page in downloads.read(handle, file_format, download.get("compression")):
                yield select(page) if file_format == "json" else page

    def poll(self, runner, name, poll, submit, *, context, scopes, where=None, client=None):
        every = parse_duration(poll["every"]).total_seconds()
        timeout = parse_duration(poll["timeout"]).total_seconds()
        call = poll if "http" in poll else components.poll_call(submit, poll)
        paths = [poll["done_when"]["path"]]
        if poll.get("fail_when") and poll["fail_when"]["path"] not in paths:
            paths.append(poll["fail_when"]["path"])
        started, checks = runner.clock(), 0
        while True:
            checks += 1
            status = as_scope(self._one(runner, call, client=client, context=context, scopes=scopes,
                                        where=dict(where or {"stream": name}, job_step="poll (check %d)" % checks)))
            text = runner.redact(", ".join("%s=%s" % (path, to_text(lookup(status, path))) for path in paths))
            if poll.get("fail_when") and matches(status, poll["fail_when"]):
                raise SourceError(runner.redact("the job failed: %s" % summarize(status, runner.redact)))
            elapsed = runner.clock() - started
            if matches(status, poll["done_when"]):
                LOG.info("stream %r: the job is done: %s (check %d, after %.0f s)", name, text, checks, elapsed,
                         extra=logs.fields(event="job_done", job_status=text, checks=checks,
                                           duration_ms=round(elapsed * 1000), **(where or {"stream": name})))
                return status
            if elapsed + every > timeout:
                raise SourceError(runner.redact("the job did not finish within %s; last status: %s" % (
                    poll["timeout"], summarize(status, runner.redact))))
            LOG.info("stream %r: the job is not done yet: %s (check %d, after %.0f s); checking again in %.0f s", name,
                     text, checks, elapsed, every, extra=logs.fields(event="poll_wait", job_status=text, checks=checks,
                                                                     duration_ms=round(every * 1000),
                                                                     **(where or {"stream": name})))
            runner.sleep(every)

READERS = MappingProxyType({"http": HttpReader(), "sdk": SdkReader(), "async_job": AsyncJobReader()})
