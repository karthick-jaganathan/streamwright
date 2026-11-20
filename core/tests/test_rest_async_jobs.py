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
"""Async jobs whose submit, poll and results are HTTP requests (REST connectors): submit -> poll -> download/results."""

import gzip
import json

import pytest

from streamwright.core.engine.runner import SourceError, SourceRunner
from streamwright.core.sdk.testing import MemoryOutput, page_stream

ID = "SELECT (record->>'id')::BIGINT AS id FROM records"


def report_source(api, job, records=None):
    """A source of one stream that reads the report `job`, polled every 5s until COMPLETED (FAILED fails it)."""
    request = {"async_job": dict(job, poll=dict(job["poll"], every="5s", timeout="1m",
                                                done_when={"path": "status", "equals": "COMPLETED"},
                                                fail_when={"path": "status", "equals": "FAILED"}))}
    return {"kind": "source", "name": "reports", "auth": {"type": "bearer", "token": "{{ secrets.token }}"},
            "http": {"base_url": api.url}, "streams": [page_stream("rows", request, ID, records=records)]}


def run(source):
    output, sleeps = MemoryOutput(), []
    SourceRunner(source, {}, {"token": "secret-token-1"}, output=output, sleep=sleeps.append).run()
    return [record["id"] for _, record in output.records], sleeps


def submit():
    return {"operation": {"path": "/reports", "method": "POST", "json": {"reportType": "daily"}}}


def poll():
    return {"operation": {"path": "/reports/{{ submit.reportId }}"}}


def test_a_report_is_submitted_polled_and_downloaded_without_the_connections_auth(api):
    statuses = [{"status": "PENDING"}, {"status": "COMPLETED", "url": api.url + "/files/r1.json.gz?sig=SIGNED"}]
    api.routes[("POST", "/reports")] = lambda request: (200, {"reportId": "r1"})
    api.routes[("GET", "/reports/r1")] = lambda request: (200, statuses.pop(0))
    api.routes[("GET", "/files/r1.json.gz")] = lambda request: (
        200, gzip.compress(json.dumps([{"id": 1}, {"id": 2}]).encode("utf-8")))
    source = report_source(api, {"submit": submit(), "poll": poll(),
                                 "download": {"url": "{{ poll.url }}", "format": "json", "compression": "gzip"}})
    assert run(source) == ([1, 2], [5.0])
    submitted, = api.calls("/reports")
    assert json.loads(submitted["body"]) == {"reportType": "daily"}
    assert submitted["headers"]["Authorization"] == "Bearer secret-token-1"
    downloaded, = api.calls("/files/r1.json.gz")
    assert "Authorization" not in downloaded["headers"]              # a presigned URL: no bearer token sent there


def test_results_are_read_like_a_request_with_records_and_a_paginator(api):
    api.routes[("POST", "/reports")] = lambda request: (200, {"reportId": "r2"})
    api.routes[("GET", "/reports/r2")] = lambda request: (200, {"status": "COMPLETED"})
    pages = {"": {"rows": [{"id": 1}], "next": api.url + "/reports/r2/rows?after=a"},
             "a": {"rows": [{"id": 2}], "next": None}}
    api.routes[("GET", "/reports/r2/rows")] = lambda request: (200, pages[request["params"].get("after", "")])
    results = {"operation": {"path": "/reports/{{ submit.reportId }}/rows",
                             "paginator": {"type": "cursor", "next_url_path": "next"}}}
    source = report_source(api, {"submit": submit(), "poll": poll(), "results": results}, records={"path": "rows"})
    assert run(source) == ([1, 2], [])


def test_a_failed_report_fails_the_partition_and_an_empty_one_gives_no_rows(api):
    api.routes[("POST", "/reports")] = lambda request: (200, {"reportId": "r3"})
    status = {"status": "FAILED", "failureReason": "bad column"}
    api.routes[("GET", "/reports/r3")] = lambda request: (200, status)
    download = {"url": "{{ poll.url }}", "format": "json", "compression": "gzip"}
    source = report_source(api, {"submit": submit(), "poll": poll(), "download": download})
    with pytest.raises(SourceError, match=r"the job failed: .*bad column"):
        run(source)
    status.update(status="COMPLETED", url="")                    # no file: no data in the window
    assert run(source) == ([], [])
    status["url"] = api.url + "/files/empty.json.gz"
    api.routes[("GET", "/files/empty.json.gz")] = lambda request: (200, gzip.compress(b"[]"))
    assert run(source) == ([], [])
