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

"""The source grammar (config.grammar): its groups, the flat keys it refuses, and issues reported where written."""

from textwrap import dedent

from streamwright.core.config import grammar
from streamwright.core.engine.runner import SourceRunner
from streamwright.core.sdk.testing import MemoryOutput
from streamwright.core.validation.engine import ERROR, validate_source, validate_text

SDK_SOURCE = dedent("""\
    kind: source
    name: demo
    auth: {provider: google_ads}
    streams:
      - name: campaigns
        policy:
          retry: {max_attempts: 2}
          timeout: 30s
        requests:
          - name: raw_campaigns
            operation:
              service: GoogleAdsService
              method: search
              arguments: {customer_id: "1", query: "SELECT campaign.id FROM campaign"}
              records: {path: results}
        transform: {mode: page, steps: [{name: campaigns, select: "SELECT record FROM raw_campaigns"}]}
        export: {campaigns: {step: campaigns}}
    """)


def issues(text, *, code=None):
    return [(issue.code, issue.path, issue.line) for issue in validate_text(text, "s.yaml")
            if code is None or issue.code == code]


def test_the_groups_translate_into_the_engine_shape():
    source = {
        "auth": {"provider": "google_ads"}, "http": {"base_url": "https://x"},
        "policy": {"retry": {"max_attempts": 2}, "rate_limit": {"requests": 1, "per": "1s"}},
        "streams": [{"name": "s", "policy": {"timeout": "5s", "on_error": "skip"}, "requests": [
            {"name": "a", "operation": {"service": "S", "method": "m", "arguments": {}, "records": {"path": "r"}}},
            {"name": "b", "operation": {"path": "/b", "records": {"path": "data"}, "paginator": {"type": "none"}}},
            {"name": "c", "async_job": {"submit": {"operation": {"service": "S", "method": "go"}},
                                        "poll": {"operation": {"method": "get", "arguments": {"id": 1}},
                                                 "every": "5s"},
                                        "results": {"operation": {"service": "S", "method": "rows",
                                                                  "records": {"path": "rows"}}}}}]}]}
    internal = grammar.to_internal(source)
    assert internal["http"] == {"base_url": "https://x", "retry": {"max_attempts": 2},
                                "rate_limit": {"requests": 1, "per": "1s"}}
    stream = internal["streams"][0]
    assert (stream["timeout"], stream["on_error"], "policy" in stream) == ("5s", "skip", False)
    call, http, job = stream["requests"]
    assert call == {"name": "a", "sdk": "google_ads", "service": "S", "method": "m", "arguments": {},
                    "records": {"path": "r"}}
    assert http == {"name": "b", "http": {"path": "/b"}, "records": {"path": "data"}, "paginator": {"type": "none"}}
    assert job["records"] == {"path": "rows"}
    assert job["async_job"]["submit"] == {"sdk": "google_ads", "service": "S", "method": "go"}
    assert job["async_job"]["poll"] == {"every": "5s", "method": "get", "arguments": {"id": 1}}
    assert grammar.to_internal(internal) == internal       # translating the engine shape changes nothing


def test_a_grouped_source_is_valid():
    assert issues(SDK_SOURCE, code="unknown-key") == []
    assert [issue for issue in validate_text(SDK_SOURCE, "s.yaml") if issue.severity == ERROR] == []


def test_internal_keys_are_unknown_where_they_are_written():
    text = dedent("""\
        kind: source
        name: demo
        auth: {provider: google_ads}
        http:
          retry: {max_attempts: 2}
        streams:
          - name: campaigns
            timeout: 30s
            requests:
              - name: raw_campaigns
                sdk: google_ads
                service: GoogleAdsService
                method: search
                records: {path: results}
            transform: {mode: page, steps: [{name: campaigns, select: "SELECT record FROM raw_campaigns"}]}
            export: {campaigns: {step: campaigns}}
        """)
    assert issues(text, code="unknown-key") == [
        ("unknown-key", "http.retry", 5),
        ("unknown-key", "streams[0].timeout", 8),
        ("unknown-key", "streams[0].requests[0].sdk", 11),
        ("unknown-key", "streams[0].requests[0].service", 12),
        ("unknown-key", "streams[0].requests[0].method", 13),
        ("unknown-key", "streams[0].requests[0].records", 14),
    ]


def test_a_type_and_a_mixed_operation_are_refused_where_written():
    typed = SDK_SOURCE.replace("records: {path: results}", "records: {path: results}\n          type: sdk")
    assert issues(typed) == [("unknown-key", "streams[0].requests[0].operation.type", 16)]
    # a call's and a request's keys: reported once, nothing else is checked under it
    mixed = SDK_SOURCE.replace("records: {path: results}", "records: {path: results}\n          path: /x")
    assert issues(mixed) == [("bad-value", "streams[0].requests[0].operation", 11)]


def test_issues_inside_a_group_name_its_public_path_and_line():
    text = SDK_SOURCE.replace("retry: {max_attempts: 2}", "retry: {max_attempts: 0}") \
                     .replace("records: {path: results}", "records: {path: results}\n          servce: x")
    found = issues(text)
    assert ("unknown-key", "streams[0].requests[0].operation.servce", 16) in found
    assert any(path == "streams[0].policy.retry.max_attempts" and line == 7 for _, path, line in found)


def test_a_call_needs_an_auth_provider():
    text = SDK_SOURCE.replace("auth: {provider: google_ads}\n", "")
    assert ("bad-value", "streams[0].requests[0].operation", 10) in issues(text)


def test_a_folder_source_reports_a_stream_file_issue_inside_its_group(tmp_path):
    (tmp_path / "streams").mkdir()
    (tmp_path / "source.yaml").write_text("kind: source\nname: demo\nauth: {provider: google_ads}\n")
    (tmp_path / "streams" / "campaigns.yaml").write_text(dedent("""\
        requests:
          - name: raw_campaigns
            operation:
              service: GoogleAdsService
              method: search
              argumnts: {}
        transform: {mode: page, steps: [{name: campaigns, select: "SELECT record FROM raw_campaigns"}]}
        export: {campaigns: {step: campaigns}}
        """))
    found = [(issue.code, issue.file, issue.path, issue.line) for issue in validate_source(str(tmp_path))]
    assert ("unknown-key", str(tmp_path / "streams" / "campaigns.yaml"), "requests[0].operation.argumnts", 6) in found


def test_the_runner_takes_the_grammar():
    source = {"kind": "source", "name": "demo", "auth": {"type": "bearer", "token": "t"},
              "http": {"base_url": "https://x"}, "policy": {"retry": {"max_attempts": 2}},
              "streams": [{"name": "s", "requests": [{"name": "r", "operation": {"path": "/a",
                                                                                 "records": {"path": "d"}}}],
                           "transform": {"mode": "page", "steps": [{"name": "s", "select": "SELECT 1 AS x"}]},
                           "export": {"s": {"step": "s"}}}]}
    runner = SourceRunner(source, {}, {}, output=MemoryOutput())
    assert runner.source["http"]["retry"] == {"max_attempts": 2}
    assert runner.source["streams"][0]["requests"][0]["records"] == {"path": "d"}
