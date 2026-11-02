#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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

"""Job requests (HTTP, SDK and async jobs) and paginators."""


from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.checker import ERROR
from streamwright.core.validation.requests import HttpChecker, RequestChecker
from streamwright.core.validation.sections.request_partitions import RequestPartitionSection


class AsyncJobSection(RequestPartitionSection):
    """Async job and paginator validation."""

    def check_job_request(self, request, path, *, container, key, info, scopes):
        """The `submit` or `results` request of an async job: an sdk request (an `http:` one is reported already)."""
        if not self._expect(isinstance(request, dict), "a mapping", request, path, container=container, key=key):
            return
        if "sdk" in request:
            self.check_sdk_request(request, path, scopes, info)
        else:
            self.error("missing-key", path, "`%s` needs `sdk`: async jobs run through a connector" % key, container,
                       key)

    def check_http_request(self, http, path, container, scopes, info):
        HttpChecker(self, info).check_request(http, path, container, scopes, info)

    def check_sdk_request(self, request, path, scopes, info, extra_allowed=()):
        RequestChecker(self, info).check_sdk(request, path, scopes, info, extra_allowed)

    def check_async_job(self, job, path, container, info):
        job = self.check_mapping(job, spec.ASYNC_JOB_KEYS, path, container=container, key="async_job",
                                 what="`async_job`", required=("submit", "poll"))
        if job is None:
            return
        outputs = [name for name in ("download", "results") if name in job]
        if len(outputs) != 1:
            self.error("bad-value", path, "`async_job` needs exactly one of `download` or `results`", container,
                       "async_job")
        base = self.scopes(info)
        for part in ("submit", "results"):
            if isinstance(job.get(part), dict) and "http" in job[part]:
                self.error("bad-value", (*path, part), "`%s` must be an sdk request: async jobs run through a "
                            "connector (its `poll` calls the same service)" % part, job, part)
        if "submit" in job and not (isinstance(job["submit"], dict) and "http" in job["submit"]):
            self.check_job_request(job["submit"], (*path, "submit"), container=job, key="submit", info=info,
                                   scopes=base)
        if "poll" in job:
            self._check_poll(job, (*path, "poll"), info)
        if "download" in job:
            self._check_download(job, (*path, "download"), info)
        if "results" in job and not (isinstance(job["results"], dict) and "http" in job["results"]):
            self.check_job_request(job["results"], (*path, "results"), container=job, key="results", info=info,
                                   scopes=self.scopes(info, "submit", "poll"))

    def _check_poll(self, job, ppath, info):
        poll = self.check_mapping(job["poll"], spec.POLL_KEYS, ppath, container=job, key="poll", what="`poll`",
                                  required=("method", "every", "timeout", "done_when"))
        if poll is None:
            return
        self.check_strings(poll, ("method",), ppath, "a method name")
        for duration in ("every", "timeout"):
            if duration in poll:
                self.check_duration(poll[duration], (*ppath, duration), poll, duration)
        for name in ("done_when", "fail_when"):
            if name in poll:
                condition = self.check_mapping(poll[name], spec.POLL_CONDITION_KEYS, (*ppath, name), container=poll,
                                               key=name, what="`%s`" % name, required=("path",))
                if condition is not None:
                    self.check_strings(condition, ("path",), (*ppath, name), "a dotted path")
                if condition is not None and len([k for k in ("equals", "in") if k in condition]) != 1:
                    self.error("bad-value", (*ppath, name), "needs exactly one of `equals` or `in`", poll, name)
        self.check_templates(poll, ppath, container=job, key="poll", scopes=self.scopes(info, "submit"), stream=info)

    def _check_download(self, job, dpath, info):
        download = self.check_mapping(job["download"], spec.DOWNLOAD_KEYS, dpath, container=job, key="download",
                                      what="`download`", required=("url", "format"))
        if download is None:
            return
        self.check_strings(download, ("url",), dpath, "a URL or a reference")
        if "format" in download:
            self.check_choice(download["format"], spec.DOWNLOAD_FORMATS, (*dpath, "format"), container=download,
                              key="format", what="format")
        if "compression" in download:
            self.check_choice(download["compression"], spec.COMPRESSIONS, (*dpath, "compression"),
                              container=download, key="compression", what="compression")
        self.check_templates(download, dpath, container=job, key="download",
                             scopes=self.scopes(info, "submit", "poll"), stream=info)

    def check_paginator(self, paginator, path, stream, info):
        if not self._expect(isinstance(paginator, dict), "a mapping", paginator, path, container=stream,
                            key="paginator"):
            return
        if "type" not in paginator:
            self.error("missing-key", path, "`paginator` needs `type` (one of: %s)" % ", ".join(spec.PAGINATOR_TYPES),
                       stream, "paginator")
            return
        kind = paginator["type"]
        if not self.check_choice(kind, spec.PAGINATOR_TYPES, (*path, "type"), container=paginator, key="type",
                                 what="paginator"):
            return
        keys = spec.PAGINATOR_TYPES[kind]
        self.require_keys(paginator, keys["required"], path, "`paginator: %s`" % kind)
        self.unknown_keys(paginator, ("type",) + keys["required"] + keys["optional"], path, ERROR,
                          "`paginator: %s` does not support it" % kind)
        self.check_strings(paginator, ("offset_param", "limit_param", "page_param", "size_param", "token_path",
                                       "param", "next_url_path", "total_path", "total_pages_path", "has_more_path"),
                           path, "a name or dotted path")
        for key in ("offset_param", "limit_param", "page_param", "size_param", "param"):
            if key in paginator:
                val = paginator[key]
                if not (isinstance(val, str) and val.strip() != ""):
                    self.error("bad-value", (*path, key), "%r must be a non-empty string" % key, stream, "paginator")
        if "in" in paginator:
            self.check_choice(paginator["in"], spec.PAGINATOR_IN_LOCATIONS, (*path, "in"), container=paginator,
                              key="in", what="location")
        if paginator.get("in") == "body" and isinstance(stream, dict) and "http" in stream:
            http_req = stream["http"]
            if isinstance(http_req, dict) and not isinstance(http_req.get("json"), dict):
                self.error("bad-value", (*path, "in"), "`in: body` requires `http.json` to be a mapping",
                           stream, "paginator")
        if kind == "cursor":
            token = "token_path" in paginator or "param" in paginator
            if token == ("next_url_path" in paginator) or (token and not ("token_path" in paginator and
                                                                         "param" in paginator)):
                self.error("bad-value", path, "`paginator: cursor` needs `token_path` and `param`, or "
                           "`next_url_path`", stream, "paginator")
        for name in ("page_size", "start"):
            if name in paginator:
                number = paginator[name]
                minimum = 1 if name == "page_size" else 0
                self._expect(isinstance(number, int) and not isinstance(number, bool) and number >= minimum,
                             "a whole number >= %d" % minimum, number, (*path, name), container=paginator, key=name)
        if kind != "none" and info is not None and getattr(info, "request_kind", None) in ("sdk", "async_job"):
            self.error("bad-value", (*path, "type"), "%s requests are paginated by the connector; remove `paginator` "
                       "or use `type: none`" % info.request_kind, paginator, "type")
        self.check_templates(paginator, path, container=stream, key="paginator", scopes=self.scopes(info, "response"),
                             stream=info)
