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

"""A stream's `requests` items."""


from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.checker import ERROR
from streamwright.core.validation.sections.partitions import PartitionSection
from streamwright.core.validation.sections.stream_context import StreamContext


class RequestSection(PartitionSection):
    """Request declarations."""

    def check_requests(self, requests, path, stream, info):
        if not self._expect(isinstance(requests, list) and len(requests) > 0, "a non-empty list of requests",
                            requests, path, container=stream, key="requests"):
            return
        for j, request in enumerate(requests):
            rpath = (*path, j)
            if not self._expect(isinstance(request, dict), "a request mapping", request, rpath, container=requests,
                                key=j):
                continue
            self.require_keys(request, ("name",), rpath, "a request")
            if "name" in request:
                self.check_name(request["name"], (*rpath, "name"), request, "name")
            request_info = StreamContext(info.name)
            request_info.partitions = list(info.partitions)
            request_info.incremental = info.incremental
            if "partitions" in request:
                self.check_request_partitions(request["partitions"], (*rpath, "partitions"), request, request_info,
                                              index=j, stream_info=info, stream=stream)
            kinds = [kind for kind in spec.REQUEST_KINDS if kind in request]
            if len(kinds) != 1:
                self.unknown_keys(request, spec.REQUEST_ITEM_KEYS, rpath, ERROR, "requests do not support it")
                self.error("bad-value", rpath, "a request needs exactly one of: http, sdk, async_job (found: %s)" %
                           (", ".join(kinds) or "none"), request)
            else:
                request_info.request_kind = kinds[0]
                allowed = {
                    "http": (*spec.REQUEST_ITEM_BASE_KEYS, "http"),
                    "sdk": spec.REQUEST_ITEM_BASE_KEYS + spec.SDK_REQUEST_KEYS,
                    "async_job": (*spec.REQUEST_ITEM_BASE_KEYS, "async_job"),
                }[request_info.request_kind]
                self.unknown_keys(request, allowed, rpath, ERROR, "requests do not support it")
                if request_info.request_kind == "async_job":
                    self.check_async_job(request["async_job"], (*rpath, "async_job"), request, request_info)
                elif request_info.request_kind == "http":
                    self.check_http_request(request["http"], (*rpath, "http"), request, self.scopes(request_info),
                                            request_info)
                else:
                    self.check_sdk_request(request, rpath, self.scopes(request_info), request_info,
                                           extra_allowed=spec.REQUEST_ITEM_BASE_KEYS)
            if "paginator" in request:
                self.check_paginator(request["paginator"], (*rpath, "paginator"), request, request_info)
            if "records" in request:
                records = self.check_mapping(request["records"], spec.RECORDS_KEYS, (*rpath, "records"),
                                             container=request, key="records", what="`records`")
                if records is not None:
                    for key in spec.RECORDS_KEYS:
                        if key in records:
                            self._expect(isinstance(records[key], str), "a dotted path", records[key],
                                         (*rpath, "records", key), container=records, key=key)
                    self.check_templates(records, (*rpath, "records"), container=request, key="records", scopes={})
