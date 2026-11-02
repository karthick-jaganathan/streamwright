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

"""The `streams` section: stream identities, removed or missing keys and exports."""

from types import MappingProxyType

from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.checker import DURATION, ERROR, describe
from streamwright.core.validation.sections.spec import SpecSection
from streamwright.core.validation.sections.stream_context import NAME, StreamContext, lower_name
from streamwright.core.validation.streams import StreamChecker

_RECORDS_AS_RETURNED = "use `transform` steps; for records as returned: `SELECT record FROM <request>`"
# stream keys of earlier formats, each with what replaced it (the last versions are the git tags fields-dsl and
# transform-cross-stream)
_REMOVED_STREAM_KEYS = MappingProxyType({
    "fields": "`fields` was replaced by SQL: `transform` steps (DuckDB SELECTs over the stream's requests) shape the "
              "records (see docs/design/source-format.md)",
    "on_record_error": "`on_record_error` was removed with `fields`: use TRY_CAST in a step's `select` for values "
                       "that may not convert",
    "request": "`request` was removed: use `requests` with one named item",
    "select": "a stream-level `select` was removed: " + _RECORDS_AS_RETURNED,
    "raw": "`raw` was removed: " + _RECORDS_AS_RETURNED,
    "primary_key": "a stream-level `primary_key` was removed: put `primary_key` on the export",
    "transform_mode": "`transform_mode` was removed: use `transform.mode`",
    "paginator": "a stream-level `paginator` was removed: put `paginator` on the request item",
    "records": "a stream-level `records` was removed: put `records` on the request item",
})
# removed keys that stand for a key the stream misses: that one is not reported as missing on top
_REPLACED_BY = MappingProxyType({"requests": ("request",), "transform": ("select", "raw", "fields")})
_MISSING_STREAM_KEYS = MappingProxyType({
    "requests": "a stream needs `requests`: streams read only their own requests",
    "transform": "a stream needs `transform`: {mode: page or run, steps: [{name, select}, ...]}",
    "export": "a stream needs `export`: at least one export (export name -> {step, primary_key?})",
})


class StreamsSection(SpecSection):
    """Stream identities and source-level exports."""

    def check_streams(self, data):
        if "streams" not in data:
            return
        streams = data["streams"]
        if not self._expect(isinstance(streams, list) and len(streams) > 0, "a non-empty list of streams",
                            streams, ("streams",), container=data, key="streams"):
            return
        names = self.stream_names
        self.streams = streams
        for i, stream in enumerate(streams):
            if isinstance(stream, dict) and isinstance(stream.get("name"), str):
                key = stream["name"].lower()
                if key in names:
                    previous = streams[names[key]].get("name")
                    suffix = "" if previous == stream["name"] else " (names ignore case)"
                    self.error("duplicate-stream", ("streams", i, "name"), "stream name %r is used twice%s" %
                               (stream["name"], suffix), stream, "name")
                names.setdefault(key, i)
        self.check_export_names()
        checked = []
        for i, stream in enumerate(streams):
            path = ("streams", i)
            if self._expect(isinstance(stream, dict), "a stream mapping", stream, path, container=streams, key=i):
                self.check_stream(stream, path)
                checked.append(i)
        self.check_partition_sources(checked)

    def check_export_names(self):
        """Export names are unique in the source; an export may have its own stream's name, not another stream's."""
        streams, names = self.streams, self.stream_names
        for i, stream in enumerate(streams):
            if not (isinstance(stream, dict) and isinstance(stream.get("export"), dict)):
                continue
            own = lower_name(stream.get("name"))
            if isinstance(own, str) and names.get(own) != i:
                continue  # a second stream with this name: reported as such
            for export in stream["export"]:
                if not isinstance(export, str):
                    continue
                key, epath = export.lower(), ("streams", i, "export", export)
                if key in names and key != own:
                    self.error("duplicate-export", epath, "export %r has the name of stream %r: an export can have "
                               "its own stream's name, not another stream's" % (export, streams[names[key]]["name"]),
                               stream["export"], export)
                elif key in self.export_owners:
                    previous, index = self.export_owners[key]
                    suffix = "" if previous == export else " (names ignore case)"
                    if index == i:
                        message = "export name %r is used twice%s" % (export, suffix)
                    else:
                        message = "export %r is also an export of stream %r%s: export names are unique in the " \
                                  "source" % (export, streams[index].get("name"), suffix)
                    self.error("duplicate-export", epath, message, stream["export"], export)
                else:
                    self.export_owners[key] = (export, i)

    def check_stream(self, stream, path):
        info = self._check_stream_header(stream, path)
        if "description" in stream:
            self._expect(isinstance(stream["description"], str), "text", stream["description"],
                         (*path, "description"), container=stream, key="description")
        if "on_partition_error" in stream and "on_error" in stream:
            self.error("conflicting-keys", (*path, "on_error"),
                       "a stream sets `on_error` or its released alias `on_partition_error`, not both", stream,
                       "on_error")
        for key in ("on_partition_error", "on_error"):
            if key in stream:
                self.check_choice(stream[key], spec.ERROR_POLICIES, (*path, key),
                                  container=stream, key=key, what="policy")
        if "timeout" in stream:
            value = stream["timeout"]
            if not ((isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0)
                    or (isinstance(value, str) and DURATION.match(value))):
                self.error("bad-value", (*path, "timeout"), "`timeout` is seconds (a positive number) or a duration "
                           "such as 30s, got %s" % describe(value), stream, "timeout")
        self.check_limits(stream, path)
        if "partitions" in stream:
            self.check_partitions(stream["partitions"], (*path, "partitions"), stream, info)
        if "incremental" in stream:
            StreamChecker(self, info).check_incremental(stream, path)
        for key in spec.REQUIRED_STREAM_KEYS:
            if key in _MISSING_STREAM_KEYS and key not in stream and \
                    not any(old in stream for old in _REPLACED_BY.get(key, ())):
                self.error("missing-key", path, _MISSING_STREAM_KEYS[key], stream)
        self.collect_tables(stream, path, info)
        if "requests" in stream:
            self.check_requests(stream["requests"], (*path, "requests"), stream, info)
        if "transform" in stream:
            self.check_transform(stream["transform"], (*path, "transform"), stream)
        if "export" in stream:
            self.check_export(stream["export"], (*path, "export"), stream, info)
        self.check_request_rules(stream, path, info)

    def _check_stream_header(self, stream, path):
        """The keys, kind and name of a stream; returns its StreamContext."""
        self.unknown_keys(stream, spec.STREAM_KEYS + tuple(_REMOVED_STREAM_KEYS), path, ERROR,
                          "streams do not support it")
        for key in stream:
            if key in _REMOVED_STREAM_KEYS:
                self.error("removed-key", (*path, key), _REMOVED_STREAM_KEYS[key], stream, key)
        if "kind" in stream and stream["kind"] != "stream":
            self.error("bad-value", (*path, "kind"), "a stream has `kind: stream`, got %r" % stream["kind"],
                       stream, "kind")
        self.require_keys(stream, ("name",), path, "a stream")
        info = StreamContext(stream.get("name") if isinstance(stream.get("name"), str) else "?")
        if path[1] in self.stream_files:  # a source folder's stream: the file name is the stream name
            if not NAME.match(stream["name"]):
                self.error("bad-value", path, "%r is not a valid stream name (letters, digits and _; not starting "
                           "with a digit): rename the file" % stream["name"], stream)
        elif "name" in stream:
            self.check_name(stream["name"], (*path, "name"), stream, "name")
        return info

    def check_request_rules(self, stream, path, info):
        """`mode: page` reads one request, without request partitions; `incremental` needs a request with a window."""
        StreamChecker(self, info).check_request_rules(stream, path)
