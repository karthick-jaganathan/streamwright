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

"""
streamwright's loggers and levels, and the wording of its log lines: structured fields, where-descriptions
and numbers.
"""


import datetime
import json
import logging

LOG = logging.getLogger("streamwright.source")
NETWORK = logging.getLogger("streamwright.network")
LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
FORMATS = ("text", "json")
DEFAULT_MAX_CHARS = 20000
# a request that keeps paging logs its progress this often
PROGRESS_SECONDS = 30.0
PROGRESS_PAGES = 100
TEXT_FORMAT = "[%(asctime)s] %(levelname)s %(name)s: %(message)s"
# the structured fields of streamwright's log lines, in the order JSON lines show them; each has one type
FIELDS = ("event", "stream", "partition", "window", "request", "export", "exports", "connector", "call", "method",
          "url", "page", "records", "pages", "requests", "retries", "failed_partitions", "streams", "outputs",
          "duration_ms", "status", "code", "attempt", "bytes", "path", "table", "outcome")


def level_number(level):
    """A standard level (a name, any case, NOTSET included, or a number) as a number; None stays None."""
    if level is None or isinstance(level, int):
        return level
    name = str(level).strip().upper()
    if name not in (*LEVELS, "NOTSET"):
        raise ValueError("unknown level %r (levels: %s)" % (level, ", ".join(LEVELS)))
    return getattr(logging, name)


def fields(**values):
    """extra= for a log line: its structured fields (FIELDS), without the empty ones."""
    return {name: value for name, value in values.items() if value is not None and value != {}}


def thousands(number):
    return f"{number:,}"


def milliseconds(seconds):
    return round(seconds * 1000)


def iso_time(moment):
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_now():
    return datetime.datetime.now(datetime.UTC)


def describe(where, request=True):
    """
    "stream 'x', request 'y', partition {...}, window 2026-09-01..2026-09-07": the parts `where` has (a mapping with
    stream, request, partition and window keys; `request`: whether to name the request).
    """
    parts = ["stream %r" % where["stream"]] if where.get("stream") else []
    if request and where.get("request"):
        parts.append("request %r" % where["request"])
    if where.get("partition"):
        parts.append("partition %s" % json.dumps(where["partition"], sort_keys=True, default=str))
    if where.get("window"):
        parts.append("window %s..%s" % (where["window"]["start"], where["window"]["end"]))
    return ", ".join(parts)


def where_prefix(where):
    text = describe(where) if where else ""
    return text + ": " if text else ""
