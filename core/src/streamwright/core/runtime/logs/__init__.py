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
Logging, progress and run metrics of `streamwright run`, on Python's standard model: named loggers and standard levels.
Logs go to stderr; stdout is for Singer messages. streamwright's loggers:

- `streamwright.source` (INFO): run, stream and window progress, summaries, warnings; DEBUG: internal details.
- `streamwright.network` (WARNING by default, so quiet): INFO, one line per API call - HTTP requests and connector SDK
  calls - and the throttling waits; WARNING, retries; DEBUG, the HTTP headers (credentials masked) and bodies.
- `streamwright.output`: the files and tables written.
SDK loggers keep their own names (ConnectorDefinition.loggers lists a connector's, for `streamwright connectors`).

- LogSetup: one command's logging - streamwright's stderr handler in the text or JSON format (--log-format) or a
  logging.config.dictConfig file's handlers (--log-config), the levels (--log-level for the `streamwright` loggers,
  --log NAME=LEVEL for any), and the redaction (the run's Redactor plus credentials masked by name) and truncation
  (--log-max-chars) of every log line of every logger, for every handler.
- fields(): a log line's structured fields (extra=), which JSON lines show (FIELDS).
- RunMetrics: a run's counts and timings in one place, fed by the runner, the HTTP clients and the connector context. It
  logs each stream's start and end, completed windows and partitions, the progress of long reads and the run's end,
  and gives the run summary that --summary FILE writes.

Modules: lines (loggers, levels, fields), redaction, network (network lines), formatting (JSON lines),
record_factory, handlers and setup (LogSetup), metrics (RunMetrics) and summary (write_summary).
"""

from streamwright.core.runtime.logs.formatting import JsonFormatter
from streamwright.core.runtime.logs.lines import (
    DEFAULT_MAX_CHARS,
    FIELDS,
    FORMATS,
    LEVELS,
    LOG,
    NETWORK,
    PROGRESS_PAGES,
    PROGRESS_SECONDS,
    TEXT_FORMAT,
    describe,
    fields,
    level_number,
)
from streamwright.core.runtime.logs.metrics import RunMetrics, StreamMetrics
from streamwright.core.runtime.logs.network import http_details, http_summary, request_details, sdk_summary
from streamwright.core.runtime.logs.redaction import mask_form, mask_headers, mask_text, mask_url, sensitive
from streamwright.core.runtime.logs.setup import LogSetup
from streamwright.core.runtime.logs.summary import write_summary

__all__ = [
    "DEFAULT_MAX_CHARS",
    "FIELDS",
    "FORMATS",
    "LEVELS",
    "LOG",
    "NETWORK",
    "PROGRESS_PAGES",
    "PROGRESS_SECONDS",
    "TEXT_FORMAT",
    "JsonFormatter",
    "LogSetup",
    "RunMetrics",
    "StreamMetrics",
    "describe",
    "fields",
    "http_details",
    "http_summary",
    "level_number",
    "mask_form",
    "mask_headers",
    "mask_text",
    "mask_url",
    "request_details",
    "sdk_summary",
    "sensitive",
    "write_summary",
]

