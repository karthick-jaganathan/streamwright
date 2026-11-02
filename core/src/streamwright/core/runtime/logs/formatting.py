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

"""The JSON log-line format (--log-format json)."""


import collections
import datetime
import json
import logging

from streamwright.core.runtime.logs.lines import FIELDS, iso_time
from streamwright.core.runtime.logs.record_factory import top_setup


def _plain(value, redact):
    if isinstance(value, str):
        return redact(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, dict):
        return {str(key): _plain(item, redact) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item, redact) for item in value]
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return redact(str(value))


class JsonFormatter(logging.Formatter):
    """
    One JSON object per line: time (ISO 8601, UTC), level, logger, message, the structured fields (FIELDS) of
    streamwright's lines when they have them, and exception (a traceback) when there is one. `setup`: the LogSetup whose
    Redactor redacts the fields (default: the one in effect; a --log-config file can name this class: `"()":
    streamwright.core.runtime.logs.JsonFormatter`).
    """

    def __init__(self, setup=None):
        super().__init__()
        self.setup = setup

    def redact(self, text):
        setup = self.setup if self.setup is not None else top_setup()
        redact = getattr(setup, "redact", None)
        return redact(text) if redact is not None else text

    def format(self, record):
        entry = collections.OrderedDict()
        entry["time"] = iso_time(datetime.datetime.fromtimestamp(record.created, datetime.UTC))
        entry["level"] = record.levelname
        entry["logger"] = record.name
        entry["message"] = record.getMessage()
        # other libraries' attributes are theirs
        if record.name == "streamwright" or record.name.startswith("streamwright."):
            for name in FIELDS:
                value = getattr(record, name, None)
                if value is not None:
                    entry[name] = _plain(value, self.redact)
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            entry["exception"] = record.exc_text
        if record.stack_info:
            entry["stack"] = record.stack_info
        return json.dumps(entry, default=str)
