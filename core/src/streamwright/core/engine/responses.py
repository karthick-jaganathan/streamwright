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

"""Reading values out of API responses: paths, async job conditions, and the first response of a call."""

import json

from streamwright.core.net.http import error_excerpt
from streamwright.core.runtime.templates import get_path, to_text

__all__ = ["as_scope", "first_response", "lookup", "matches", "same_value", "summarize"]


def lookup(data, path):
    """The value at a dotted path, or None when it is missing."""
    try:
        return get_path(data, path)
    except KeyError:
        return None


def as_scope(value):
    """A submit / poll response as a scope: mappings as they are, anything else as `result`."""
    return value if isinstance(value, dict) else {"result": value}


def same_value(value, expected):
    return value == expected or (value is not None and expected is not None and to_text(value) == to_text(expected))


def matches(data, condition):
    """Whether a response meets a `{path, equals}` / `{path, in}` condition."""
    value = lookup(data, condition["path"])
    expected = condition["in"] if "in" in condition else [condition.get("equals")]
    return any(same_value(value, item) for item in expected)


def summarize(data, redact):
    """A job status in an error message: its JSON, redacted before it is cut (error_excerpt)."""
    return error_excerpt(json.dumps(data, default=str, sort_keys=True), redact)


def first_response(responses):
    """The first of a call's responses (None if it has none); the rest are not read."""
    iterator = iter(responses)
    try:
        return next(iterator, None)
    finally:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()
