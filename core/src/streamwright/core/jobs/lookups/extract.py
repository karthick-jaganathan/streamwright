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

"""
The values a lookup returns, parsed (`format`) and picked (`field`).

format: auto (default) parses JSON when the text is JSON (a JSON string holding JSON is unwrapped) and keeps it as
text otherwise; text never parses (one opaque token); json must be JSON; json_string must be a JSON document
serialized in a JSON string. field: a dot path (`credentials.clientId`), list indexes (`accounts[0].token` or
`accounts.0.token`), a column of a row; empty or `.` is the whole value. A path through a text holding JSON parses it
and goes on.
"""

import json
import re

__all__ = ["FORMATS", "LookupValueError", "extract", "parse_output", "text_value"]

FORMATS = ("auto", "text", "json", "json_string")
_INDEX = re.compile(r"\[(\d+)\]")


class LookupValueError(ValueError):
    """A lookup's output is not in the declared format, or lacks a field (the message never holds a value)."""


def parse_output(text, fmt="auto"):
    """The value of a command's or function's text output in `fmt`."""
    fmt = fmt or "auto"
    if fmt not in FORMATS:
        raise LookupValueError("unknown format %r (%s)" % (fmt, ", ".join(FORMATS)))
    if not isinstance(text, str):
        return text
    stripped = text.strip()
    if fmt == "text":
        return stripped
    try:
        value = json.loads(stripped)
    except ValueError:
        if fmt == "auto":
            return stripped
        raise LookupValueError("the output is not JSON (format %s)" % fmt) from None
    if isinstance(value, str) and (fmt == "json_string" or value.strip()[:1] in ("{", "[")):
        try:
            return json.loads(value)
        except ValueError:
            if fmt == "json_string":
                raise LookupValueError("the output is a JSON string that holds no JSON (format json_string)") \
                    from None
    elif fmt == "json_string":
        raise LookupValueError("the output is JSON but not a JSON string (format json_string)")
    return value


def _parts(field):
    return [part for part in _INDEX.sub(r".\1", field).split(".") if part]


def extract(value, field=None):
    """The part of `value` at the path `field` (None or `.`: all of it)."""
    if field in (None, "", "."):
        return value
    current = value
    for depth, part in enumerate(_parts(field)):
        if isinstance(current, str):
            parsed = parse_output(current, "auto")
            if isinstance(parsed, (dict, list)):
                current = parsed
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            where = ".".join(_parts(field)[:depth]) or "the output"
            raise LookupValueError("field %r: %s has no %r" % (field, where, part))
    return current


def text_value(value, what):
    """A picked value as text: numbers and booleans become text; objects and lists need a deeper field."""
    if value is None:
        raise LookupValueError("%s is empty (null)" % what)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    raise LookupValueError("%s is a%s %s: name a field inside it" % (
        what, "n" if isinstance(value, list) else "", "list" if isinstance(value, list) else "object"))
