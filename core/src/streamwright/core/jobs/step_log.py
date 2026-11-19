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
A step's `log:` (DuckDBJob and SparkJob, one meaning): once a batch has built the step, a line per row of its result,
up to `max_rows`, on the `streamwright.steps` logger at `level`, under the log context of the step:

    step <name>: <message, its {column} placeholders filled from the row>
    step <name>: ... and N more row(s)                     when the result has more rows
    step <name>: no rows to log                            (DEBUG) when it has none

`{column}` names a column of the step's result: NULL is `null`, other values their text (newlines escaped); a dotted
name that is not a column reaches into one (`{outcome.status}`: the `status` of the struct column `outcome`). The
`{{ }}` references a step may hold are rendered as usual (`{{ config.* }}` once per run, the engine's batch constants
such as `{{ batch.id }}` once per batch). Every line is redacted with the run's secrets, then cut to MAX_LINE_CHARS.
A run that may not log row values (`streamwright serve`) logs only `step <name>: N row(s) (...)`.
"""

import json
import logging
import re

from streamwright.core.jobs.fields import Field
from streamwright.core.jobs.refs import MOUSTACHE
from streamwright.core.runtime.logs.lines import fields

__all__ = ["DEFAULT_MAX_ROWS", "LEVELS", "LOG_KEY", "MAX_LINE_CHARS", "MAX_ROWS", "STEP_LOG", "STEP_LOG_FIELD",
           "StepLog", "StepLogError", "check_step_log", "log_step_rows", "parse_message"]

STEP_LOG = logging.getLogger("streamwright.steps")
LOG_KEY = "log"
LEVELS = ("debug", "info", "warning")
DEFAULT_LEVEL = "info"
DEFAULT_MAX_ROWS = 20
MAX_ROWS = 1000
MAX_LINE_CHARS = 1000
NOT_LOGGED = "row values are not logged by streamwright serve"
_PLACEHOLDER = re.compile(r"\{([^{}]*)\}")
_COLUMN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z0-9_]+)*$")     # a dotted name is one column ("a.b")
STEP_LOG_FIELD = Field("object", fields={
    "message": Field("string", required=True,
                     description="the line logged per row: text with {column} placeholders of the step's columns"),
    "level": Field("string", enum=LEVELS, default=DEFAULT_LEVEL),
    "max_rows": Field("integer", minimum=1, maximum=MAX_ROWS, default=DEFAULT_MAX_ROWS,
                      description="the most rows logged per batch; more are summed up in one line"),
}, description="log the step's rows once a batch has built it (streamwright.steps): `step <name>: <message>` per "
               "row, up to max_rows")


class StepLogError(ValueError):
    """A step's `log:` cannot be used (its message, or a placeholder the step's columns do not have)."""


def _literal(parts, text):
    if "{" in text or "}" in text:
        raise StepLogError("log.message: a lone %r (a placeholder is {column}, a reference {{ name }})"
                           % ("{" if "{" in text else "}"))
    if text:
        parts.append(("text", text))


def parse_message(message):
    """The parts of a message: ("text", text), ("column", name) and ("ref", a {{ }} expression), in order."""
    if not isinstance(message, str) or not message.strip():
        raise StepLogError("log.message: a non-empty text is required")
    parts = []
    for index, piece in enumerate(MOUSTACHE.split(message)):
        if index % 2:
            parts.append(("ref", piece))
            continue
        position = 0
        for match in _PLACEHOLDER.finditer(piece):
            _literal(parts, piece[position:match.start()])
            if not _COLUMN.match(match.group(1)):
                raise StepLogError("log.message: {%s} is not a column placeholder ({column}: letters, digits and _, "
                                   "parts joined by .)" % match.group(1))
            parts.append(("column", match.group(1)))
            position = match.end()
        _literal(parts, piece[position:])
    return tuple(parts)


def check_step_log(log):
    """Problems of a step's `log:` the structure check does not see: [(code, message, is_warning)]."""
    if not isinstance(log, dict) or "message" not in log:
        return []                                 # the structure check reports it
    try:
        parse_message(log["message"])
    except StepLogError as exc:
        return [("bad-value", str(exc), False)]
    return []


def _value(value):
    if value is None:
        return "null"
    return str(value).replace("\r", "\\r").replace("\n", "\\n")


def _known(name, columns):
    """Whether a placeholder names a column, or a field inside one (`a.b`: column `a`; `a.b.c`: `a` or `a.b`)."""
    parts = name.split(".")
    return any(".".join(parts[:end]) in columns for end in range(len(parts), 0, -1))


def _field(row, name):
    """A placeholder's value: the column `name`, or the field it reaches into a struct (or JSON object) column."""
    if name in row:
        return row[name]
    parts = name.split(".")
    for end in range(len(parts) - 1, 0, -1):
        column = ".".join(parts[:end])
        if column in row:
            value = row[column]
            for key in parts[end:]:
                if isinstance(value, str) and value.startswith("{"):
                    try:
                        value = json.loads(value)
                    except ValueError:
                        return None
                if not isinstance(value, dict):
                    return None
                value = value.get(key)
            return value
    return None


class StepLog:
    """A step's `log:`, parsed: what one batch logs about the step's result."""

    kind, logger, event = "step", STEP_LOG, "step_log"

    def __init__(self, step, spec):
        self.step, self.parts = step, parse_message(spec.get("message"))
        self.level = getattr(logging, str(spec.get("level") or DEFAULT_LEVEL).upper())
        self.max_rows = int(spec.get("max_rows") or DEFAULT_MAX_ROWS)
        self.columns = tuple(dict.fromkeys(text for kind, text in self.parts if kind == "column"))

    @classmethod
    def of(cls, step):
        """The StepLog of a step (its `log:`), or None when it has none."""
        spec = step.get(LOG_KEY)
        return cls(step["name"], spec) if isinstance(spec, dict) else None

    def check_columns(self, columns):
        """Raises StepLogError when a placeholder names a column the step's result does not have."""
        for name in self.columns:
            if not _known(name, columns):
                raise StepLogError("log.message: no column %r in %s %s; columns: %s" % (
                    name, self.kind, self.step, ", ".join(map(str, columns)) or "none"))

    def render(self, row, constants):
        """The message for one row ({column: value}), with the batch constants."""
        return "".join(text if kind == "text" else _value(_field(row, text)) if kind == "column"
                       else str(constants[text]) if text in constants else "{{ %s }}" % text
                       for kind, text in self.parts)

    def emit(self, text, redact):
        text = redact("%s %s: %s" % (self.kind, self.step, text))
        if len(text) > MAX_LINE_CHARS:
            text = text[:MAX_LINE_CHARS - 3] + "..."
        self.logger.log(self.level, "%s", text, extra=fields(event=self.event))


def log_step_rows(log, columns, head, count, *, row_values=True, constants=None, redact=None):
    """
    Logs a batch's result of a step with a `log:` (`log`: its StepLog; `columns`: the result's column names).
    `head(n)`: its first n rows, each a sequence in column order; `count()`: its number of rows, asked only when it
    has more than max_rows (or when `row_values` is False: a run that may not log values logs that number only).
    """
    log.check_columns(columns)
    constants, redact = constants or {}, redact or str
    if not row_values:
        total = count()
        if total:
            log.emit("%d row(s) (%s)" % (total, NOT_LOGGED), redact)
        else:
            log.logger.debug("%s %s: no rows to log", log.kind, log.step)
        return
    rows = list(head(log.max_rows + 1))
    if not rows:
        log.logger.debug("%s %s: no rows to log", log.kind, log.step)
        return
    for row in rows[:log.max_rows]:
        log.emit(log.render(dict(zip(columns, row, strict=False)), constants), redact)
    if len(rows) > log.max_rows:
        log.emit("... and %d more row(s)" % (count() - log.max_rows), redact)
