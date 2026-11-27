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
The lines of `streamwright.sql` (WARNING by default, so quiet): one per SQL statement streamwright runs. INFO says what
ran, where and how long (`job: CREATE OR REPLACE TEMP TABLE orders, 12 ms`); DEBUG adds the statement, its
parameters written in as SQL literals so it reads (and runs) as it ran, its whitespace collapsed (the `sql` field of a
JSON line keeps the statement as run, with its placeholders). A run that must not log row values (`streamwright serve`:
`sql_values(False)`) keeps the placeholders and logs only how many parameters there were. A statement is redacted and
cut like every log line, and credentials named in it masked (`password=***`); a statement that sets credentials
(CREATE SECRET; SET of a key, token, secret or password) logs only its head, never its text or parameters. The state
store's statements log on its child `streamwright.sql.state` (STATE_SQL), which takes its level unless named:
`--log streamwright.sql.state=WARNING` hides them.

- log_statement: the line of one statement.
- logged: a DuckDB connection whose statements log their lines (the connection itself while the logger is quiet).
- sql_values: whether the run's SQL lines show parameter values.
- inline: a statement with its parameters written in.
"""

import contextlib
import contextvars
import datetime
import decimal
import logging
import math
import re
import time
from collections.abc import Mapping

from streamwright.core.runtime.logs.lines import SQL, fields, milliseconds
from streamwright.core.runtime.logs.redaction import mask_text, sensitive

__all__ = ["STATE_SQL", "inline", "log_statement", "logged", "sql_values", "statement_head"]

STATE_SQL = logging.getLogger("streamwright.sql.state")
_VALUES = contextvars.ContextVar("streamwright_sql_values", default=True)
MANY_SHOWN = 100                     # an executemany's line writes in its first rows only
_SPACE = re.compile(r"\s+")
_OBJECT = r"((?:\"[^\"]+\"|[^\s(\"]+)(?:\.(?:\"[^\"]+\"|[^\s(\"]+))*)"   # a name, maybe quoted, maybe qualified
# SET, RESET, PRAGMA: the setting's name only (`SET s3_secret_access_key='...'` must not show its value)
_SETTING = re.compile(r"^(SET|RESET|PRAGMA)\s+(?:(?:GLOBAL|SESSION|LOCAL)\s+)?(\"[^\"]+\"|[A-Za-z_][\w.]*)",
                      re.IGNORECASE)
_SECRET = re.compile(r"^CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:PERSISTENT|TEMP|TEMPORARY)\s+)?SECRET\b", re.IGNORECASE)
# name=value inside a statement, as in a connection string ('host=h password=p'), masked when the name is a credential
_PAIR = re.compile(r"(?<![\w.$:])([A-Za-z_]\w*)=('(?:[^']|'')*'|[^\s',;)]+)")
_HEADS = (
    re.compile(r"^(CREATE(?:\s+OR\s+REPLACE)?(?:\s+TEMP(?:ORARY)?)?\s+(?:TABLE|VIEW|SCHEMA|INDEX|SECRET)"
               r"(?:\s+IF\s+NOT\s+EXISTS)?)\s+" + _OBJECT, re.IGNORECASE),
    re.compile(r"^(INSERT\s+(?:OR\s+\w+\s+)?INTO|DELETE\s+FROM|UPDATE|MERGE\s+INTO|DROP\s+(?:TABLE|VIEW)"
               r"(?:\s+IF\s+EXISTS)?|ALTER\s+TABLE|ATTACH|DETACH|LOAD|INSTALL|DESCRIBE)\s+" + _OBJECT,
               re.IGNORECASE),
    re.compile(r"^(COPY)\b.*?\b(TO\s+'[^']*')", re.IGNORECASE),                     # COPY (...) TO 'path'
    re.compile(r"^((?:WITH\b.*?\b)?SELECT)\b.*?\bFROM\s+" + _OBJECT, re.IGNORECASE),   # the first table read
)
# what a parameter marker can be next to: a quoted string or identifier, a comment, or a marker (?, $1, $name, :name)
_TOKENS = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|--[^\n]*|/\*.*?\*/|::|\?|[$:](?:\d+|[A-Za-z_]\w*)", re.DOTALL)


@contextlib.contextmanager
def sql_values(shown=True):
    """Within it, SQL lines show parameter values (`shown`) or only how many there are (streamwright serve)."""
    token = _VALUES.set(bool(shown))
    try:
        yield
    finally:
        _VALUES.reset(token)


def statement_head(sql):
    """What a statement does, in a few words: `CREATE OR REPLACE TEMP TABLE orders`, `UPDATE sw_events`, `SELECT`."""
    text = _SPACE.sub(" ", str(sql)).strip()
    setting = _SETTING.match(text)
    if setting:
        return "%s %s" % (setting.group(1).upper(), setting.group(2).replace('"', ""))
    for pattern in _HEADS:
        match = pattern.match(text)
        if match:
            verb = match.group(1).upper()
            return "%s %s" % ("SELECT FROM" if verb.endswith("SELECT") else verb, match.group(2).replace('"', ""))
    return (text.split(" ", 1)[0] or "?").upper()


def _quoted(text):
    return "'%s'" % str(text).replace("'", "''")


# how a parameter of each type is written as a SQL literal (anything else: as quoted text)
_LITERALS = (
    (bool, lambda value: "TRUE" if value else "FALSE"),
    ((int, decimal.Decimal), str),
    (float, lambda value: repr(value) if math.isfinite(value) else "'%s'::DOUBLE" % value),
    ((list, tuple), lambda value: "[%s]" % ", ".join(_literal(item) for item in value)),
    (Mapping, lambda value: "{%s}" % ", ".join("%s: %s" % (_quoted(key), _literal(item))
                                               for key, item in value.items())),
    ((bytes, bytearray), lambda value: "'%s'::BLOB" % "".join("\\x%02x" % byte for byte in value)),
    ((datetime.datetime, datetime.date, datetime.time), lambda value: _quoted(value.isoformat())),
)


def _literal(value):
    """A parameter as a SQL literal."""
    if value is None:
        return "NULL"
    for kinds, write in _LITERALS:
        if isinstance(value, kinds):
            return write(value)
    return _quoted(value)


def inline(sql, parameters, named="$"):
    """
    `sql` with its parameters written in as SQL literals: ? and $1 by position, `named`name ($name; :name on Spark) from
    a mapping. Markers inside quotes or comments stay; one without a value stays as it is.
    """
    if not parameters:
        return str(sql)
    positional = [] if isinstance(parameters, Mapping) else list(parameters)
    remaining = iter(positional)

    def value(match):
        token = match.group(0)
        if token == "?":
            item = next(remaining, _MISSING)
            return token if item is _MISSING else _literal(item)
        if token[0] not in "$:" or (token[0] == ":" and named != ":") or (token[0] == "$" and named == ":"):
            return token
        name = token[1:]
        if name.isdigit() and positional and 0 < int(name) <= len(positional):
            return _literal(positional[int(name) - 1])
        if isinstance(parameters, Mapping) and name in parameters:
            return _literal(parameters[name])
        return token

    return _TOKENS.sub(value, str(sql))


_MISSING = object()


def _count(parameters):
    try:
        return len(parameters) if parameters is not None else 0
    except TypeError:
        return 0


def log_statement(database, sql, seconds, parameters=None, *, error=None, logger=SQL, rows=None, named="$"):
    """
    The line of one statement `database` ran (a short label: job, transform, state, ...) with `parameters` (a sequence
    or a mapping), on `logger`. `rows`: an executemany's, `parameters` then the rows: the statement once per row (the
    first MANY_SHOWN), `;`-separated.
    """
    if not logger.isEnabledFor(logging.INFO):
        return
    took = _took(seconds) if error is None else "failed after %s (%s)" % (_took(seconds), type(error).__name__)
    count = sum(_count(row) for row in parameters or ()) if rows is not None else _count(parameters)
    counted = (", %d rows" % rows if rows is not None else "") + (
        ", %d parameter%s" % (count, "" if count == 1 else "s") if count else "")
    extra = fields(event="sql", database=database, duration_ms=milliseconds(seconds), parameters=count or None,
                   status=None if error is None else "failed")
    if logger.isEnabledFor(logging.DEBUG):
        if _sets_credentials(sql):
            logger.debug("%s: %s, %s%s: (not shown: it holds credentials)", database, statement_head(sql), took,
                         counted, extra=extra)
            return
        shown = sql if not _VALUES.get() else _many(sql, parameters, named) if rows is not None else \
            inline(sql, parameters, named)
        logger.debug("%s: %s, %s%s: %s", database, statement_head(sql), took, counted,
                     _masked(_SPACE.sub(" ", str(shown)).strip()), extra=dict(extra, sql=_masked(str(sql))))
    else:
        logger.info("%s: %s, %s%s", database, statement_head(sql), took, counted, extra=extra)


def _sets_credentials(sql):
    """Whether a statement sets credentials: CREATE SECRET, or SET / PRAGMA of a key, token, secret or password."""
    text = str(sql).lstrip()
    if _SECRET.match(text):
        return True
    setting = _SETTING.match(text)
    return bool(setting) and sensitive(setting.group(2).strip('"'))


def _masked(text):
    """A statement with the values of credential name=value pairs and URL credentials masked."""
    if "=" in text:
        text = _PAIR.sub(lambda match: match.group(1) + "=***" if sensitive(match.group(1)) else match.group(0), text)
    return mask_text(text)


def _many(sql, rows, named):
    """An executemany's statement once per row, the first MANY_SHOWN of them."""
    rows = list(rows or ())
    shown = "; ".join(inline(sql, row, named) for row in rows[:MANY_SHOWN])
    return shown + (" -- and %d more row(s)" % (len(rows) - MANY_SHOWN) if len(rows) > MANY_SHOWN else "")


def _took(seconds):
    """`0.4 ms`, `12 ms`: a tenth of a millisecond below 10 ms."""
    return "%.1f ms" % (seconds * 1000) if seconds < 0.01 else "%d ms" % milliseconds(seconds)


class _Logged:
    """A DuckDB connection or cursor whose execute, executemany and sql calls log their statements."""

    def __init__(self, target, database):
        self._target, self._database = target, database

    def _timed(self, method, sql, shown, rows, *args, **kwargs):
        start = time.perf_counter()
        try:
            result = getattr(self._target, method)(sql, *args, **kwargs)
        except Exception as error:
            log_statement(self._database, sql, time.perf_counter() - start, shown, error=error, rows=rows)
            raise
        log_statement(self._database, sql, time.perf_counter() - start, shown, rows=rows)
        return self if result is self._target else result      # execute returns its connection: keep the wrapper

    def execute(self, query, parameters=None, **kwargs):
        if parameters is None:
            return self._timed("execute", query, None, None, **kwargs)
        return self._timed("execute", query, parameters, None, parameters, **kwargs)

    def executemany(self, query, parameters=(), **kwargs):
        rows = list(parameters)
        return self._timed("executemany", query, rows, len(rows), rows, **kwargs)

    def sql(self, query, *args, **kwargs):
        return self._timed("sql", query, kwargs.get("params"), None, *args, **kwargs)

    def cursor(self):
        return _Logged(self._target.cursor(), self._database)

    def __enter__(self):
        self._target.__enter__()
        return self

    def __exit__(self, *exc):
        return self._target.__exit__(*exc)

    def __getattr__(self, name):
        return getattr(self._target, name)


def logged(connection, database):
    """`connection` with its statements logged on `streamwright.sql` when that logs (INFO, DEBUG); else itself."""
    return _Logged(connection, database) if SQL.isEnabledFor(logging.INFO) else connection
