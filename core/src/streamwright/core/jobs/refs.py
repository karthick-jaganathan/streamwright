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
`{{ ... }}` references in job manifests.

* `{{ config.NAME }}` / `{{ secrets.NAME }}`: inputs, substituted once by `resolve` before a job is checked or run.
* `{{ step }}` / `{{ step.column }}`: values from an earlier step's data. In SQL fragments (table-format predicates, for
  example) they are rendered by `substitute` as escaped SQL *literals* from pinned values, never spliced as text.
  Values are pinned in the batch manifest in a type-tagged JSON form (`encode_value`) so a replay renders the same
  literals.
"""

import datetime
import decimal
import math
import re

from streamwright.core.sql.quote import quote_literal

__all__ = [
    "INPUT_REF",
    "MOUSTACHE",
    "STEP_REF",
    "RefError",
    "decode_value",
    "encode_value",
    "input_references",
    "references",
    "resolve",
    "sql_list",
    "sql_literal",
    "step_columns_used",
    "substitute",
    "walk_strings",
]

MOUSTACHE = re.compile(r"\{\{\s*([^{}]*?)\s*\}\}")
INPUT_REF = re.compile(r"^(config|secrets)\.([A-Za-z_][A-Za-z0-9_]*)$")
STEP_REF = re.compile(r"^([a-z][a-z0-9_]*)(?:\.([A-Za-z_][A-Za-z0-9_]*))?$")
_COLUMN_REF = re.compile(r"^([a-z][a-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$")


class RefError(ValueError):
    """A reference cannot be resolved or a value cannot be rendered as an SQL literal."""


def references(text):
    """The `{{ ... }}` expressions in a string (stripped); [] for anything else."""
    return [m.group(1) for m in MOUSTACHE.finditer(text)] if isinstance(text, str) else []


def walk_strings(node):
    """Every string in a nested structure, mapping keys included."""
    if isinstance(node, str):
        yield node
    elif isinstance(node, list):
        for item in node:
            yield from walk_strings(item)
    elif isinstance(node, dict):
        for key, value in node.items():
            if isinstance(key, str):
                yield key
            yield from walk_strings(value)


def input_references(node, source):
    """The names used as `{{ <source>.NAME }}` (source: config or secrets) anywhere in `node`."""
    found = set()
    for text in walk_strings(node):
        for expr in references(text):
            hit = INPUT_REF.match(expr)
            if hit and hit.group(1) == source:
                found.add(hit.group(2))
    return found


def resolve(job, config=None, secrets=None):
    """
    `(job, problems)`: `{{ config.* }}` and `{{ secrets.* }}` replaced by their values; any other reference is kept.
    A reference without a value is a problem; secret values never appear in a problem.
    """
    config, secrets, problems = config or {}, secrets or {}, []

    def substitute_inputs(text):
        def one(match):
            hit = INPUT_REF.match(match.group(1))
            if not hit:
                return match.group(0)
            source, name = hit.groups()
            values = config if source == "config" else secrets
            if values.get(name) is None:
                problems.append("%s.%s has no value" % (source, name))
                return match.group(0)
            return str(values[name])
        return MOUSTACHE.sub(one, text)

    def walk(node):
        if isinstance(node, str):
            return substitute_inputs(node)
        if isinstance(node, list):
            return [walk(item) for item in node]
        if isinstance(node, dict):
            return {substitute_inputs(k) if isinstance(k, str) else k: walk(v) for k, v in node.items()}
        return node

    return walk(job), sorted(set(problems))


def sql_literal(value, quote_text=quote_literal):
    """
    `value` as an SQL literal. Text is quoted by `quote_text` (standard SQL by default: quotes doubled, no
    backslash escapes; an engine whose SQL treats backslashes as escapes passes its own). Numbers must be finite;
    dates and timestamps become typed literals (aware timestamps in UTC).
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RefError("a non-finite number cannot be used in SQL")
        return repr(value)
    if isinstance(value, decimal.Decimal):
        if not value.is_finite():
            raise RefError("a non-finite number cannot be used in SQL")
        return str(value)
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.UTC).replace(tzinfo=None)
        return "TIMESTAMP '%s'" % value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, datetime.date):
        return "DATE '%s'" % value.isoformat()
    if isinstance(value, str):
        try:
            return quote_text(value)
        except ValueError as exc:
            raise RefError(str(exc)) from None
    raise RefError("cannot use a %s value in SQL" % type(value).__name__)


def sql_list(values, limit, quote_text=quote_literal):
    """A comma-separated list of literals; NULL for none, so `x IN (...)` matches nothing."""
    values = list(values)
    if len(values) > limit:
        raise RefError("%d values exceed runtime.limits.reference_values (%d); use a coarser column" % (
            len(values), limit))
    return ", ".join(sql_literal(v, quote_text) for v in values) if values else "NULL"


def encode_value(value):
    """A JSON-safe, type-tagged form of a value for the batch manifest."""
    if isinstance(value, datetime.datetime):
        return {"$ts": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"$date": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return {"$dec": str(value)}
    if isinstance(value, (bytes, bytearray)):
        raise RefError("binary values cannot be used in references")
    return value


def decode_value(value):
    """The inverse of encode_value."""
    if isinstance(value, dict) and len(value) == 1:
        (tag, text), = value.items()
        if tag == "$ts":
            return datetime.datetime.fromisoformat(text)
        if tag == "$date":
            return datetime.date.fromisoformat(text)
        if tag == "$dec":
            return decimal.Decimal(text)
    return value


def step_columns_used(text):
    """`[(step, column)]` for the `{{ step.column }}` references in a fragment."""
    return [m.groups() for m in (_COLUMN_REF.match(expr) for expr in references(text)) if m]


def substitute(text, values, constants, limit, quote_text=quote_literal):
    """
    `text` with `{{ step.column }}` rendered from `values[(step, column)]` (a list of literals) and
    `{{ name }}` from `constants` (batch.id, batch.ts, ...; one literal each). Anything else is an error: a predicate
    never keeps braces.
    """
    def one(match):
        expr = match.group(1).strip()
        if expr in constants:
            return sql_literal(constants[expr], quote_text)
        hit = _COLUMN_REF.match(expr)
        if hit and (hit.group(1), hit.group(2)) in values:
            return sql_list(values[(hit.group(1), hit.group(2))], limit, quote_text)
        raise RefError("cannot resolve {{ %s }} in %r" % (expr, text[:80]))
    return MOUSTACHE.sub(one, text)
