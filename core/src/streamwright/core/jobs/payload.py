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
A job's input payload (`spec.input`, plan §3): rows a caller hands to one run - `--input FILE` or `--input -` (stdin)
- as the batch's `input` table, the table a consume step fills from a broker (a job has one or the other).

    spec:
      input:
        columns:                                   # declared and typed like spec.config inputs (not required unless
          gclid: {type: string, required: true}    # `required: true`), with the same constraints
          conversion_value: {type: number, minimum: 0}
          currency_code: {type: string, enum: [USD, EUR]}
        max_rows: 10000                            # default 10000
        max_bytes: 104857600                       # default 100 MB, checked before parsing
        unknown_columns: reject                    # reject (default) | drop
        retain: until_complete                     # until_complete (default) | none

The payload is a JSON array of objects or NDJSON (one object per line). Rows are typed and checked here, before a
batch exists, then loaded into DuckDB with the declared types (no inference, no file). The rows are pinned in the batch
so a retry reads the same rows, and removed from the state store when the batch completes (only their SHA-256 stays);
`retain: none` never stores them (a failed batch then cannot be retried). Row values are never logged.
"""

import hashlib
import json
import re
import sys
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from streamwright.core.config.inputs import InputError, resolve_inputs
from streamwright.core.jobs.fields import Field
from streamwright.core.sql.quote import quote_ident
from streamwright.core.validation import vocabulary

__all__ = ["INPUT_FIELD", "Payload", "PayloadError", "check_input", "declared_input", "input_rows", "load_input",
           "pin", "read_payload", "run_payload", "settle", "strict_json"]

DEFAULTS = MappingProxyType({"max_rows": 10_000, "max_bytes": 100 * 1024 * 1024, "unknown_columns": "reject",
                             "retain": "until_complete"})
MAX_DEPTH = 32
MAX_REPORTED = 10                                   # row problems listed in one error
_SQL_TYPES = MappingProxyType({"string": "VARCHAR", "integer": "BIGINT", "number": "DOUBLE", "boolean": "BOOLEAN",
                               "date": "DATE"})
_COLUMN = Field("object", fields={
    "type": Field("string", enum=vocabulary.INPUT_TYPES), "items": Field("string", enum=vocabulary.LIST_ITEM_TYPES),
    "required": Field("boolean"), "description": Field("string"),
    "enum": Field("array"), "pattern": Field("string"), "minimum": Field("number"), "maximum": Field("number"),
    "max_length": Field("integer", minimum=1)})
INPUT_FIELD = Field("object", fields={
    "columns": Field("object", required=True, values=_COLUMN, description="the payload's columns, typed"),
    "max_rows": Field("integer", minimum=1, description="the most rows one payload may hold (10000)"),
    "max_bytes": Field("integer", minimum=1, description="the largest payload, in bytes (100 MB)"),
    "unknown_columns": Field("string", enum=("reject", "drop"), description="a column not declared fails the "
                                                                            "payload (reject) or is left out"),
    "retain": Field("string", enum=("until_complete", "none"), description="the rows stay in the state store until "
                                                                         "the batch completes, or are never stored"),
}, description="the rows a run is given (--input): the batch's `input` table")


class PayloadError(InputError):
    """A payload that cannot be read or does not fit its spec.input."""


@dataclass(frozen=True)
class Payload:
    """Typed, JSON-ready rows and their SHA-256 (of the canonical JSON)."""
    rows: tuple[dict[str, Any], ...]
    sha256: str
    retain: str = "until_complete"


def declared_input(job):
    """spec.input with its defaults, or None when the job takes no payload."""
    spec = job.get("spec") if isinstance(job.get("spec"), dict) else {}
    declared = spec.get("input")
    return dict(DEFAULTS, **declared) if isinstance(declared, dict) else None


def check_input(job):
    """Problems of spec.input's declaration, as (path, message)."""
    declared = declared_input(job)
    if declared is None:
        return []
    found = []
    columns = declared.get("columns")
    if not isinstance(columns, dict) or not columns:
        return [*found, (("spec", "input", "columns"), "declare at least one column")]
    for name, declared_column in columns.items():
        path = ("spec", "input", "columns", str(name))
        if not isinstance(name, str) or not re.match(vocabulary.NAME_PATTERN, name):
            found.append((path, "%r is not a valid column name" % (name,)))
        definition = declared_column if isinstance(declared_column, dict) else {}
        if definition.get("type", "string") == "list" and "items" not in definition:
            found.append(((*path, "items"), "a list column needs `items`"))
        if "pattern" in definition:
            try:
                re.compile(str(definition["pattern"]))
            except re.error as exc:
                found.append(((*path, "pattern"), "not a regular expression: %s" % exc))
    return found


def _no_duplicates(pairs):
    found = {}
    for key, value in pairs:
        if key in found:
            raise PayloadError("an object repeats the key %r" % key)
        found[key] = value
    return found


def _depth(value, what, level=0):
    if level > MAX_DEPTH:
        raise PayloadError("%s nests deeper than %d levels" % (what, MAX_DEPTH))
    children = value.values() if isinstance(value, dict) else value if isinstance(value, list) else ()
    for child in children:
        _depth(child, what, level + 1)


def strict_json(text, what="the payload"):
    """JSON `text` parsed with a key repeated in an object refused and at most MAX_DEPTH levels (PayloadError)."""
    try:
        value = json.loads(text, object_pairs_hook=_no_duplicates)
    except json.JSONDecodeError as exc:
        raise PayloadError("%s is not JSON: %s (line %d)" % (what, exc.msg, exc.lineno)) from None
    _depth(value, what)
    return value


def _parse(text):
    if text.lstrip().startswith("["):
        rows = strict_json(text)
    else:
        rows = [strict_json(line) for line in text.splitlines() if line.strip()]
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise PayloadError("the payload is a JSON array of objects, or NDJSON (one object per line)")
    return rows


def input_rows(rows, declared):
    """The rows typed and checked against spec.input (`declared`, with defaults): a Payload, or PayloadError."""
    if len(rows) > int(declared["max_rows"]):
        raise PayloadError("the payload has %d rows, more than spec.input.max_rows (%d)" % (
            len(rows), declared["max_rows"]))
    columns = {name: dict(definition or {}, required=bool((definition or {}).get("required", False)))
               for name, definition in declared["columns"].items()}
    typed, problems = [], []
    for number, row in enumerate(rows, start=1):
        unknown = sorted(set(row) - set(columns))
        if unknown and declared["unknown_columns"] == "reject":
            problems.append("row %d: unknown column(s) %s" % (number, ", ".join(unknown)))
            continue
        try:
            typed.append(resolve_inputs(columns, {k: v for k, v in row.items() if k in columns}, "column"))
        except InputError as exc:
            problems.append("row %d: %s" % (number, exc))
    if problems:
        more = "; and %d more" % (len(problems) - MAX_REPORTED) if len(problems) > MAX_REPORTED else ""
        raise PayloadError("the payload does not fit spec.input: %s%s" % ("; ".join(problems[:MAX_REPORTED]), more))
    ready = json.loads(json.dumps(typed, sort_keys=True, default=str))
    digest = hashlib.sha256(json.dumps(ready, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return Payload(tuple(ready), digest, declared["retain"])


def read_payload(source, job, stdin=None):
    """The payload at `source` (a path, or "-" for stdin), checked against the job's spec.input."""
    declared = declared_input(job)
    if declared is None:
        raise PayloadError("--input: the job declares no spec.input (it takes no payload)")
    limit = int(declared["max_bytes"])
    try:
        if source == "-":
            stream = stdin or sys.stdin.buffer
            data = stream.read(limit + 1)
        else:
            with open(source, "rb") as stream:
                data = stream.read(limit + 1)
    except OSError as exc:
        raise PayloadError("--input %s: %s" % (source, exc.strerror or exc)) from None
    if len(data) > limit:
        raise PayloadError("the payload is larger than spec.input.max_bytes (%d bytes)" % limit)
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        raise PayloadError("the payload is not UTF-8 text") from None
    return input_rows(_parse(text), declared)


def run_payload(job, source, *, needed=True, rows=None):
    """
    A run's payload: read from `source` (a path, or "-"), or `rows` given in memory (streamwright serve); a job with
    spec.input needs one to start new work (`needed`), a job without one takes none.
    """
    if rows is not None:
        declared = declared_input(job)
        if declared is None:
            raise PayloadError("input rows: the job declares no spec.input (it takes no payload)")
        return input_rows(list(rows), declared)
    if source is not None:
        return read_payload(source, job)
    if declared_input(job) is not None and needed:
        raise PayloadError("the job takes an input payload (spec.input): give it with --input FILE, or --input - for "
                           "stdin")
    return None


def pin(manifest, payload):
    """A new batch's manifest with its payload: the fingerprint, and the rows unless retain: none."""
    manifest["input"] = {"sha256": payload.sha256, "rows": len(payload.rows)}
    if payload.retain != "none":
        manifest["input"]["records"] = list(payload.rows)
    return manifest


def settle(manifest):
    """A completed batch's manifest: the payload rows removed (only the fingerprint stays)."""
    pinned = manifest.get("input")
    if isinstance(pinned, dict) and "records" in pinned:
        manifest = dict(manifest, input={key: value for key, value in pinned.items() if key != "records"})
    return manifest


def _column_type(definition):
    kind = (definition or {}).get("type", "string")
    if kind == "list":
        return "%s[]" % _SQL_TYPES.get((definition or {}).get("items", "string"), "VARCHAR")
    return _SQL_TYPES.get(kind, "VARCHAR")


def load_input(con, table, declared, manifest, payload):
    """
    Creates the typed `table` from the batch's payload: the rows its manifest pinned, else this run's payload when it
    is the one the batch was made with (retain: none).
    """
    pinned = manifest.get("input") or {}
    if "records" in pinned:
        rows = pinned["records"]
    elif payload is not None and payload.sha256 == pinned.get("sha256"):
        rows = payload.rows
    else:
        raise PayloadError("the batch's payload was not kept (spec.input.retain: none, or it completed): it cannot "
                           "be resumed; abandon it (`streamwright state abandon --url URL JOB_ID`)")
    names = list(declared["columns"])
    con.execute("CREATE OR REPLACE TEMP TABLE %s (%s)" % (quote_ident(table), ", ".join(
        "%s %s" % (quote_ident(name), _column_type(declared["columns"][name])) for name in names)))
    if rows:
        con.executemany("INSERT INTO %s VALUES (%s)" % (quote_ident(table), ", ".join("?" * len(names))),
                        [[row.get(name) for name in names] for row in rows])
