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

"""Column rules (`columns:`), the controlled vocabulary and `row_index`."""

import re

from streamwright.core.jobs.ingest.model import IngestError

TYPE_PATTERN = re.compile(r"^(string|int|bigint|long|double|float|boolean|date|timestamp"
                          r"|decimal\([0-9]{1,2},[0-9]{1,2}\))$")
UNMAPPED_COLUMN_POLICIES = ("ignore", "warn", "reject")

_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_TRUE = ("true", "t", "yes", "y", "1")


def is_true(value):
    return value is True or str(value).strip().lower() in _TRUE


def normalize_columns(columns):
    """
    Normalizes columns input into (flat_rules: list[dict], selector: str | None).
    Supports:
      - flat list of dicts: returns (list, None)
      - dict with 'by' or 'key' selector: returns (flat_rules, selector)
      - dict of lists or dict of dicts without 'by': returns (flat_rules, None)
    """
    if columns is None:
        return None, None
    if isinstance(columns, (list, tuple)):
        # a plain list applies to every file (the empty path prefix)
        return [rule if any(k in rule for k in ("prefix", "path_regex", "schema_key")) else dict(rule, prefix="")
                for rule in columns if isinstance(rule, dict)], None
    if not isinstance(columns, dict):
        return columns, None

    selector = columns.get("by") or columns.get("key")
    flat = []
    for schema_key, rules in columns.items():
        if schema_key not in ("by", "key"):
            flat.extend(_scoped(entry, str(schema_key)) for entry in _rule_entries(rules))
    return flat, selector


def _rule_entries(rules):
    """The column rules under one key of a `columns` mapping: a list of rules, or a mapping of canonical names."""
    if isinstance(rules, list):
        return [dict(rule) for rule in rules]
    if not isinstance(rules, dict):
        return []
    entries = []
    for col_name, details in rules.items():
        if isinstance(details, dict):
            entry = dict(details)
            entry.setdefault("canonical", str(col_name))
        else:
            entry = {"header": str(details), "canonical": str(col_name)}
        entries.append(entry)
    return entries


def _scoped(entry, schema_key):
    """
    A rule scoped by its `columns` key: `regex:PATTERN` (path_regex), a URI or absolute path (prefix), else a schema
    key.
    """
    if schema_key.startswith("regex:"):
        entry["path_regex"] = schema_key[6:]
    elif "://" in schema_key or schema_key.startswith("/"):
        entry["prefix"] = schema_key
    else:
        entry["schema_key"] = schema_key
    return entry


def _compile_problem(label, name, pattern):
    if not pattern or "{{" in str(pattern):
        return []
    try:
        re.compile(str(pattern))
    except re.error as exc:
        return ["%s: invalid %s %r: %s" % (label, name, pattern, exc)]
    return []


def _target_problems(label, row):
    """A mapping row's target (prefix, path_regex or schema_key) and its header matcher (header or regex)."""
    problems = []
    if row.get("prefix") is None and row.get("path_regex") is None and row.get("schema_key") is None:
        problems.append("%s: must specify either prefix, path_regex, or schema_key" % label)
    problems += _compile_problem(label, "path_regex", row.get("path_regex"))
    if not row.get("header") and not row.get("regex"):
        problems.append("%s: must specify either header or regex" % label)
    return problems + _compile_problem(label, "regex", row.get("regex"))


def _column_problems(label, row, vocab):
    """A mapping row's canonical name and type, against the controlled vocabulary when there is one."""
    problems = ["%s: %s is empty" % (label, key) for key in ("canonical", "type") if row.get(key) in (None, "")]
    canonical, kind = str(row.get("canonical", "")), str(row.get("type", "")).strip().lower()
    if canonical and not _NAME.match(canonical):
        problems.append("%s: canonical name %r is not a plain identifier" % (label, canonical))
    if kind and not TYPE_PATTERN.match(kind):
        problems.append("%s: type %r is not supported" % (label, kind))
    if vocab is None or not canonical:
        return problems
    if canonical not in vocab:
        target = row.get("prefix") or row.get("path_regex") or row.get("schema_key")
        problems.append("%s: %r (target %s) is not in the controlled vocabulary" % (label, canonical, target))
    elif kind and vocab[canonical] != kind:
        problems.append("%s: %r has type %s but the vocabulary says %s" % (label, canonical, kind, vocab[canonical]))
    return problems


def check_vocabulary(mappings, vocabulary):
    """Raises IngestError for mapping rows that are malformed or outside the controlled vocabulary."""
    norm_mappings, _ = normalize_columns(mappings)
    if not norm_mappings:
        return
    vocab = {row["name"]: str(row["type"]).strip().lower() for row in vocabulary} if vocabulary else None
    problems = []
    for number, row in enumerate(norm_mappings):
        label = "mapping row %d" % number
        problems += _target_problems(label, row) + _column_problems(label, row, vocab)
    if problems:
        more = " (and %d more)" % (len(problems) - 20) if len(problems) > 20 else ""
        raise IngestError("; ".join(problems[:20]) + more)


def normalize_row_index(row_index):
    """
    `row_index` as (name, start), or None when it is off:
      True -> ("_row_index", 0); False / None / "" -> None; "row_id" -> ("row_id", 0);
      {"name": "line_no", "start": 1} -> ("line_no", 1). Anything else is an IngestError.
    """
    if row_index is None or row_index is False:
        return None
    if row_index is True:
        return ("_row_index", 0)
    if isinstance(row_index, str):
        return (row_index.strip(), 0) if row_index.strip() else None
    if isinstance(row_index, dict):
        start = row_index.get("start", 0)
        if not isinstance(start, int) or isinstance(start, bool):
            raise IngestError("row_index.start must be an integer")
        return (str(row_index.get("name", "_row_index")).strip(), start)
    raise IngestError("row_index must be true/false, a column name or {name, start}, not %r" % (row_index,))
