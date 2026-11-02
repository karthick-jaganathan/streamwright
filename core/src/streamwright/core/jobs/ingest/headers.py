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

"""File headers: parsing, hashing (drift detection), comparison and column-group matching."""

import csv
import fnmatch
import hashlib
import io
import re


def compute_header_hash(header):
    """Deterministic hash of header cells for schema drift detection."""
    normalized = "\x1f".join(header) if header else ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def match_column_groups(header_cells, column_groups, mapped_1to1=None):
    """
    Matches header cells against column_groups rules (glob or regex).
    Returns (cg_assigned: dict, unmapped_cells: list).
    """
    mapped_set = set(mapped_1to1 or [])
    cg_assigned = {
        cg["name"]: {
            "type": str(cg.get("type", "string")).strip().lower(),
            "keep_nulls": bool(cg.get("keep_nulls", False)),
            "entries": [],
        }
        for cg in (column_groups or [])
    }
    unmapped = []
    for cell in header_cells:
        if cell in mapped_set:
            continue
        matched = False
        for cg in (column_groups or []):
            glob_pat = cg.get("glob")
            regex_pat = cg.get("regex")
            if (glob_pat and fnmatch.fnmatch(cell, glob_pat)) or (regex_pat and re.search(regex_pat, cell)):
                matched = True
            if matched:
                key_name = cell
                sp = cg.get("strip_prefix")
                if sp and cell.startswith(sp):
                    key_name = cell[len(sp):]
                cg_assigned[cg["name"]]["entries"].append((cell, key_name))
                break
        if not matched:
            unmapped.append(cell)
    return cg_assigned, unmapped


def parse_header(line, sep=","):
    """The cells of a CSV/TSV header line, stripped."""
    if not line:
        return []
    rows = list(csv.reader(io.StringIO(line.lstrip("\ufeff")), delimiter=sep))
    return [cell.strip() for cell in rows[0]] if rows else []


def first_header_difference(actual, expected):
    for i, (a, e) in enumerate(zip(actual, expected, strict=False)):
        if a != e:
            return "differing column at position %d: %r (expected %r)" % (i, a, e)
    if len(actual) < len(expected):
        return "missing column(s) at end: %s" % ", ".join(repr(c) for c in expected[len(actual):])
    if len(actual) > len(expected):
        return "extra column(s) at end: %s" % ", ".join(repr(c) for c in actual[len(expected):])
    return "headers differ"
