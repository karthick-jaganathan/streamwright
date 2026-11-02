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

"""Mapped reads (`columns:` rules) and plan_ingest, the planner of every read."""

import re

from streamwright.core.jobs.ingest.column_rules import UNMAPPED_COLUMN_POLICIES, is_true, normalize_columns
from streamwright.core.jobs.ingest.headers import compute_header_hash, match_column_groups
from streamwright.core.jobs.ingest.model import FileReject, Group, IngestError, SchemaReport
from streamwright.core.jobs.ingest.plain import path_variables, plan_plain_groups


def _rule_sets(rows):
    """({schema key: rule set}, {prefix or path_regex: rule set}) of normalized mapping rows."""
    by_schema_key, by_path = {}, {}
    for row in rows:
        if row.get("schema_key") is not None:
            key = str(row["schema_key"])
            by_schema_key.setdefault(key, {"schema_key": key, "rows": []})["rows"].append(row)
            continue
        prefix = str(row["prefix"]) if row.get("prefix") is not None else None
        path_regex = str(row["path_regex"]) if row.get("path_regex") is not None else None
        key = prefix if prefix is not None else path_regex
        if key not in by_path:
            by_path[key] = {"prefix": prefix, "path_regex": path_regex,
                            "compiled_path_regex": re.compile(path_regex) if path_regex else None, "rows": []}
        by_path[key]["rows"].append(row)
    return by_schema_key, by_path


def _map_header(header, rows):
    """([(cell, canonical, type)], {canonical: cell}, problem): the cells the rows map (exact header, then regex)."""
    exact = {str(row["header"]).strip(): row for row in rows if row.get("header") is not None}
    patterns = [(re.compile(str(row["regex"])), row) for row in rows if row.get("regex") is not None]
    chosen, seen = [], {}
    for cell in header:
        row = exact.get(cell)
        if row is None:
            row = next((candidate for pattern, candidate in patterns if pattern.search(cell)), None)
        if row is None:
            continue
        name = str(row["canonical"])
        if name in seen:
            return chosen, seen, "columns %r and %r both map to %s" % (seen[name], cell, name)
        seen[name] = cell
        chosen.append((cell, name, str(row["type"]).strip().lower()))
    return chosen, seen, None


class _FilePlan:
    """One file of a mapped read: its report, rule-set label, mapped columns and column groups, or why rejected."""

    def __init__(self, report, label, columns=None, column_groups=None, reason=None):
        self.report, self.label, self.columns, self.column_groups, self.reason = (
            report, label, columns, column_groups, reason)


class _MappedPlanner:
    """Plans the files of a mapped read: the column-rule set of each file (by path or schema key) and its header."""

    def __init__(self, rows, selector, path_pattern, column_groups, *, on_unmapped_column, previous_signatures,
                 step_name):
        self.path_pattern, self.column_groups, self.on_unmapped_column = path_pattern, column_groups, on_unmapped_column
        self.previous_signatures, self.step_name = previous_signatures, step_name
        self.compiled, self.path_vars_spec = path_variables(path_pattern)
        self.selects_by_key = any(row.get("schema_key") is not None for row in rows)
        self.selector = self._selector(selector)
        self.by_schema_key, by_path = _rule_sets(rows)
        regex_rules = [rule for rule in by_path.values() if rule["compiled_path_regex"] is not None]
        prefix_rules = [rule for rule in by_path.values()
                        if rule["compiled_path_regex"] is None and rule["prefix"] is not None]
        self.path_rules = regex_rules + sorted(prefix_rules, key=lambda rule: len(rule["prefix"] or ""), reverse=True)

    def _selector(self, selector):
        if selector is not None or self.compiled is None or not self.selects_by_key:
            return selector
        if len(self.compiled.groupindex) != 1:
            raise IngestError("path_pattern has %d named groups; name the one that selects the column rules with "
                              "columns.by" % len(self.compiled.groupindex))
        return next(iter(self.compiled.groupindex))

    def _schema_key(self, path):
        values = self.compiled.search(path).groupdict()
        if not self.selector:
            return next(iter(values.values())) if values else None
        if "{{" not in self.selector:
            return values.get(self.selector)
        key = self.selector
        for name, value in values.items():
            key = key.replace("{{ %s }}" % name, str(value)).replace("{{%s}}" % name, str(value))
        return key

    def _rule_set(self, path):
        """(rule set, label, None), or (None, label, why the path has no column rules)."""
        if self.compiled is not None and not self.compiled.search(path):
            return None, "", "path does not match path_pattern: %s" % self.path_pattern
        if self.compiled is not None and self.selects_by_key:
            key = self._schema_key(path)
            if key not in self.by_schema_key:
                return None, str(key or ""), "unknown schema key %r extracted from path" % (key,)
            return self.by_schema_key[key], str(key), None
        for rule in self.path_rules:
            pattern = rule["compiled_path_regex"]
            if pattern.search(path) if pattern is not None else path.startswith(rule["prefix"]):
                return rule, rule["prefix"] or rule["path_regex"] or "", None
        return None, "", "no column mapping for this path"

    def _header_problem(self, rows, seen, chosen, column_groups, unmapped):
        required = sorted({str(row["canonical"]) for row in rows if is_true(row.get("required"))})
        missing = [name for name in required if name not in seen]
        if missing:
            return "missing required column(s): %s" % ", ".join(missing)
        if not chosen and not any(group["entries"] for group in column_groups.values()):
            return "no header cell matches a mapped column or column group"
        if unmapped and self.on_unmapped_column == "reject":
            return "unmapped column(s): %s" % ", ".join(unmapped)
        return None

    def plan_file(self, path, header):
        rule_set, label, reason = self._rule_set(path)
        if reason is not None:
            return _FilePlan(SchemaReport(self.step_name, path, label, "rejected", "", [], header or [], []), label,
                             reason=reason)
        if not header:
            return _FilePlan(SchemaReport(self.step_name, path, label, "rejected", "", [], [], []), label,
                             reason="empty or unreadable header")
        rows, header_hash = rule_set["rows"], compute_header_hash(header)
        chosen, seen, problem = _map_header(header, rows)
        column_groups, unmapped = match_column_groups(header, self.column_groups,
                                                      mapped_1to1={cell for cell, _, _ in chosen})
        optional = {str(row["canonical"]) for row in rows if not is_true(row.get("required"))}
        missing_optional = sorted(optional - set(seen))
        problem = problem or self._header_problem(rows, seen, chosen, column_groups, unmapped)
        mapped = [name for _, name, _ in chosen] + [key for key, group in column_groups.items() if group["entries"]]
        if problem is not None:
            return _FilePlan(SchemaReport(self.step_name, path, label, "rejected", header_hash, mapped, unmapped,
                                          missing_optional), label, reason=problem)
        previous = self.previous_signatures.get(label)
        status = "drift" if unmapped or (previous is not None and previous != header_hash) else "ok"
        return _FilePlan(SchemaReport(self.step_name, path, label, status, header_hash, mapped, unmapped,
                                      missing_optional), label, chosen, column_groups)


def plan_ingest(files, mappings=None, headers=None, formats=None, *, schema_mode="by_name",
                on_unmapped_column="warn", previous_signatures=None, options=None,
                max_groups=200, step_name="", column_groups=None, path_pattern=None,
                inject_path_vars=True):
    """
    Unified ingest planner for plain reads (mappings is None) and mapped reads (mappings is provided).
    Supports column_groups pattern folding into Map columns and named path_pattern extraction.
    Returns (groups: list[Group], rejects: list[FileReject], reports: list[SchemaReport]).
    """
    headers, formats, options = headers or {}, formats or {}, dict(options or {})
    if on_unmapped_column not in UNMAPPED_COLUMN_POLICIES:
        raise IngestError("on_unmapped_column must be one of %s" % ", ".join(UNMAPPED_COLUMN_POLICIES))
    if schema_mode not in ("by_name", "strict"):
        raise IngestError("schema_mode must be by_name or strict")
    norm_mappings, selector = normalize_columns(mappings)
    if norm_mappings is None:
        return plan_plain_groups(files=files, headers=headers, formats=formats, schema_mode=schema_mode,
                                 options=options, max_groups=max_groups, step_name=step_name,
                                 column_groups=column_groups, path_pattern=path_pattern)
    planner = _MappedPlanner(norm_mappings, selector, path_pattern, column_groups,
                             on_unmapped_column=on_unmapped_column, previous_signatures=previous_signatures or {},
                             step_name=step_name)
    path_vars_spec = planner.path_vars_spec if inject_path_vars else None
    groups, rejects, reports = {}, [], []
    for path in sorted(set(files)):
        planned = planner.plan_file(path, headers.get(path))
        reports.append(planned.report)
        if planned.reason is not None:
            rejects.append(FileReject(path, planned.reason))
            continue
        header, fmt = headers[path], formats.get(path, "csv")
        assigned = tuple(sorted((k, tuple(v["entries"])) for k, v in planned.column_groups.items()))
        key = (fmt, planned.label, tuple(sorted(options.items())), tuple(header), assigned)
        if key in groups:
            groups[key].files.append(path)
        else:
            groups[key] = Group(planned.label, list(header), planned.columns, [path], format=fmt, options=options,
                                column_groups=planned.column_groups, path_vars_spec=path_vars_spec)
    if len(groups) > max_groups:
        raise IngestError("%d read groups exceed runtime.limits.read_groups (%d): clients' headers vary more than "
                          "expected" % (len(groups), max_groups))
    return [groups[key] for key in sorted(groups, key=lambda k: (k[0], k[1], k[3]))], rejects, reports
