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

"""Plain reads (`schema_mode: by_name | strict`) and named `path_pattern` groups."""

import re

from streamwright.core.jobs.ingest.headers import compute_header_hash, first_header_difference, match_column_groups
from streamwright.core.jobs.ingest.model import FileReject, Group, IngestError, SchemaReport


def path_variables(path_pattern):
    """(compiled pattern, (pattern without group names, {name: group index})) of a path_pattern, or (None, None)."""
    if path_pattern is None:
        return None, None
    compiled = re.compile(str(path_pattern))
    if not compiled.groupindex:
        return compiled, None
    stripped = re.sub(r"\(\?P?<[a-zA-Z_][a-zA-Z0-9_]*>", "(", str(path_pattern))
    return compiled, (stripped, dict(compiled.groupindex))


def plan_plain_groups(files, headers, formats, *, schema_mode="by_name", options=None, max_groups=200, step_name="",
                      column_groups=None, path_pattern=None):
    """
    Plans groups for plain reads (no column mapping), with optional pattern-based column_groups.
    schema_mode:
      - 'by_name' (default): files with the same format and header signature are grouped together.
      - 'strict': all files must share the exact format and header signature as the first file;
                  deviating files are rejected into FileReject with the first differing column.
    Returns (groups: list[Group], rejects: list[FileReject], reports: list[SchemaReport]).
    """
    groups = {}
    rejects = []
    reports = []
    options = dict(options or {})
    compiled_pp, path_vars_spec = path_variables(path_pattern)
    sorted_files = []
    for path in sorted(set(files)):
        if compiled_pp is not None and not compiled_pp.search(path):
            rejects.append(FileReject(path, "path does not match path_pattern: %s" % path_pattern))
            reports.append(SchemaReport(step_name, path, "", "rejected", "", [], headers.get(path, []), []))
        else:
            sorted_files.append(path)

    if not sorted_files:
        return [], rejects, reports

    if schema_mode == "strict":
        baseline_file = sorted_files[0]
        expected_format = formats.get(baseline_file)
        expected_header = headers.get(baseline_file, [])
        valid_files = []

        for path in sorted_files:
            fmt = formats.get(path)
            hdr = headers.get(path, [])
            hdr_hash = compute_header_hash(hdr)
            if fmt != expected_format:
                reason = "format mismatch: %s (expected %s)" % (fmt, expected_format)
                rejects.append(FileReject(path, reason))
                reports.append(SchemaReport(step_name, path, "", "rejected", hdr_hash, [], hdr, []))
            elif hdr != expected_header:
                reason = "schema mismatch: %s" % first_header_difference(hdr, expected_header)
                rejects.append(FileReject(path, reason))
                reports.append(SchemaReport(step_name, path, "", "rejected", hdr_hash, [], hdr, []))
            else:
                valid_files.append(path)
                reports.append(SchemaReport(step_name, path, "", "ok", hdr_hash, hdr, [], []))

        if valid_files:
            cg_assigned, remaining_cols = match_column_groups(expected_header, column_groups, mapped_1to1=set())
            cols = [(c, c, None) for c in remaining_cols]
            grp = Group(prefix="", header=expected_header, columns=cols, files=valid_files,
                        format=expected_format, options=options, column_groups=cg_assigned,
                        path_vars_spec=path_vars_spec)
            groups_list = [grp]
        else:
            groups_list = []
        return groups_list, rejects, reports

    # schema_mode == "by_name"
    for path in sorted_files:
        fmt = formats.get(path, "csv")
        hdr = headers.get(path, [])
        hdr_hash = compute_header_hash(hdr)
        cg_assigned, remaining_cols = match_column_groups(hdr, column_groups, mapped_1to1=set())
        cols = [(c, c, None) for c in remaining_cols]
        cg_key = tuple(sorted((k, tuple(v["entries"])) for k, v in cg_assigned.items()))
        key = (fmt, tuple(sorted(options.items())), tuple(remaining_cols), cg_key)
        if key not in groups:
            groups[key] = Group(prefix="", header=list(hdr), columns=cols, files=[path],
                                format=fmt, options=options, column_groups=cg_assigned,
                                path_vars_spec=path_vars_spec)
        else:
            groups[key].files.append(path)
        mapped_summary = remaining_cols + [k for k, v in cg_assigned.items() if v["entries"]]
        reports.append(SchemaReport(step_name, path, "", "ok", hdr_hash, mapped_summary, [], []))

    if len(groups) > max_groups:
        raise IngestError("%d read groups exceed runtime.limits.read_groups (%d)" % (len(groups), max_groups))

    return [groups[k] for k in sorted(groups, key=lambda k: (k[0], k[2]))], rejects, reports
