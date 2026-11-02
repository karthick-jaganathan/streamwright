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
Multi-file, multi-schema ingest planning, shared by every engine (pure Python: an engine only lists files, reads
their headers and compiles the planned Groups into its own reads).

Plain reads (`schema_mode: by_name | strict`):
  Files are grouped by (format, options, header signature). `by_name` unions groups by column name; `strict`
  requires the first file's header signature everywhere and rejects deviating files (`FileReject`).

Column groups (`column_groups:`):
  Fold matching columns (glob such as `metric_*`, or regex) into typed map columns, with optional prefix stripping.

Mapped reads (`columns:` rules, optionally keyed by path prefix, path regex or a `path_pattern` group named by `by`):
  Header cells are matched to canonical names and types (`vocabulary` constrains both). Unmapped columns follow
  `on_unmapped_column` (ignore | warn | reject); a changed header signature is drift (`SchemaReport`).

Replay stability: an engine pins `serialize_plan(...)` in the batch manifest and rebuilds the groups from it on replay
without listing storage again.

Modules: model (the plan and its manifest form), headers, formats, column_rules, plain and mapped (plan_ingest).
"""

from streamwright.core.jobs.ingest.column_rules import (
    TYPE_PATTERN,
    UNMAPPED_COLUMN_POLICIES,
    check_vocabulary,
    is_true,
    normalize_columns,
    normalize_row_index,
)
from streamwright.core.jobs.ingest.formats import resolve_file_format, resolve_file_formats
from streamwright.core.jobs.ingest.headers import compute_header_hash, match_column_groups, parse_header
from streamwright.core.jobs.ingest.mapped import plan_ingest
from streamwright.core.jobs.ingest.model import (
    FileReject,
    Group,
    IngestError,
    SchemaReport,
    deserialize_plan,
    serialize_plan,
)
from streamwright.core.jobs.ingest.plain import plan_plain_groups

__all__ = [
    "TYPE_PATTERN",
    "UNMAPPED_COLUMN_POLICIES",
    "FileReject",
    "Group",
    "IngestError",
    "SchemaReport",
    "check_vocabulary",
    "compute_header_hash",
    "deserialize_plan",
    "is_true",
    "match_column_groups",
    "normalize_columns",
    "normalize_row_index",
    "parse_header",
    "plan_ingest",
    "plan_plain_groups",
    "resolve_file_format",
    "resolve_file_formats",
    "serialize_plan",
]
