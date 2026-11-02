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

"""The ingest plan: read groups, rejected files and schema reports, and their manifest form."""

from dataclasses import dataclass


class IngestError(Exception):
    """A problem with the job's mapping or ingest configuration: the batch must fail."""


class FileReject:
    def __init__(self, path, reason):
        self.path, self.reason = path, reason

    def __repr__(self):
        return "FileReject(%r, %r)" % (self.path, self.reason)

    def __eq__(self, other):
        return isinstance(other, FileReject) and self.path == other.path and self.reason == other.reason

    __hash__ = None  # compared by value and mutable: unhashable


class Group:
    def __init__(self, prefix, header, columns, files, *, format="csv", options=None, column_groups=None,
                 path_vars_spec=None):
        self.prefix = prefix
        self.header = list(header)
        self.columns = list(columns)   # [(src, name, type_or_none)]
        self.files = list(files)
        self.format = format
        self.options = dict(options or {})
        self.column_groups = dict(column_groups or {})
        self.path_vars_spec = path_vars_spec  # (stripped_pattern, {var_name: group_index})

    def __repr__(self):
        return "Group(prefix=%r, format=%r, files=%d, columns=%d, column_groups=%d)" % (
            self.prefix, self.format, len(self.files), len(self.columns), len(self.column_groups))


@dataclass(repr=False)
class SchemaReport:
    step: str
    path: str
    prefix: str
    status: str  # "ok" | "drift" | "rejected"
    header_hash: str | None
    mapped: list
    unmapped: list
    missing_optional: list

    def __post_init__(self):
        self.mapped, self.unmapped = list(self.mapped), list(self.unmapped)
        self.missing_optional = list(self.missing_optional)

    def as_dict(self):
        return {
            "step": self.step,
            "path": self.path,
            "prefix": self.prefix,
            "status": self.status,
            "header_hash": self.header_hash,
            "mapped": self.mapped,
            "unmapped": self.unmapped,
            "missing_optional": self.missing_optional,
        }

    def as_tuple(self):
        return (
            self.step,
            self.path,
            self.prefix,
            self.status,
            self.header_hash,
            self.mapped,
            self.unmapped,
            self.missing_optional,
        )


def serialize_plan(groups, rejects, reports):
    """Serializes groups, rejects, and schema reports into a JSON-safe dict for manifest pinning."""
    return {
        "groups": [
            {
                "prefix": g.prefix,
                "header": g.header,
                "columns": g.columns,
                "files": g.files,
                "format": g.format,
                "options": g.options,
                "column_groups": g.column_groups,
                "path_vars_spec": list(g.path_vars_spec) if getattr(g, "path_vars_spec", None) else None,
            }
            for g in groups
        ],
        "rejects": [
            {"path": r.path, "reason": r.reason}
            for r in rejects
        ],
        "reports": [
            r.as_dict() if hasattr(r, "as_dict") else dict(r)
            for r in reports
        ],
    }


def deserialize_plan(data):
    """Reconstructs (groups, rejects, reports) from pinned manifest data."""
    groups = [
        Group(
            prefix=g["prefix"],
            header=g["header"],
            columns=g["columns"],
            files=g["files"],
            format=g.get("format", "csv"),
            options=g.get("options"),
            column_groups=g.get("column_groups"),
            path_vars_spec=tuple(g["path_vars_spec"]) if g.get("path_vars_spec") else None,
        )
        for g in data.get("groups", [])
    ]
    rejects = [
        FileReject(path=r["path"], reason=r["reason"])
        for r in data.get("rejects", [])
    ]
    reports = [
        SchemaReport(**r)
        for r in data.get("reports", [])
    ]

    return groups, rejects, reports
