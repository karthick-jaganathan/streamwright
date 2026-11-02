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

"""File formats from explicit settings or file extensions."""

from types import MappingProxyType

from streamwright.core.jobs.ingest.model import FileReject

_COMPRESSED = (".gz", ".gzip", ".zst", ".zstd", ".bz2", ".snappy")
_EXT = MappingProxyType({
    ".csv": "csv",
    ".tsv": "tsv",
    ".tab": "tsv",
    ".json": "json",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".parquet": "parquet",
    ".pq": "parquet",
})


def resolve_file_format(path, explicit_format="auto"):
    """
    Resolves the format for a path.
    If explicit_format != 'auto', returns explicit_format.
    Otherwise infers from file extension.
    Returns format string, or None if unknown.
    """
    if explicit_format and explicit_format != "auto":
        return explicit_format
    name = path.lower().split("?")[0]
    for ending in _COMPRESSED:
        if name.endswith(ending):
            name = name[:-len(ending)]
    for ending, found in _EXT.items():
        if name.endswith(ending):
            return found
    return None


def resolve_file_formats(files, explicit_format="auto"):
    """
    Returns (resolved_formats: {path: format}, rejects: [FileReject]).
    """
    resolved = {}
    rejects = []
    for path in sorted(set(files)):
        fmt = resolve_file_format(path, explicit_format)
        if fmt is None:
            rejects.append(FileReject(path, "cannot determine format from file extension"))
        else:
            resolved[path] = fmt
    return resolved, rejects
