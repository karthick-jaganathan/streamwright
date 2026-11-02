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
Reading `kind: *Job` manifests and merging --overlay files.

A manifest is read through a reader chosen by the location's URI scheme. Local files (no scheme) are built in; an engine
adds others (an object-store reader, for example) by passing `readers={"<scheme>": read_function}`. A reader takes the
location and the size limit and returns the text; it reads at most `limit + 1` characters so an oversized file is
reported.

A directory location means its `job.yaml` (or `job.yml`). YAML errors report the line and column, never the text of
the file: a manifest may hold values that must not reach logs.
"""

import copy
import os
import re

import yaml

__all__ = [
    "MANIFEST_FILE_NAMES",
    "MAX_MANIFEST_BYTES",
    "JobError",
    "load_job",
    "load_job_text",
    "manifest_directory",
    "merge_overlay",
    "read_local",
]

MAX_MANIFEST_BYTES = 5 * 1024 * 1024
MANIFEST_FILE_NAMES = ("job.yaml", "job.yml")
_SCHEME = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*)://")


class JobError(Exception):
    """A manifest or overlay cannot be read or is not a YAML mapping."""


def read_local(location, limit):
    """The text of a local manifest (a file, or a directory holding job.yaml / job.yml)."""
    path = str(location)
    if os.path.isdir(path):
        for name in MANIFEST_FILE_NAMES:
            candidate = os.path.join(path, name)
            if os.path.isfile(candidate):
                path = candidate
                break
        else:
            raise JobError("directory %r has no %s" % (path, " or ".join(MANIFEST_FILE_NAMES)))
    try:
        with open(path, encoding="utf-8") as stream:
            return stream.read(limit + 1)
    except OSError as exc:
        raise JobError("cannot read %r: %s" % (path, exc.strerror or type(exc).__name__)) from exc


def manifest_directory(location):
    """The directory relative paths in a local manifest are resolved against (None for a remote location)."""
    path = str(location)
    if _SCHEME.match(path):
        return None
    path = os.path.abspath(path)
    return path if os.path.isdir(path) else os.path.dirname(path)


def _read(location, readers):
    text_location = str(location)
    match = _SCHEME.match(text_location)
    if match:
        reader = (readers or {}).get(match.group(1).lower())
        if reader is None:
            raise JobError("cannot read %r: no reader for %s:// locations (readers: %s)" % (
                text_location, match.group(1), ", ".join(sorted(readers or {})) or "local files only"))
    else:
        reader = read_local
    return _parse(reader(location, MAX_MANIFEST_BYTES), repr(text_location))


def _parse(text, where):
    if len(text) > MAX_MANIFEST_BYTES:
        raise JobError("%s is larger than %d bytes" % (where, MAX_MANIFEST_BYTES))
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        position = " at line %d, column %d" % (mark.line + 1, mark.column + 1) if mark is not None else ""
        problem = getattr(exc, "problem", None)
        raise JobError("%s is not valid YAML%s%s" % (where, position, ": %s" % problem if problem else "")) from None
    if not isinstance(data, dict):
        raise JobError("%s must be a mapping" % where)
    return data


def load_job(location, overlays=(), readers=None):
    """The manifest at `location` (a path, a directory or a URI with a reader) with `overlays` merged in order."""
    job = _read(location, readers)
    for overlay in overlays:
        job = merge_overlay(job, _read(overlay, readers))
    return job


def load_job_text(text, overlays=(), readers=None):
    """A manifest given as YAML text, with `overlays` (locations) merged in order."""
    if not isinstance(text, str):
        raise JobError("the job text must be a string, not %s" % type(text).__name__)
    job = _parse(text, "the job text")
    for overlay in overlays:
        job = merge_overlay(job, _read(overlay, readers))
    return job


def merge_overlay(base, overlay):
    """Mappings merge by key, anything else replaces; a null in the overlay deletes the key."""
    result = copy.deepcopy(base)
    for key, value in overlay.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge_overlay(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result
