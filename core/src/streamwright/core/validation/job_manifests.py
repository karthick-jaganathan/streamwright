#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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
The units `streamwright validate` checks under a path: files, source folders and job manifests. A job manifest is a
YAML file whose `kind` is an installed job kind (e.g. `google_ads/campaigns.yaml`, `kind: DuckDBJob`) or a folder's
`job.yaml`; its overlays are the files named `<job>.<environment>.yaml` beside it (e.g. `campaigns.local.yaml`), checked
merged onto it, never on their own.
"""


import os
from dataclasses import replace

import yaml

from streamwright.core.config import loader as source_files
from streamwright.core.jobs.engine import load_job_engine
from streamwright.core.jobs.manifest import MANIFEST_FILE_NAMES
from streamwright.core.runtime import plugins
from streamwright.core.runtime.plugins import PluginError
from streamwright.core.validation.checker import ERROR
from streamwright.core.validation.issues import Issue
from streamwright.core.validation.marked_yaml import parse_yaml, read_file

# what a unit of validate_paths is: one file, a source folder (checked as one source) or a job manifest
FILE_UNIT, SOURCE_UNIT, JOB_UNIT = "file", "source", "job"
_YAML = (".yaml", ".yml")


def _declared_kind(path):
    """The top-level `kind` of a YAML file, or None (unreadable or not a mapping: the file's own checks report it)."""
    try:
        with open(path, encoding="utf-8") as stream:
            data = yaml.safe_load(stream)
    except (OSError, UnicodeDecodeError, yaml.YAMLError):
        return None
    return data.get("kind") if isinstance(data, dict) else None


def _job_manifests(folder, names, job_kinds):
    """The job manifests among a folder's YAML file `names`: `job.yaml` / `job.yml`, or a file of a job kind."""
    return [name for name in names
            if name in MANIFEST_FILE_NAMES or _declared_kind(os.path.join(folder, name)) in job_kinds]


def _overlay_names(manifest, names):
    """The overlays of `manifest` among `names`: `<job>.<environment>.yaml` beside `<job>.yaml`."""
    stem = os.path.splitext(manifest)[0] + "."
    return sorted(name for name in names if name != manifest and name.startswith(stem) and name.endswith(_YAML))


def validation_units(path):
    """What to check under `path`, in order: (path, FILE_UNIT | SOURCE_UNIT | JOB_UNIT)."""
    job_kinds = {entry["name"] for entry in plugins.discover(plugins.JOB_KINDS)}
    if not os.path.isdir(path):
        folder = source_files.source_folder(path)
        if folder is not None:
            return [(folder, SOURCE_UNIT)]
        directory, name = os.path.split(path)
        is_job = _job_manifests(directory or ".", [name], job_kinds)
        return [(path, JOB_UNIT if is_job else FILE_UNIT)]
    units = []
    for root, dirs, files in os.walk(path):
        folder = source_files.source_folder(root)  # the folder itself, or the source of a streams/ folder
        if folder is not None:
            units.append((folder, SOURCE_UNIT))
            dirs[:] = []
            continue
        names = sorted(name for name in files if name.endswith(_YAML))
        manifests = _job_manifests(root, names, job_kinds)
        overlays = {overlay for manifest in manifests for overlay in _overlay_names(manifest, names)}
        dirs[:] = [] if manifests else sorted(d for d in dirs if not d.startswith("."))  # a job's own data
        for name in names:
            file_path = os.path.join(root, name)
            if name in manifests:
                units.append((file_path, JOB_UNIT))
            elif name not in overlays:
                folder = source_files.source_folder(file_path)  # e.g. a streams/ folder given on its own
                units.append((folder, SOURCE_UNIT) if folder is not None else (file_path, FILE_UNIT))
    return units


def _job_issues(report, file):
    """A JobReport's issues, each pointing to `file` unless the engine located it already."""
    return [issue if issue.file else replace(issue, file=file) for issue in report.issues]


def validate_job_manifest(manifest):
    """
    A job manifest through the engine of its `kind`, then each of its overlays (`<job>.<environment>.yaml`) merged
    onto it; only what an overlay adds is reported. Returns (files_checked, issues).
    """
    folder, name = os.path.split(manifest)
    overlays = [os.path.join(folder, overlay) for overlay in _overlay_names(name, os.listdir(folder or "."))]
    text, issues = read_file(manifest)
    if text is None:
        return [manifest], issues
    loader, data, issues = parse_yaml(text, manifest)
    if loader is None:
        return [manifest], issues
    loader.dispose()
    kind = data.get("kind") if isinstance(data, dict) else None
    try:
        engine = load_job_engine(kind) if isinstance(kind, str) else None
    except PluginError as exc:
        return [manifest], [Issue(ERROR, "unknown-kind", "job kind %r: %s" % (kind, exc), manifest)]
    if engine is None:
        return [manifest], [Issue(ERROR, "missing-key", "missing `kind` (the job kind of this manifest)", manifest)]
    found = _job_issues(engine.validate_job(manifest), manifest)
    seen = {(issue.code, issue.message) for issue in found}
    for overlay in overlays:
        # what only the merged job has comes from the overlay: point there (the manifest's line numbers don't apply)
        found.extend(replace(issue, file=overlay, line=None, column=None)
                     for issue in engine.validate_job(manifest, overlays=[overlay]).issues
                     if (issue.code, issue.message) not in seen)
    return [manifest, *overlays], found
