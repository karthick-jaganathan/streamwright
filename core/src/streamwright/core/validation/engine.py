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
`streamwright validate`: static checks for StreamWright source configurations (`kind: source`).

Validates sources without running them: YAML problems, unknown or misspelled keys, missing keys, wrong value
types, and patterns known to fail (or to silently produce wrong data) at run time. Each finding points to the
file, line and YAML path:

    source.yaml:12:5: error [unknown-key] auth.provder: unknown key 'provder' (did you mean 'provider'?); ...

A source folder (source.yaml plus one file per stream in streams/) is checked as one source, and its findings
point to the file they are in. This module holds the reporting and entry points (YAML loading with source
locations: validation.marked_yaml; job manifests and their overlays: validation.job_manifests); the source checks
themselves are in validation.source.SourceChecker.

Exit status: 0 = no errors, 1 = errors (or warnings with --strict), 2 = usage error.
"""

import argparse
import json
import os
import sys

import yaml

from streamwright.core.config import loader as source_files
from streamwright.core.engine import sql
from streamwright.core.runtime import components
from streamwright.core.validation import schema
from streamwright.core.validation.checker import ERROR, NON_STRICT_CODES, WARNING, suggest
from streamwright.core.validation.issues import Issue
from streamwright.core.validation.job_manifests import FILE_UNIT, JOB_UNIT, validate_job_manifest, validation_units
from streamwright.core.validation.marked_yaml import FolderLoaders, parse_yaml, read_file
from streamwright.core.validation.source import SourceChecker

__all__ = [
    "ERROR",
    "NON_STRICT_CODES",
    "WARNING",
    "Issue",
    "check_components",
    "is_valid",
    "main",
    "validate_file",
    "validate_paths",
    "validate_source",
    "validate_text",
]



def is_valid(issues, *, strict=False, non_strict_codes=NON_STRICT_CODES):
    """The verdict on validation issues: no errors and, when `strict`, no warnings outside `non_strict_codes`."""
    return not any(issue.severity == ERROR or (strict and issue.code not in non_strict_codes) for issue in issues)


def _locate(document, path):
    """(container, key) of the deepest part of `path` in the document: where a finding is shown (file and line)."""
    container, key, node = None, None, document
    for part in path:
        if (isinstance(node, dict) and part in node) or \
                (isinstance(node, list) and isinstance(part, int) and 0 <= part < len(node)):
            container, key, node = node, part, node[part]
        else:
            break
    return container, key


def check_components(checker, source):
    """
    Validation callback for `source_check`: checks against installed connectors and query builders,
    and validates streams' DuckDB SQL transforms (compiled by DuckDB).
    Reports 'connector-not-installed' (warning), 'connector-check' (error), and 'sql-check' (error).
    """
    for path, message, missing in components.component_problems(source, checker.options.allowed_connectors):
        container, key = _locate(source, path)
        if missing:
            checker.warn("connector-not-installed", path, "%s; the source is not checked against it" % message,
                         container, key)
        else:
            checker.error("connector-check", path, message, container, key)
    for path, message in sql.check_source(source):
        container, key = _locate(source, path)
        checker.error("sql-check", path, message, container, key)

# * -------
# * helpers
# * -------

class _Options:

    def __init__(self, kind=None, allowed_connectors=None, source_check=None, connector_metadata=None):
        self.kind = kind
        self.allowed_connectors = allowed_connectors
        # source_check(checker, document) adds checks after the built-in ones pass (e.g. the connectors')
        self.source_check = source_check
        self.connector_metadata = connector_metadata or self._describe_connector

    def _describe_connector(self, name):
        return components.describe(name, allowed=self.allowed_connectors)


# * -----------
# * entry points
# * -----------
def _sorted(issues, files=()):
    order = {path: index for index, path in enumerate(files)}
    return sorted(issues, key=lambda i: (order.get(i.file, -1), i.line or 0, i.column or 0))


def _check_kind(checker, data):
    """A single file: reports a missing or other `kind`; True when the file is to be checked as a source."""
    if not isinstance(data, dict):
        return True  # check_document reports it
    kind = data.get("kind", checker.options.kind)
    if kind == schema.KIND:
        return True
    if kind is None:
        checker.error("missing-key", (), "missing `kind: %s` (or pass --kind %s)" % (
            schema.KIND, schema.KIND), data)
        return True
    checker.check_preamble(data)
    in_file = "kind" in data
    checker.error("unknown-kind", ("kind",) if in_file else (), "unknown kind %r%s; streamwright validate checks "
                  "`kind: %s` configurations" % (kind, suggest(kind, (schema.KIND,)), schema.KIND),
                  data, "kind" if in_file else None)
    return False


def validate_text(text, filename=None, kind=None, allowed_connectors=None, source_check=None, *,
                  connector_metadata=None):
    """
    Validates YAML text; returns a list of Issue. For sources without errors, source_check(checker, document) can
    add more (see SourceChecker).
    """
    options = _Options(kind, allowed_connectors, source_check, connector_metadata)
    loader, data, issues = parse_yaml(text, filename)
    if loader is None:
        return issues
    try:
        checker = SourceChecker(filename, loader, options)
        if _check_kind(checker, data):
            checker.check_document(data)
        return _sorted(checker.issues)
    finally:
        loader.dispose()


def _validate_folder(layout, options):
    """A source folder: source.yaml and the stream files, checked as one source."""
    issues = [Issue(ERROR, "source-folder", message, path) for path, message in layout.problems]
    loaders, documents = [], []
    try:
        for path in layout.files:
            text, problems = read_file(path)
            loader, data, problems = (None, None, problems) if text is None else parse_yaml(text, path)
            issues.extend(problems)
            if loader is not None:
                loaders.append(loader)
            documents.append(data)
        if len(loaders) < len(documents):  # a file is unreadable or not YAML: the source cannot be put together
            return _sorted(issues, layout.files)

        source = documents[0]
        problems, stream_paths = source_files.assemble(layout, source, documents[1:])
        combined = FolderLoaders(loaders)
        checker = SourceChecker(layout.source_file, combined, options)
        checker.folder = layout.folder
        checker.files = frozenset(layout.files)
        checker.stream_files = dict(enumerate(stream_paths))
        for key, paths in (("streams", stream_paths),):
            if paths and isinstance(source, dict):
                # the list is made here, not read: give it a node whose items are the files' mappings
                top = combined.streamwright_nodes[id(source)]
                combined.streamwright_nodes[id(source[key])] = yaml.SequenceNode(
                    "tag:yaml.org,2002:seq", [combined.streamwright_nodes[id(item)] for item in source[key]],
                    top.start_mark, top.end_mark)
        for path, container, key, message in problems:
            checker._add(ERROR, "source-folder", (), message, container=container, key=key, file=path)
        if isinstance(source, dict) and source.get("kind", options.kind) != schema.KIND:
            if "kind" in source:
                checker.error("bad-value", ("kind",), "a source folder's source.yaml is `kind: source`, got %r" % (
                    source["kind"],), source, "kind")
                return _sorted(issues + checker.issues, layout.files)
            checker.error("missing-key", (), "missing `kind: source`", source)
        checker.check_document(source)
        return _sorted(issues + checker.issues, layout.files)
    finally:
        for loader in loaders:
            loader.dispose()


def validate_source(path, kind=None, allowed_connectors=None, source_check=None, connector_metadata=None):
    """
    Validates the source at `path`: a source folder as one source (given as the folder, its source.yaml or one of
    its stream files; or a SourceLayout), or a single file. Returns a list of Issue.
    """
    if isinstance(path, source_files.SourceLayout):
        layout = path
    else:
        if not os.path.exists(path):
            return [Issue(ERROR, "unreadable-file", source_files.missing_path_message(path), path)]
        try:
            layout = source_files.find_source(path)
        except source_files.SourceFilesError as exc:
            return [Issue(ERROR, "source-folder", str(exc), path)]
    if layout.folder is not None:
        return _validate_folder(layout, _Options(kind, allowed_connectors, source_check, connector_metadata))
    text, issues = read_file(layout.source_file)
    if text is None:
        return issues
    return validate_text(text, layout.source_file, kind, allowed_connectors, source_check,
                         connector_metadata=connector_metadata)


def validate_file(path, kind=None, allowed_connectors=None, source_check=None):
    """Validates one YAML file (for a file of a source folder: the whole folder); returns a list of Issue."""
    if source_files.source_folder(path) is not None:
        return validate_source(path, kind, allowed_connectors, source_check)
    text, issues = read_file(path)
    if text is None:
        return issues
    return validate_text(text, path, kind, allowed_connectors, source_check)


def validate_paths(paths, kind=None, allowed_connectors=None, source_check=None):
    """
    Validates files and directories (recursively; each source folder as one source, each job manifest with its
    overlays through the engine of its kind); returns (files_checked, issues).
    """
    files, issues, folders = [], [], set()
    for path in paths:
        if not os.path.exists(path):
            issues.append(Issue(ERROR, "unreadable-file", source_files.missing_path_message(path), path))
            continue
        for unit, unit_type in validation_units(path):
            if unit_type == FILE_UNIT:
                files.append(unit)
                issues.extend(validate_file(unit, kind, allowed_connectors, source_check))
                continue
            if os.path.realpath(unit) in folders:
                continue
            folders.add(os.path.realpath(unit))
            if unit_type == JOB_UNIT:
                checked, found = validate_job_manifest(unit)
                files.extend(checked)
                issues.extend(found)
                continue
            try:
                layout = source_files.find_source(unit)
            except source_files.SourceFilesError as exc:
                issues.append(Issue(ERROR, "source-folder", str(exc), unit))
                continue
            files.extend(layout.files)
            issues.extend(validate_source(layout, kind, allowed_connectors, source_check))
    return files, issues


def export_schemas(directory):
    """Writes source.schema.json and stream.schema.json (the stream files of source folders) to directory."""
    schemas = [("%s.schema.json" % schema.KIND, schema.json_schema()),
               ("stream.schema.json", schema.stream_json_schema())]
    written = []
    for relative, document in schemas:
        target = os.path.join(directory, relative)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "w", encoding="utf-8") as stream:
            json.dump(document, stream, indent=2, sort_keys=True)
            stream.write("\n")
        written.append(target)
    return written


def _parser(prog="streamwright validate"):
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Validate StreamWright source configurations (`kind: source`) without running them.")
    parser.add_argument("paths", nargs="*", metavar="PATH",
                        help="files or directories to validate, recursively; a source folder (source.yaml and "
                             "streams/) is checked as one source (default: $STREAMWRIGHT_CONFIGS)")
    parser.add_argument("--kind", choices=(schema.KIND,),
                        help="kind to assume for files that do not declare `kind`")
    parser.add_argument("--allow-connector", action="append", dest="allowed_connectors", metavar="NAME",
                        help="sources: only allow these connectors in `auth.provider` and `sdk` (repeatable)")
    parser.add_argument("--strict", action="store_true", help="exit with status 1 on warnings too")
    parser.add_argument("--format", choices=("text", "json", "github"), default="text",
                        help="output format; `github` prints GitHub Actions annotations")
    parser.add_argument("--export-schema", metavar="DIR",
                        help="write the JSON Schemas of sources and stream files to DIR for editor autocomplete, "
                             "then exit")
    return parser


def main(argv=None, source_check=None, prog="streamwright validate", non_strict_codes=()):
    """The `streamwright validate` command; it passes a source_check that also runs the connectors' checks.

    non_strict_codes: warning codes that --strict does NOT escalate (e.g. an environment missing a connector
    the config references is not a defect in the config).
    """
    parser = _parser(prog)
    args = parser.parse_args(argv)

    if args.export_schema:
        for path in export_schemas(args.export_schema):
            print(path)
        return 0

    configs_env = os.environ.get("STREAMWRIGHT_CONFIGS")
    paths = args.paths or ([configs_env] if configs_env else [])
    if not paths:
        parser.print_usage(sys.stderr)
        print("%s: error: give a PATH or set STREAMWRIGHT_CONFIGS" % prog, file=sys.stderr)
        return 2

    files, issues = validate_paths(paths, args.kind, args.allowed_connectors, source_check)
    errors = sum(1 for i in issues if i.severity == ERROR)
    warnings = len(issues) - errors

    if args.format == "json":
        print(json.dumps({"files": len(files), "errors": errors, "warnings": warnings,
                          "issues": [i.to_dict() for i in issues]}, indent=2))
    else:
        for issue in issues:
            print(issue.github_annotation() if args.format == "github" else issue)
        print("%d file(s) checked: %d error(s), %d warning(s)" % (len(files), errors, warnings))

    return 0 if is_valid(issues, strict=args.strict, non_strict_codes=non_strict_codes) else 1


if __name__ == "__main__":
    sys.exit(main())
