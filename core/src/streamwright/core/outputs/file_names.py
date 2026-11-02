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

"""The files of the file outputs: file-name templates (--file-name, `file_name`), export paths and their checks."""


import os

from streamwright.core.config.inputs import InputError
from streamwright.core.runtime.templates import TemplateError, expressions, render, to_text
from streamwright.core.validation.schema import split_outside_quotes

# what --file-name can use: {{ export }}, {{ source }}, {{ today }}, {{ timestamp }} and {{ config.NAME }}
FILE_NAME_SCOPES = ("export", "source", "today", "timestamp", "format")
STATE_FILE = "state.json"


def is_inside(directory, path):
    """Whether the file `path` (in `directory`) is inside it once symbolic links are followed."""
    root = os.path.normcase(os.path.realpath(directory))
    target = os.path.normcase(os.path.realpath(path))
    try:
        return target != root and os.path.commonpath([root, target]) == root
    except ValueError:  # (on another drive)
        return False


def _file_problem(path):
    """Why a rendered --file-name is not the path of a file inside the output directory, or None."""
    if not path.strip():
        return "is empty"
    if "\x00" in path:
        return "holds a NUL character"
    if os.path.isabs(path) or os.path.splitdrive(path)[0] or path.startswith(("/", "\\")):
        return "is not a relative path"
    parts = path.replace(os.sep, "/").replace(os.altsep or "/", "/").split("/")
    if ".." in parts:
        return "goes outside the output directory (..)"
    if parts[-1] in ("", "."):
        return "names a folder, not a file"
    normalized = os.path.normpath(path)
    first = normalized.replace(os.sep, "/").replace(os.altsep or "/", "/").split("/")[0]
    if first.lower() == STATE_FILE:  # (some file systems ignore case)
        return "is the state file's name" if first == normalized else \
            "needs the folder %r, the state file's name" % first
    return None


def export_files(templates, scopes, directory=None):
    """
    The file of each export that is named (export -> path inside the output directory): `templates` maps an export to
    its file-name template (the run's --file-name, else the export's `file_name`, else the source's
    `export.file_name`), rendered with {{ export }} and `scopes` (source, today, timestamp, format - the output's file
    extension - and config: values, never secrets). Raises ValueError, with every problem, for references it cannot
    render and for paths that are not inside the directory, that start with a folder named like the state file, that
    two exports share, that are (or need) folders that are files there, or that lead outside it through symbolic
    links.
    """
    config = scopes.get("config") or {}
    problems = [problem for template in dict.fromkeys(templates.values())
                for problem in _template_problems(template, config)]
    if problems:
        raise ValueError("file names: %s" % "; ".join(problems))
    paths, owners = _export_paths(templates, scopes, problems)
    for export, path in paths.items():
        problems.extend(_path_problems(export, path, owners, directory))
    if problems:
        raise ValueError("file names: %s" % "; ".join(problems))
    return paths


def config_references(template):
    """The config inputs a file-name template reads ({{ config.NAME }})."""
    names = []
    for expression in expressions(template):
        scope, _, attribute = split_outside_quotes(expression, "|")[0].strip().partition(".")
        if scope == "config" and attribute:
            names.append(attribute)
    return names


def reference_problems(template, declared_config):
    """The references of a file-name template that a file name cannot use (`declared_config`: the config inputs)."""
    problems = []
    for expression in expressions(template):
        scope, _, attribute = split_outside_quotes(expression, "|")[0].strip().partition(".")
        if scope == "config" and attribute:
            if attribute not in declared_config:
                problems.append("{{ %s }}: %r is not a config input (declared: %s)" % (
                    expression.strip(), attribute, ", ".join(sorted(declared_config)) or "none"))
        elif scope not in FILE_NAME_SCOPES or attribute:
            problems.append("{{ %s }}: a file name can use {{ export }}, {{ source }}, {{ today }}, {{ timestamp }}, "
                            "{{ format }} and {{ config.NAME }}" % expression.strip())
    return problems


def _template_problems(template, config):
    """The references of a file-name template that a file name cannot use, or whose config input has no value."""
    problems = reference_problems(template, config)
    for expression in expressions(template):
        scope, _, attribute = split_outside_quotes(expression, "|")[0].strip().partition(".")
        if scope == "config" and attribute in config and config[attribute] is None:
            problems.append("{{ %s }}: config %r has no value (give it with --set or --config)" % (
                expression.strip(), attribute))
    return problems


def _export_paths(templates, scopes, problems):
    """(export -> path, normalized path ignoring case -> export) of the rendered file names; appends to `problems`."""
    paths, owners = {}, {}
    for export, template in templates.items():
        try:
            value = render(template, dict(scopes, export=export))
        except (TemplateError, InputError, TypeError, ValueError) as exc:
            raise ValueError("file names: %s" % exc) from exc
        path = value if isinstance(value, str) else to_text(value)
        problem = _file_problem(path)
        if problem:
            problems.append("%r (export %r) %s" % (path, export, problem))
            continue
        paths[export] = os.path.normpath(path)
        key = os.path.normcase(paths[export]).lower()  # some file systems ignore case
        if key in owners:
            problems.append("exports %r and %r get the same file %r: use {{ export }}" % (
                owners[key], export, paths[export]))
        owners.setdefault(key, export)
    return paths, owners


def _path_problems(export, path, owners, directory):
    """
    Why an export's file cannot be written: its folders are other exports' files or files in `directory`, or it is a
    folder there or leads outside it through a symbolic link.
    """
    problems = []
    folder = os.path.dirname(path)
    while folder:
        if os.path.normcase(folder).lower() in owners:
            problems.append("%r is the file of export %r and a folder of export %r's file %r" % (
                folder, owners[os.path.normcase(folder).lower()], export, path))
        elif directory is not None and os.path.isfile(os.path.join(directory, folder)):
            problems.append("%r (export %r) needs the folder %r, a file in %s" % (path, export, folder, directory))
        folder = os.path.dirname(folder)
    if directory is not None and os.path.isdir(os.path.join(directory, path)):
        problems.append("%r (export %r) is a folder in %s" % (path, export, directory))
    elif directory is not None and not is_inside(directory, os.path.join(directory, path)):
        problems.append("%r (export %r) leads outside %s through a symbolic link" % (path, export, directory))
    return problems
