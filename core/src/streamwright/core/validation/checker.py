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
The base of the validation checks: severities, issue reporting with YAML locations (Checker), and the value helpers
the section checkers share.
"""

import copy
import datetime
import re

import yaml

from streamwright.core.config import grammar
from streamwright.core.runtime.issues import Issue
from streamwright.core.runtime.text import suggest
from streamwright.core.validation import vocabulary

__all__ = ["DURATION", "ERROR", "NON_STRICT_CODES", "WARNING", "Checker", "describe", "is_date", "render_path"]

ERROR = "error"
WARNING = "warning"
# the warnings that do not fail a --strict validation
NON_STRICT_CODES = ("connector-not-installed", "driver-not-installed")
DURATION = re.compile(vocabulary.DURATION_PATTERN)
_DATE = re.compile(r"^(today|-?[0-9]+d|[0-9]{4}-[0-9]{2}-[0-9]{2})$")


def is_date(value):
    # PyYAML loads an unquoted 2025-01-01 as a datetime.date
    return isinstance(value, datetime.date) or (isinstance(value, str) and bool(_DATE.match(value)))


def render_path(parts):
    out = ""
    for part in parts:
        if isinstance(part, int) and not isinstance(part, bool):
            out += "[%d]" % part
        elif isinstance(part, str) and part.isidentifier():
            out += ("." if out else "") + part
        else:
            out += "[%r]" % (part,)
    return out


def describe(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "a boolean"
    if isinstance(value, (int, float)):
        return "a number"
    if isinstance(value, str):
        return "text %r" % value if len(value) <= 40 else "text"
    if isinstance(value, dict):
        return "a mapping"
    if isinstance(value, list):
        return "a list"
    return type(value).__name__


class Checker:
    """Reporting and generic checks; the checks of `kind: source` are in validation.source.SourceChecker."""

    def __init__(self, filename, loader, options):
        self.file = filename
        self.nodes = loader.streamwright_nodes
        self.loader = loader
        self.options = options
        self.issues = []
        # source folders: the files read, and the file of each stream (index in `streams` -> path)
        self.folder = None
        self.files = frozenset()
        self.stream_files = {}
        # the source as written (its public grammar) and the document checked (translated in place, see `translate`)
        self.public = None
        self.root = None
        self.refused = frozenset()    # the paths the grammar reported: no other key issue at or under them
        self.halted = frozenset()     # operations that mix a call's and a request's keys: no other issue under them

    def translate(self, document):
        """Reports the grammar problems of a public source and translates it into the shape the checks read."""
        if not isinstance(document, dict):
            return
        problems = grammar.problems(document)
        for code, path, container, key, message in problems:
            self.error(code, path, message, container, key)
        self.refused = frozenset(path for _, path, _, _, _ in problems)
        self.halted = frozenset(path for code, path, _, _, _ in problems
                                if code == "bad-value" and path and path[-1] == "operation")
        self.public, self.root = copy.deepcopy(document), document
        grammar.to_internal(document)

    # reporting -------------------------------------------------------------

    @staticmethod
    def _key_mark(node, key):
        if isinstance(node, yaml.MappingNode) and key is not None:
            for key_node, _ in reversed(node.value):
                if isinstance(key_node, yaml.ScalarNode) and key_node.value == str(key):
                    return key_node.start_mark
        if isinstance(node, yaml.SequenceNode) and isinstance(key, int) and 0 <= key < len(node.value):
            return node.value[key].start_mark
        return None

    def _walk(self, path):
        """The mark of `path` (public) in the source as written, or None."""
        node = self.nodes.get(id(self.root)) if self.root is not None else None
        mark = None
        for position, key in enumerate(path):
            child = None
            if isinstance(node, yaml.MappingNode):
                child = next((value for key_node, value in reversed(node.value)
                              if isinstance(key_node, yaml.ScalarNode) and key_node.value == str(key)), None)
                if child is None and position == 0 and isinstance(self.root.get(key), list):
                    child = self.nodes.get(id(self.root[key]))       # a source folder's streams
            mark = self._key_mark(node, key) or mark
            if isinstance(node, yaml.SequenceNode) and isinstance(key, int) and 0 <= key < len(node.value):
                child = node.value[key]
            if child is None:
                return mark if position == len(path) - 1 else None
            node = child
        return mark

    def _mark(self, container, key, path=()):
        node = self.nodes.get(id(container)) if container is not None else None
        exact = self._key_mark(node, key)
        if exact is not None:
            return exact
        # a key the translation of the grammar moved (or a mapping it made): find it where it is written
        walked = self._walk(path) if key is not None or node is None else None
        if walked is not None:
            return walked
        return node.start_mark if node is not None else None

    def _add(self, severity, code, path, message, *, container=None, key=None, mark=None, file=None):
        path = tuple(path)
        if self.public is not None:
            path = grammar.public_path(path, self.public)
            if code == "unknown-key" and any(path[:len(refused)] == refused for refused in self.refused):
                return
            if any(path[:len(halted)] == halted for halted in self.halted):
                return
        mark = mark or self._mark(container, key, path)
        if file is None:
            file = self.file
            part_files = {"streams": self.stream_files}.get(path[0] if path else None)
            if len(path) > 1 and part_files and path[1] in part_files:
                # a stream of a source folder: report it in its file, with the path inside that file
                file, path = part_files[path[1]], path[2:]
            if mark is not None and getattr(mark, "name", None) in self.files:
                file = mark.name
        line, column = (mark.line + 1, mark.column + 1) if mark is not None else (None, None)
        self.issues.append(Issue(severity, code, message, file, render_path(path), line, column))

    def error(self, code, path, message, container=None, key=None):
        self._add(ERROR, code, path, message, container=container, key=key)

    def warn(self, code, path, message, container=None, key=None):
        self._add(WARNING, code, path, message, container=container, key=key)

    # generic value checks ----------------------------------------------------

    def _expect(self, ok, expected, value, path, *, container, key):
        if not ok:
            self.error("bad-value", path, "expected %s, got %s" % (expected, describe(value)), container, key)
        return ok

    def require_keys(self, mapping, keys, path, what):
        for key in keys:
            if key not in mapping:
                self.error("missing-key", path, "%s needs `%s`" % (what, key), mapping)

    def unknown_keys(self, mapping, allowed, path, severity, consequence):
        for key in mapping:
            if key not in allowed:
                self._add(severity, "unknown-key", (*path, key),
                          "unknown key %r%s; %s" % (key, suggest(key, allowed), consequence), container=mapping,
                          key=key)

    # documents ---------------------------------------------------------------

    def check_preamble(self, data):
        """The YAML findings and the document's shape; returns False when the document cannot be checked further."""
        for severity, code, mark, message in self.loader.streamwright_findings:
            self._add(severity, code, (), message, mark=mark)
        for anchor, mark in sorted(self.loader.streamwright_anchor_marks.items(), key=lambda kv: kv[1].line):
            if anchor not in self.loader.streamwright_aliases:
                self._add(WARNING, "unused-anchor", (), "anchor &%s is defined but never used" % anchor, mark=mark)

        if data is None:
            self.error("empty-document", (), "the file is empty")
            return False
        if not isinstance(data, dict):
            self.error("bad-value", (), "a configuration must be a mapping, got %s" % describe(data), data)
            return False
        return True
