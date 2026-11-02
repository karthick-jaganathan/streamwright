"""
The base of the validation checks: severities, issue reporting with YAML locations (Checker), and the value helpers
the section checkers share.
"""

import datetime
import difflib
import re

import yaml

from streamwright.core.validation import vocabulary
from streamwright.core.validation.issues import Issue

__all__ = ["DURATION", "ERROR", "NON_STRICT_CODES", "WARNING", "Checker", "describe", "is_date", "render_path",
           "suggest"]

ERROR = "error"
WARNING = "warning"
# the warnings that do not fail a --strict validation
NON_STRICT_CODES = ("connector-not-installed",)
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


def suggest(word, candidates):
    if not isinstance(word, str):
        return ""
    match = difflib.get_close_matches(word, [c for c in candidates if isinstance(c, str)], n=1, cutoff=0.6)
    return " (did you mean %r?)" % match[0] if match else ""


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

    # reporting -------------------------------------------------------------

    def _mark(self, container, key):
        node = self.nodes.get(id(container)) if container is not None else None
        if node is None:
            return None
        if isinstance(node, yaml.MappingNode) and key is not None:
            for key_node, _ in reversed(node.value):
                if isinstance(key_node, yaml.ScalarNode) and key_node.value == str(key):
                    return key_node.start_mark
        if isinstance(node, yaml.SequenceNode) and isinstance(key, int) and 0 <= key < len(node.value):
            return node.value[key].start_mark
        return node.start_mark

    def _add(self, severity, code, path, message, *, container=None, key=None, mark=None, file=None):
        mark = mark or self._mark(container, key)
        path = tuple(path)
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
