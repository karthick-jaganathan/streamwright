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

"""YAML loading with source locations: every mapping and list knows the file, line and column it came from."""



import yaml

from streamwright.core.validation.checker import ERROR, WARNING
from streamwright.core.validation.issues import Issue


class MarkedLoader(yaml.SafeLoader):
    """SafeLoader that remembers which YAML node every mapping/list came from."""

    def __init__(self, stream):
        super().__init__(stream)
        self.streamwright_nodes = {}          # id(constructed dict/list) -> yaml node
        self.streamwright_anchor_marks = {}   # anchor name -> mark of its definition
        self.streamwright_aliases = set()     # anchor names referenced by *alias or <<: *alias
        self.streamwright_findings = []       # (severity, code, mark, message)

    def compose_node(self, parent, index):
        event = self.peek_event()
        if isinstance(event, yaml.AliasEvent):
            self.streamwright_aliases.add(event.anchor)
            return super().compose_node(parent, index)
        node = super().compose_node(parent, index)
        if event.anchor is not None:
            self.streamwright_anchor_marks.setdefault(event.anchor, event.start_mark)
        return node

    def compose_mapping_node(self, anchor):
        node = super().compose_mapping_node(anchor)
        seen = set()
        for key_node, _ in node.value:
            if not isinstance(key_node, yaml.ScalarNode) or key_node.tag == "tag:yaml.org,2002:merge":
                continue
            if key_node.value in seen:
                self.streamwright_findings.append((
                    ERROR, "duplicate-key", key_node.start_mark,
                    "duplicate key %r: YAML keeps only the last value" % key_node.value))
            seen.add(key_node.value)
        return node

    def construct_yaml_map(self, node):
        data = {}
        self.streamwright_nodes[id(data)] = node
        yield data
        data.update(self.construct_mapping(node))

    def construct_yaml_seq(self, node):
        data = []
        self.streamwright_nodes[id(data)] = node
        yield data
        data.extend(self.construct_sequence(node))

    def construct_yaml_bool(self, node):
        value = super().construct_yaml_bool(node)
        if node.value.lower() in ("yes", "no", "on", "off"):
            self.streamwright_findings.append((
                WARNING, "yaml-boolean", node.start_mark,
                "%r is read as the boolean %s (YAML 1.1); write true/false, or quote it "
                "if you meant text" % (node.value, str(value).lower())))
        return value


MarkedLoader.add_constructor("tag:yaml.org,2002:map", MarkedLoader.construct_yaml_map)
MarkedLoader.add_constructor("tag:yaml.org,2002:seq", MarkedLoader.construct_yaml_seq)
MarkedLoader.add_constructor("tag:yaml.org,2002:bool", MarkedLoader.construct_yaml_bool)
# YAML 1.1 resolves a bare `=` to the "value" type; read it as text, like config_reader.load_yaml does
MarkedLoader.add_constructor("tag:yaml.org,2002:value", yaml.SafeLoader.construct_scalar)


class FolderLoaders:
    """The YAML nodes and findings of the files of a source folder, which are checked as one document."""

    def __init__(self, loaders):
        self.streamwright_nodes = {}
        self.streamwright_findings = []
        self.streamwright_anchor_marks = {}  # anchors belong to their file, so unused ones are found per file below
        self.streamwright_aliases = set()
        for loader in loaders:
            self.streamwright_nodes.update(loader.streamwright_nodes)
            self.streamwright_findings.extend(loader.streamwright_findings)
            for anchor, mark in loader.streamwright_anchor_marks.items():
                if anchor not in loader.streamwright_aliases:
                    self.streamwright_findings.append((WARNING, "unused-anchor", mark,
                                                "anchor &%s is defined but never used" % anchor))


def read_file(path):
    """(text, issues): the text is None when the file cannot be read."""
    try:
        with open(path, encoding="utf-8") as stream:
            return stream.read(), []
    except (OSError, UnicodeDecodeError) as exc:
        return None, [Issue(ERROR, "unreadable-file", str(exc), path)]


def parse_yaml(text, filename):
    """(loader, data, issues) for YAML text, read with source locations; the loader is None when it is not YAML."""
    try:
        loader = MarkedLoader(text)
    except yaml.YAMLError as exc:  # e.g. a control character
        return None, None, [Issue(ERROR, "yaml-syntax", str(exc), filename)]
    if filename:
        loader.name = filename  # every mark, and so every finding, knows its file
    try:
        return loader, loader.get_single_data(), []
    except yaml.MarkedYAMLError as exc:
        loader.dispose()
        mark = exc.problem_mark or exc.context_mark
        message = " ".join(str(p) for p in (exc.context, exc.problem) if p)
        line, column = (mark.line + 1, mark.column + 1) if mark is not None else (None, None)
        lines = text.splitlines()
        if line is not None and line <= len(lines) and "{{" in lines[line - 1]:
            message += "; quote template values, e.g. \"{{ config.x }}\""
        return None, None, [Issue(ERROR, "yaml-syntax", message, filename, "", line, column)]
    except yaml.YAMLError as exc:
        loader.dispose()
        return None, None, [Issue(ERROR, "yaml-syntax", str(exc), filename)]
