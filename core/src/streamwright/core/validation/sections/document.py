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

"""Document preamble, shared state and the generic value checks of `kind: source`."""

import os
import re

from streamwright.core.config import loader
from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.checker import DURATION, ERROR, Checker, describe, suggest
from streamwright.core.validation.sections.stream_context import NAME, transform_steps


class DocumentSection(Checker):

    def __init__(self, filename, loader, options):
        super().__init__(filename, loader, options)
        self.inputs = {"config": {}, "secrets": {}}
        self.used_inputs = set()
        self.auth_provider = None
        self.auth_type = None
        self.auth_transport = None
        self.base_url = None
        self.http_paginator = None
        self.params_encoding = "plain"
        self.streams = []        # the source's streams
        self.stream_names = {}   # lowercase stream name -> index of the first stream with it
        self.export_owners = {}  # lowercase export name -> (export name, index of its stream)

    # document ----------------------------------------------------------------

    def check_document(self, data):
        if not self.check_preamble(data):
            return
        if "version" in data and str(data["version"]) not in spec.SUPPORTED_VERSIONS:
            self.error("unsupported-version", ("version",), "`kind: source` has no version %r; omit `version` (it "
                       "defaults to the current format)" % (data["version"],), data, "version")
            return
        # `kind` may come from --kind for files that do not declare it
        required = [key for key in spec.REQUIRED_TOP_LEVEL_KEYS if key != "kind"]
        if self.folder is not None and "streams" not in data:
            required.remove("streams")
            self.error("missing-key", (), "a source folder needs a file for each stream in %s/" % os.path.join(
                self.folder, loader.STREAMS_FOLDER), data)
        self.require_keys(data, required, (), "a source")
        for key in data:
            if key not in spec.TOP_LEVEL_KEYS and not str(key).startswith(spec.EXTENSION_PREFIX):
                if key == "models":
                    self.error("removed-key", (key,), loader.MODELS_REMOVED_HINT, data, key)
                else:
                    self.error("unknown-key", (key,), "unknown top-level key %r%s; use an `%s` prefix for custom keys "
                               "(e.g. to hold YAML anchors)" % (key, suggest(key, spec.TOP_LEVEL_KEYS),
                                                               spec.EXTENSION_PREFIX), data, key)
        if "name" in data:
            self.check_name(data["name"], ("name",), data, "name")
        if "description" in data:
            self._expect(isinstance(data["description"], str), "text", data["description"], ("description",),
                         container=data, key="description")
        self.check_spec(data)
        self.check_auth(data)
        self.check_http_defaults(data)
        self.check_export_defaults(data)
        self.check_streams(data)
        streams = data.get("streams") if isinstance(data.get("streams"), list) else []
        queries = " ".join(step["select"] for stream in streams if isinstance(stream, dict)
                           for step in transform_steps(stream) or () if isinstance(step, dict)
                           and isinstance(step.get("select"), str))
        for section in spec.INPUT_SECTIONS:
            inputs = self.inputs[section]
            for name in inputs:
                if section == "config" and re.search(r"\b%s\b" % re.escape(name), queries):
                    continue  # read in a step's `select`: $name, or config->>'name'
                if (section, name) not in self.used_inputs:
                    self.warn("unused-input", ("spec", section, name), "%s input %r is declared but never used" % (
                        section, name), self.spec_section(data, section), name)
        if self.options.source_check is not None and not any(issue.severity == ERROR for issue in self.issues):
            self.options.source_check(self, data)  # e.g. the checks of installed connectors and query builders

    def spec_section(self, data, section):
        spec_block = data.get("spec")
        return spec_block.get(section) if isinstance(spec_block, dict) else None

    def check_name(self, value, path, container, key):
        if self._expect(isinstance(value, str), "a name", value, path, container=container,
                        key=key) and not NAME.match(value):
            self.error("bad-value", path, "%r is not a valid name (letters, digits and _; not starting with a "
                       "digit)" % value, container, key)

    def check_duration(self, value, path, container, key):
        if not (isinstance(value, str) and DURATION.match(value)):
            self.error("bad-value", path, "expected a duration such as 15s, 30m, 1h or 7d, got %s" % describe(value),
                       container, key)

    def check_choice(self, value, choices, path, *, container, key, what):
        if isinstance(value, str) and value in choices:
            return True
        self.error("bad-value", path, "unknown %s %r%s; one of: %s" % (
            what, value, suggest(value, choices), ", ".join(str(c) for c in choices)), container, key)
        return False

    def check_strings(self, mapping, keys, path, expected="text"):
        for key in keys:
            if key in mapping:
                self._expect(isinstance(mapping[key], str), expected, mapping[key], (*path, key), container=mapping,
                             key=key)

    def check_mapping(self, value, allowed, path, *, container, key, what, required=()):
        """Expects a mapping with only `allowed` keys; returns it (or None)."""
        if not self._expect(isinstance(value, dict), "a mapping", value, path, container=container, key=key):
            return None
        self.require_keys(value, required, path, what)
        self.unknown_keys(value, allowed, path, ERROR, "%s does not support it" % what)
        return value
