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

"""Reference scopes and `{{ template }}` checks."""

import re

from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.checker import suggest
from streamwright.core.validation.sections.document import DocumentSection
from streamwright.core.validation.sections.templates import REFERENCE_HEAD, TEMPLATE

_FILTER = re.compile(r"^([A-Za-z_]\w*)\s*(?:\((.*)\))?$", re.S)
_LITERAL = re.compile(r"""^('[^']*'|"[^"]*"|-?[0-9]+(\.[0-9]+)?|true|false|null)$""")


class ReferenceSection(DocumentSection):
    """Reference and template checks on the shared issue sink."""

    def scopes(self, stream=None, *extra):
        """Scopes available in stream requests and field values: config, today, partition, window, + extra."""
        available = {"config": set(self.inputs["config"]), "today": ()}
        if stream is not None and stream.partitions:
            available["partition"] = set(stream.partitions)
        if stream is not None and stream.incremental:
            available["window"] = set(spec.WINDOW_ATTRIBUTES)
        for scope in extra:
            available[scope] = None
        return available

    def check_templates(self, value, path, *, container, key, scopes, stream=None):
        """Checks every {{ reference }} in value (recursively) against the available scopes."""
        if isinstance(value, dict):
            for k, v in value.items():
                if isinstance(k, str) and ("{{" in k or "}}" in k):
                    self.error("template-syntax", (*path, k), "references are not allowed in mapping keys", value, k)
                self.check_templates(v, (*path, k), container=value, key=k, scopes=scopes, stream=stream)
        elif isinstance(value, list):
            for i, v in enumerate(value):
                self.check_templates(v, (*path, i), container=value, key=i, scopes=scopes, stream=stream)
        elif isinstance(value, str) and ("{{" in value or "}}" in value):
            if "{{" in TEMPLATE.sub("", value):
                self.error("template-syntax", path, "unclosed `{{` in %r" % value, container, key)
            for inner in TEMPLATE.findall(value):
                self.check_reference(inner, path, container=container, key=key, scopes=scopes, stream=stream)

    def check_reference(self, inner, path, *, container, key, scopes, stream):
        parts = spec.split_outside_quotes(inner, "|")
        match = REFERENCE_HEAD.match(parts[0])
        shown = "{{ %s }}" % inner.strip()
        if not match:
            self.error("template-syntax", path, "invalid reference %s" % shown, container, key)
            return
        scope, attribute = match.group(1), match.group(2)
        if scope not in spec.SCOPES:
            self.error("unknown-reference", path, "unknown scope %r in %s%s; scopes: %s" % (
                scope, shown, suggest(scope, spec.SCOPES), ", ".join(spec.SCOPES)), container, key)
            return
        if scope not in scopes:
            if scope == "secrets":
                self.error("secret-outside-auth", path,
                           "%s: secrets can only be used inside `auth` or `http.headers`" % shown, container, key)
            elif scope == "window" and stream is not None:
                self.error("unavailable-reference", path, "%s needs `incremental` on stream %r" % (
                    shown, stream.name), container, key)
            elif scope == "partition" and stream is not None:
                self.error("unavailable-reference", path, "%s: stream %r has no partitions" % (
                    shown, stream.name), container, key)
            else:
                self.error("unavailable-reference", path, "%s cannot be used here%s" % (
                    shown, "; available: " + ", ".join(sorted(scopes)) if scopes else ""), container, key)
            return
        allowed = scopes[scope]
        if scope == "today":
            if attribute:
                self.error("template-syntax", path, "%s: `today` has no attributes" % shown, container, key)
        elif not attribute:
            self.error("template-syntax", path, "%s needs a name, e.g. {{ %s.x }}" % (shown, scope), container, key)
        elif allowed is not None:
            name = attribute.split(".")[0]
            if "." in attribute:
                self.error("template-syntax", path, "%s: %s values have no attributes" % (shown, scope),
                           container, key)
            elif name not in allowed:
                where = {"config": "spec.config", "secrets": "spec.secrets"}.get(scope, scope)
                self.error("unknown-reference", path, "%s: %r is not declared in %s%s" % (
                    shown, name, where, suggest(name, allowed)), container, key)
            if scope in spec.INPUT_SECTIONS:
                self.used_inputs.add((scope, name))
        for text in parts[1:]:
            self.check_filter(text, shown, path, container, key)

    def check_filter(self, text, shown, path, container, key):
        match = _FILTER.match(text)
        if not match:
            self.error("template-syntax", path, "invalid filter %r in %s" % (text, shown), container, key)
            return
        name, arguments = match.group(1), match.group(2)
        if name not in spec.FILTERS:
            self.error("unknown-filter", path, "unknown filter %r in %s%s; filters: %s" % (
                name, shown, suggest(name, spec.FILTERS), ", ".join(spec.FILTERS)), container, key)
            return
        values = ([value for value in spec.split_outside_quotes(arguments, ",") if value]
                  if arguments is not None else [])
        if len(values) != spec.FILTERS[name]:
            self.error("bad-value", path, "filter %r takes %d argument(s), got %d in %s" % (
                name, spec.FILTERS[name], len(values), shown), container, key)
        for value in values:
            if not _LITERAL.match(value):
                self.error("template-syntax", path, "filter arguments must be quoted text or numbers, got %r in %s "
                           "(references are not allowed inside filters)" % (value, shown), container, key)
