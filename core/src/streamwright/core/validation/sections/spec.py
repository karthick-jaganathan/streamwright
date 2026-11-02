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

"""The `spec` section: inputs, authentication, connector and HTTP defaults."""


from streamwright.core.outputs.file_names import config_references, reference_problems
from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.auth import AuthChecker
from streamwright.core.validation.checker import is_date
from streamwright.core.validation.sections.references import ReferenceSection


class SpecSection(ReferenceSection):
    """Input, authentication and HTTP defaults checks."""

    def check_spec(self, data):
        if "spec" not in data:
            return
        block = self.check_mapping(data["spec"], spec.INPUT_SECTIONS, ("spec",), container=data, key="spec",
                                   what="`spec`")
        if block is None:
            return
        for section in spec.INPUT_SECTIONS:
            if section not in block or block[section] is None:
                continue
            inputs = block[section]
            spath = ("spec", section)
            if not self._expect(isinstance(inputs, dict), "a mapping of input names", inputs, spath, container=block,
                                key=section):
                continue
            for name, definition in inputs.items():
                self.check_name(name, (*spath, name), inputs, name)
                self.check_input(section, name, definition, (*spath, name), inputs)
                self.inputs[section][name] = definition

    def check_input(self, section, name, definition, path, container):
        what = "%s input %r" % (section, name)
        definition = self.check_mapping(definition, spec.INPUT_KEYS, path, container=container, key=name, what=what,
                                        required=("type",))
        if definition is None:
            return
        self.check_templates(definition, path, container=container, key=name, scopes={})
        input_type = definition.get("type")
        if "type" in definition and not self.check_choice(input_type, spec.INPUT_TYPES, (*path, "type"),
                                                          container=definition, key="type", what="input type"):
            return
        if input_type == "list":
            if "items" not in definition:
                self.error("missing-key", path, "%s is a list and needs `items` (one of: %s)" % (
                    what, ", ".join(spec.LIST_ITEM_TYPES)), container, name)
            else:
                self.check_choice(definition["items"], spec.LIST_ITEM_TYPES, (*path, "items"), container=definition,
                                  key="items", what="item type")
        elif "items" in definition:
            self.error("bad-value", (*path, "items"), "`items` only applies to `type: list`", definition, "items")
        for flag in ("required",):
            if flag in definition:
                self._expect(isinstance(definition[flag], bool), "true or false", definition[flag],
                             (*path, flag), container=definition, key=flag)
        if "description" in definition:
            self._expect(isinstance(definition["description"], str), "text", definition["description"],
                         (*path, "description"), container=definition, key="description")
        if "default" in definition:
            if section == "secrets":
                self.warn("secret-default", (*path, "default"), "secrets should not have defaults in config "
                          "files; pass them at run time", definition, "default")
            self.check_default(input_type, definition["default"], (*path, "default"), definition)

    def check_default(self, input_type, value, path, container):
        checks = {
            "string": (lambda v: isinstance(v, str), "text"),
            "integer": (lambda v: isinstance(v, int) and not isinstance(v, bool), "a whole number"),
            "number": (lambda v: isinstance(v, (int, float)) and not isinstance(v, bool), "a number"),
            "boolean": (lambda v: isinstance(v, bool), "true or false"),
            "date": (is_date, "a date: YYYY-MM-DD, today or -30d"),
            "list": (lambda v: isinstance(v, list), "a list"),
        }
        if input_type in checks:
            ok, expected = checks[input_type]
            self._expect(value is None or ok(value), expected, value, path, container=container, key="default")

    def check_auth(self, data):
        AuthChecker(self).check(data)

    def check_connector(self, name, path, container, key):
        allowed = self.options.allowed_connectors
        if allowed is not None and name not in allowed:
            self.error("connector-not-allowed", path, "connector %r is not in the allowed list (%s)" % (
                name, ", ".join(allowed) or "empty"), container, key)

    def check_export_defaults(self, data):
        """source.yaml `export`: defaults for every export of the source (`file_name`)."""
        if "export" not in data:
            return
        export = self.check_mapping(data["export"], spec.SOURCE_EXPORT_KEYS, ("export",), container=data,
                                    key="export", what="`export`")
        if export is not None and "file_name" in export:
            self.check_file_name(export["file_name"], ("export", "file_name"), container=export)

    def check_file_name(self, value, path, *, container):
        """A file-name template: the path of an export's file inside the file output's directory."""
        if not self._expect(isinstance(value, str) and value.strip(), "a file-name template, e.g. "
                            "\"{{ export }}/{{ today }}.csv\"", value, path, container=container, key="file_name"):
            return
        declared = self.inputs["config"]
        for problem in reference_problems(value, declared):
            self.error("bad-reference", path, problem, container, "file_name")
        self.used_inputs.update(("config", name) for name in config_references(value) if name in declared)

    def check_http_defaults(self, data):
        if "http" not in data:
            return
        http = self.check_mapping(data["http"], spec.HTTP_KEYS, ("http",), container=data, key="http", what="`http`")
        if http is None:
            return
        path = ("http",)
        if "base_url" in http:
            url = http["base_url"]
            if self._expect(isinstance(url, str), "a URL", url, (*path, "base_url"), container=http, key="base_url"):
                if url.startswith("http://"):
                    self.warn("insecure-url", (*path, "base_url"), "use https:// for API requests", http, "base_url")
                elif not url.startswith(("https://", "{{")):
                    self.error("bad-value", (*path, "base_url"), "expected an http(s):// URL, got %r" % url,
                               http, "base_url")
                self.base_url = url
        if "headers" in http:
            self.check_headers(http["headers"], (*path, "headers"), http)
            headers_scopes = dict(self.scopes(), secrets=set(self.inputs["secrets"]))
            self.check_templates(http["headers"], (*path, "headers"), container=http, key="headers",
                                 scopes=headers_scopes)
        if "paginator" in http:
            self.http_paginator = http["paginator"]
            self.check_paginator(http["paginator"], (*path, "paginator"), http, None)
        if "records" in http:
            records = self.check_mapping(http["records"], spec.RECORDS_KEYS, (*path, "records"), container=http,
                                         key="records", what="`records`")
            if records is not None:
                for key in spec.RECORDS_KEYS:
                    if key in records:
                        self._expect(isinstance(records[key], str), "a dotted path", records[key],
                                     (*path, "records", key), container=records, key=key)
                self.check_templates(records, (*path, "records"), container=http, key="records", scopes={})
        if "params_encoding" in http:
            self.check_choice(http["params_encoding"], spec.PARAMS_ENCODING_TYPES,
                              (*path, "params_encoding"), container=http, key="params_encoding", what="encoding")
            self.params_encoding = http["params_encoding"]
        self.check_limits(http, path)
        non_header_http = {k: v for k, v in http.items() if k not in ("headers", "paginator", "records")}
        self.check_templates(non_header_http, path, container=data, key="http", scopes=self.scopes())

    def check_headers(self, headers, path, container):
        if self._expect(isinstance(headers, dict), "a mapping of header names to values", headers, path,
                        container=container, key="headers"):
            for name, value in headers.items():
                self._expect(isinstance(value, str), "text (quote numbers, e.g. \"2.0.0\")", value, (*path, name),
                             container=headers, key=name)

    def check_limits(self, block, path):
        if "rate_limit" in block:
            limit = self.check_mapping(block["rate_limit"], spec.RATE_LIMIT_KEYS, (*path, "rate_limit"),
                                       container=block,
                                       key="rate_limit", what="`rate_limit`", required=spec.RATE_LIMIT_KEYS)
            if limit is not None:
                if "requests" in limit:
                    self._expect(isinstance(limit["requests"], int) and not isinstance(limit["requests"], bool)
                                 and limit["requests"] > 0, "a positive whole number", limit["requests"],
                                 (*path, "rate_limit", "requests"), container=limit, key="requests")
                if "per" in limit:
                    self.check_duration(limit["per"], (*path, "rate_limit", "per"), limit, "per")
        if "retry" in block:
            retry = self.check_mapping(block["retry"], spec.RETRY_KEYS, (*path, "retry"), container=block, key="retry",
                                       what="`retry`")
            if retry is not None:
                rpath = (*path, "retry")
                if "codes" in retry and self._expect(isinstance(retry["codes"], list), "a list of status or "
                                                     "error codes", retry["codes"], (*rpath, "codes"), container=retry,
                                                     key="codes"):
                    for i, code in enumerate(retry["codes"]):
                        self._expect(isinstance(code, (int, str)) and not isinstance(code, bool), "a status or "
                                     "error code", code, (*rpath, "codes", i), container=retry["codes"], key=i)
                if "max_attempts" in retry:
                    attempts = retry["max_attempts"]
                    self._expect(isinstance(attempts, int) and not isinstance(attempts, bool) and attempts >= 1,
                                 "a whole number >= 1", attempts, (*rpath, "max_attempts"), container=retry,
                                 key="max_attempts")
                if "backoff" in retry:
                    self.check_choice(retry["backoff"], spec.BACKOFF_TYPES, (*rpath, "backoff"), container=retry,
                                      key="backoff", what="backoff")
                if "max_delay" in retry:
                    self.check_duration(retry["max_delay"], (*rpath, "max_delay"), retry, "max_delay")
