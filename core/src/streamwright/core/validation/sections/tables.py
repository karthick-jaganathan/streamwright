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

"""A stream's tables: its requests and `transform` steps, and its exports."""


from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.checker import ERROR, describe, suggest
from streamwright.core.validation.sections.stream_context import transform_steps
from streamwright.core.validation.sections.streams import StreamsSection
from streamwright.core.validation.sections.templates import TEMPLATE


class TableSection(StreamsSection):
    """Request/step table namespaces, transformations and exports."""

    def collect_tables(self, stream, path, info):
        """The names of the stream's requests and steps: its tables, one namespace (names ignore case)."""
        requests = stream.get("requests") if isinstance(stream.get("requests"), list) else []
        steps = transform_steps(stream)
        info.steps_known = bool(steps)
        entries = [("request", j, item, (*path, "requests", j, "name")) for j, item in enumerate(requests)]
        entries += [("step", j, item, (*path, "transform", "steps", j, "name")) for j, item in enumerate(steps or ())]
        for kind, index, item, npath in entries:
            if not (isinstance(item, dict) and isinstance(item.get("name"), str)):
                continue
            name = item["name"]
            previous = info.tables.get(name.lower())
            if previous is None:
                info.tables[name.lower()] = (kind, index, name)
                continue
            suffix = "" if previous[2] == name else " (names ignore case)"
            if previous[0] == kind:
                message = "%s name %r is used twice%s" % (kind, name, suffix)
            else:
                message = "step %r has the name of request %r%s: a stream's requests and steps are its tables, " \
                          "so their names are unique" % (name, previous[2], suffix)
            self.error("duplicate-%s" % kind, npath, message, item, "name")

    def check_transform(self, transform, path, stream):
        if isinstance(transform, list):
            self.error("bad-value", path, "`transform` is a mapping {mode: page or run, steps: [...]}: move these "
                       "steps to `transform.steps`", stream, "transform")
            return
        transform = self.check_mapping(transform, spec.TRANSFORM_KEYS, path, container=stream, key="transform",
                                       what="`transform`", required=spec.TRANSFORM_KEYS)
        if transform is None:
            return
        if "mode" in transform:
            self.check_choice(transform["mode"], spec.TRANSFORM_MODES, (*path, "mode"), container=transform, key="mode",
                              what="transform mode")
        if "steps" not in transform:
            return
        steps = transform["steps"]
        if not self._expect(isinstance(steps, list) and len(steps) > 0, "a non-empty list of steps", steps,
                            (*path, "steps"), container=transform, key="steps"):
            return
        for j, step in enumerate(steps):
            spath = (*path, "steps", j)
            if not self._expect(isinstance(step, dict), "a step mapping {name, select}", step, spath, container=steps,
                                key=j):
                continue
            self.unknown_keys(step, spec.STEP_KEYS, spath, ERROR, "steps do not support it")
            self.require_keys(step, ("name", "select"), spath, "a step")
            if "name" in step:
                self.check_name(step["name"], (*spath, "name"), step, "name")
            if "select" in step:
                query = step["select"]
                if not (isinstance(query, str) and query.strip()):
                    self.error("bad-value", (*spath, "select"), "`select` must be a SQL query, got %s" %
                               describe(query), step, "select")
                elif TEMPLATE.search(query):  # as the template engine finds them: `}}` alone ends a struct literal
                    self.error("bad-value", (*spath, "select"), "`select` is SQL: read config values as `$name` "
                               "parameters (or the columns of the request tables: %s), not with `{{ ... }}` "
                               "references" % ", ".join(spec.REQUEST_COLUMNS), step, "select")
            if "description" in step:
                self._expect(isinstance(step["description"], str), "text", step["description"],
                             (*spath, "description"), container=step, key="description")

    def check_export(self, export, path, stream, info):
        if export is None or export == {}:
            self.error("bad-value", path, "a stream needs at least one export", stream, "export")
            return
        if not self._expect(isinstance(export, dict), "a mapping of export names to {step, primary_key?}", export,
                            path, container=stream, key="export"):
            return
        steps = sorted(name for kind, _, name in info.tables.values() if kind == "step")
        for name, definition in export.items():
            epath = (*path, name)
            self.check_name(name, epath, export, name)
            if not self._expect(isinstance(definition, dict), "an export mapping {step, primary_key?}", definition,
                                epath, container=export, key=name):
                continue
            self.unknown_keys(definition, spec.EXPORT_KEYS, epath, ERROR, "exports do not support it")
            self.require_keys(definition, ("step",), epath, "an export")
            if "file_name" in definition:
                self.check_file_name(definition["file_name"], (*epath, "file_name"), container=definition)
            step = definition.get("step")
            if "step" in definition and self._expect(isinstance(step, str), "a step name", step, (*epath, "step"),
                                                     container=definition, key="step") and info.steps_known:
                table = info.tables.get(step.lower())
                if table is None:
                    self.error("unknown-step", (*epath, "step"), "unknown step %r%s" % (step, suggest(step, steps)),
                               definition, "step")
                elif table[0] == "request":
                    self.error("bad-value", (*epath, "step"), "%r is a request: an export writes a step's rows (for "
                               "the records as returned: a step `SELECT record FROM %s`)" % (step, table[2]),
                               definition, "step")
            if "primary_key" in definition:
                keys = definition["primary_key"]
                if self._expect(isinstance(keys, list) and len(keys) > 0, "a non-empty list of column names", keys,
                                (*epath, "primary_key"), container=definition, key="primary_key"):
                    for k, key in enumerate(keys):
                        self._expect(isinstance(key, str), "a column name", key, (*epath, "primary_key", k),
                                     container=keys, key=k)
            if "description" in definition:
                self._expect(isinstance(definition["description"], str), "text", definition["description"],
                             (*epath, "description"), container=definition, key="description")
