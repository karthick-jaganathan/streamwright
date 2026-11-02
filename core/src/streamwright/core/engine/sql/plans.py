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

"""A source's plan: each stream's requests, steps and exports, checked before any request."""

from streamwright.core.engine.sql.compilation import StreamCompilation
from streamwright.core.engine.sql.ordering import RunOrdering
from streamwright.core.engine.sql.sandbox import Sandbox
from streamwright.core.engine.sql.stream_plan import (
    StreamPlan,
    exports_of,
    incremental_exports,
    is_table_name,
    partition_fields,
)
from streamwright.core.engine.sql.tables import MODES, _path_text, param_type

__all__ = [
    "SourcePlan",
    "StreamPlan",
    "check_source",
    "exports_of",
    "incremental_exports",
    "partition_fields",
    "plan_source",
]


class SourcePlan(StreamCompilation, RunOrdering):
    """
    A source's streams, checked with DuckDB before any request (streamwright validate and streamwright run): each
    stream's steps compiled in list order against empty tables of its requests and earlier steps, with `$name`
    parameters bound to typed placeholders; request and step names that DuckDB has; export keys and `cursor_field`; the
    sources and fields of request partitions; the order of a run-mode stream's requests and steps; `from_stream`
    parents, their one export and its columns; and cycles within streams and across `from_stream`.

    - streams[name]: its StreamPlan; export_owner[export]: its stream; order: the stream names, parents first
    - types[name]: each config input's DuckDB type as a `$name` parameter (None: it cannot be one)
    - problems: (path in the source, message), with paths such as ("streams", 2, "transform", "steps", 0, "select"),
      so streamwright validate reports file and line
    """

    def __init__(self, sandbox, source):
        spec = source.get("spec") if isinstance(source.get("spec"), dict) else {}
        config = spec.get("config") if isinstance(spec.get("config"), dict) else {}
        self.types = {name: param_type(definition) for name, definition in config.items()}
        self.streams, self.names, self.export_owner, self.problems = {}, {}, {}, []
        for index, stream in enumerate(source.get("streams") or []):
            if isinstance(stream, dict) and is_table_name(stream.get("name")) and \
                    stream["name"].lower() not in self.names:
                # names ignore case, as `streamwright validate` does
                self.names[stream["name"].lower()] = stream["name"]
                self.streams[stream["name"]] = StreamPlan(stream["name"], index, stream)
        self._check_export_names()
        for plan in self.streams.values():
            self._compile(sandbox, plan)
        self._parents()
        self.order = self._order()

    def _problem(self, path, message):
        self.problems.append((path, message))

    # selection ---------------------------------------------------------------------

    def closure(self, names):
        """The streams that run for these stream or export names: them and their `from_stream` parents, in order."""
        needed = set()

        def visit(name):
            name = self.export_owner.get(name, name)
            if name in needed or name not in self.streams:
                return
            needed.add(name)
            for _, _, parent in self.streams[name].parents:
                visit(parent)
        for name in names:
            visit(name)
        return [name for name in self.order if name in needed]

    def messages(self, streams=None):
        """The problems as text, "stream 'x': transform.steps[0].select: ..." (only those of `streams`, if given)."""
        names = {plan.index: plan.name for plan in self.streams.values() if streams is None or
                     plan.name in streams}
        found = []
        for path, message in self.problems:
            if path[1] in names:
                where = _path_text(path[2:])
                found.append("stream %r: %s%s" % (names[path[1]], where + ": " if where else "", message))
        return found

    # checks --------------------------------------------------------------------------

    def _check_export_names(self):
        """Every stream has exports; export names are unique in the source and are not another stream's name."""
        taken = {}
        for plan in self.streams.values():
            path = ("streams", plan.index, "export")
            if not isinstance(plan.stream.get("export"), dict) or not plan.stream["export"]:
                self._problem(path, "a stream needs at least one export: `export: {NAME: {step: STEP}}`")
                continue
            for name in exports_of(plan.stream):
                lower = name.lower()
                if lower in taken:
                    self._problem((*path, name), "export %r is also an export of stream %r: export names are unique "
                                                  "in a source (names ignore case)" % (name, taken[lower]))
                elif lower in self.names and self.names[lower] != plan.name:
                    self._problem((*path, name), "export %r has the name of stream %r: an export can have its own "
                                                  "stream's name, not another's" % (name, self.names[lower]))
                else:
                    taken[lower] = plan.name
                    self.export_owner[name] = plan.name

    def _compile(self, sandbox, plan):
        stream, path = plan.stream, ("streams", plan.index)
        if not plan.requests:  # (none, or none with a name: `streamwright validate` reports those)
            self._problem((*path, "requests"), "a stream needs `requests`, a list of named requests: a stream reads "
                                                "only its own requests")
        if not isinstance(stream.get("transform"), dict):
            self._problem((*path, "transform"), "a stream needs `transform: {mode: page|run, steps: [...]}`")
            return
        if plan.mode not in MODES:
            self._problem((*path, "transform", "mode"), "`transform.mode` must be page or run")
        if not plan.steps:
            self._problem((*path, "transform", "steps"), "`transform.steps` must be a list of named steps")
        usable = self._check_names(sandbox, plan)
        self._compile_steps(sandbox, plan, usable)
        self._check_exports(plan)
        self._check_requests(plan)
        if plan.mode == "run":
            plan.order = self._run_order(plan)
        else:
            plan.order = [("request", item) for _, item in plan.requests[:1]] + [("step", step)
                                                                                 for _, step in plan.steps]


def plan_source(source):
    """A source's SourcePlan (its problems, and what runs for a selection, in which order), made in its own DuckDB."""
    sandbox = Sandbox()
    try:
        return SourcePlan(sandbox, source)
    finally:
        sandbox.close()


def check_source(source):
    """
    DuckDB checks of a source's streams before any request (SourcePlan): findings are (path, message) with paths into
    the source, such as ("streams", 2, "transform", "steps", 0, "select"), so streamwright validate reports file and
    line.
    """
    if not any(isinstance(stream, dict) for stream in source.get("streams") or []):
        return []
    return plan_source(source).problems
