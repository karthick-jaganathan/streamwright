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

"""The compilation of a stream's steps with DuckDB, and the checks of its names, exports and request partitions."""

import contextlib
import re

from streamwright.core.engine.sql.list_parameters import compared_lists, quantified
from streamwright.core.engine.sql.stream_plan import exports_of, mapping_entries, partition_fields
from streamwright.core.engine.sql.tables import COLUMNS, SqlError, _column_problem, _sample
from streamwright.core.engine.sql.types import _unnamed_struct

_READS = "a step reads its stream's requests and earlier steps"
_BUILTIN = "%r is the name of a DuckDB built-in table or view: name it otherwise"
_LIST_COMPARED = ("`$%(name)s` is a list (%(type)s), which IN and comparisons take as one value: use "
                  "`= ANY($%(name)s)` or `list_contains($%(name)s, x)` (to compare whole lists, cast it: "
                  "`$%(name)s::%(type)s`)")
_QUANTIFIED = "IN/ANY/ALL"
# the errors of the places where DuckDB takes no subquery (lambdas, arguments that must be constants, non-inner joins,
# reused column aliases, TRY, ...), the only ones that make a step's parameters plain casts; and of the lists of ANY
# and ALL: DuckDB reads `x = ANY($ids)` as `x = ANY(SELECT unnest($ids))`, but `x = ANY((SELECT ...))` compares x
# with the subquery's one row, the whole list
_NO_SUBQUERY = re.compile(r"subquer|constant", re.I)


class StreamCompilation:
    """SourcePlan's checks of a stream: table names, compiled steps, exports and request partitions."""

    def _check_names(self, sandbox, plan):
        """
        Request and step names are the stream's tables: unique (ignoring case) and not DuckDB's. Returns lowercase
        name -> "request" / "step" of the names that can be tables.
        """
        usable, seen = {}, set()
        for kind, entries, where in (("request", plan.requests, ("requests",)),
                                     ("step", plan.steps, ("transform", "steps"))):
            for position, item in entries:
                lower, path = item["name"].lower(), ("streams", plan.index, *where, position, "name")
                if lower in seen:
                    self._problem(path, "%r is the name of another request or step of this stream: they are its "
                                        "tables (names ignore case)" % item["name"])
                elif lower in sandbox.relations:
                    self._problem(path, _BUILTIN % item["name"])
                else:
                    usable[lower] = kind
                seen.add(lower)
        return usable

    def _make(self, sandbox, name, columns, *, made, path, what):
        """An empty table for compiling, dropped when its stream is compiled; False (a problem) if DuckDB cannot."""
        try:
            sandbox.create_empty(name, columns)
        except SqlError as exc:
            self._problem(path, "DuckDB cannot make the table of %s: %s" % (what, exc))
            return False
        made.append(name)
        return True

    def _compile_steps(self, sandbox, plan, usable):
        """Each step compiled in list order, against empty tables of the requests and earlier steps it reads."""
        names = [item["name"] for _, item in plan.requests] + [step["name"] for _, step in plan.steps]
        positions = {step["name"].lower(): at for at, (_, step) in enumerate(plan.steps)}
        made, ready = [], set()  # the tables made for this stream; the lowercase names a step can read
        try:
            for position, item in plan.requests:
                lower = item["name"].lower()
                if usable.get(lower) == "request" and self._make(
                        sandbox, item["name"], COLUMNS, made=made,
                        path=("streams", plan.index, "requests", position, "name"), what="request %r" % item["name"]):
                    ready.add(lower)
            for at, (position, step) in enumerate(plan.steps):
                name, path = step["name"], ("streams", plan.index, "transform", "steps", position, "select")
                try:
                    query, reads, parameters = sandbox.parse(step.get("select"), names)
                except SqlError as exc:
                    self._problem(path, str(exc))
                    continue
                plan.reads[name] = reads
                if name.lower() in reads:
                    self._problem(path, "step %r reads itself: %s" % (name, _READS))
                    continue
                later = [plan.steps[positions[read]][1]["name"] for read in sorted(reads)
                         if positions.get(read, -1) > at]
                if later:
                    self._problem(path, "step %r reads later step(s) %s: %s" % (name, ", ".join(later), _READS))
                    continue
                bound = self._bind(sandbox, query, parameters, path)
                if bound is None:
                    continue
                plan.parameters[name] = sorted(set(parameters))
                if not all(read in ready for read in reads):
                    continue  # it reads a request or step with problems, reported where they are
                try:
                    query, columns, plain = self._describe(sandbox, bound, plan.parameters[name])
                except SqlError as exc:
                    self._problem(path, str(exc))
                    continue
                problem = _column_problem([column for column, _ in columns])
                if problem:  # every step's columns can be output fields
                    self._problem(path, problem)
                    continue
                unnamed = [column for column, kind in columns if _unnamed_struct(kind)]
                if unnamed:  # every step is a table
                    self._problem(path, "column(s) %s hold unnamed structs (row(...) or (a, b)), which a step's "
                                        "table cannot store: name their fields, {'a': ..., 'b': ...}" % ", ".join(
                                            unnamed))
                    continue
                plan.queries[name], plan.columns[name] = query, columns
                plan.conform[name] = columns if plain else None
                if usable.get(name.lower()) == "step" and self._make(sandbox, name, columns, made=made, path=path,
                                                                     what="step %r" % name):
                    ready.add(name.lower())
        finally:
            for table in made:
                with contextlib.suppress(SqlError):  # (the next stream's tables replace it)
                    sandbox.drop_table(table)

    def _bind(self, sandbox, query, parameters, path):
        """
        The query with its `$name` parameters typed by their config inputs, as (scalar subqueries, plain casts,
        whether one is in the list of an ANY or ALL): see Sandbox.typed. None if a parameter is not a config input, or
        a list is compared as one value (problems reported).
        """
        problems = []
        for name in sorted(set(parameters)):
            if name not in self.types:
                problems.append("`$%s` is not a config input (declare it in spec.config)" % name)
            elif self.types[name] is None:
                problems.append("`$%s`: the config input's type cannot be a parameter" % name)
        lowered = {}
        for name in sorted(set(parameters)):
            lowered.setdefault(name.lower(), []).append(name)
        problems += ["%s are one parameter to DuckDB (names ignore case): use one of them" % " and ".join(
            "`$%s`" % name for name in same) for same in lowered.values() if len(same) > 1]
        if problems:
            self.problems += [(path, problem) for problem in problems]
            return None
        try:
            tree = sandbox.tree(query) if parameters else {}
            compared = compared_lists(tree, self.types)
            if compared:
                self.problems += [(path, _LIST_COMPARED % {"name": name, "type": self.types[name]})
                                  for name in compared]
                return None
            return (sandbox.typed(query, parameters, self.types),
                    sandbox.typed(query, parameters, self.types, scalar=False), quantified(tree))
        except SqlError as exc:
            self._problem(path, str(exc))
            return None

    def _describe(self, sandbox, bound, parameters):
        """
        (query, columns, plain) of a step: its parameters as scalar subqueries, compiled with null placeholders (the
        types are the same whatever the values); or, only where DuckDB takes no subquery (_NO_SUBQUERY, and the lists
        of ANY and ALL), as plain casts, compiled with sample values (or nulls, if DuckDB rejects those), whose table
        a run gives these columns. Any other error is the query's.
        """
        scalar, plain, quantified = bound
        nulls = dict.fromkeys(parameters)
        try:
            return scalar, sandbox.describe(scalar, nulls), False
        except SqlError as exc:
            if not parameters or not (_NO_SUBQUERY.search(str(exc)) or (quantified and _QUANTIFIED in str(exc))):
                raise
        try:
            return plain, sandbox.describe(plain, {name: _sample(self.types[name]) for name in parameters}), True
        except SqlError:
            return plain, sandbox.describe(plain, nulls), True

    def _check_exports(self, plan):
        """Each export writes a step of its stream; its keys are the step's columns, and `cursor_field` an export's."""
        stream, path = plan.stream, ("streams", plan.index, "export")
        steps = {step["name"].lower(): step["name"] for _, step in plan.steps}
        export = stream.get("export") if isinstance(stream.get("export"), dict) else {}
        known = {}  # export -> its column names
        for name in exports_of(stream):
            spec = export[name]
            if not isinstance(spec, dict) or not isinstance(spec.get("step"), str):
                self._problem((*path, name), "export %r needs `step`: the step it writes" % name)
                continue
            step = steps.get(spec["step"].lower())
            if step is None:
                self._problem((*path, name, "step"), "%r is not a step of this stream (steps: %s)" % (
                    spec["step"], ", ".join(step for _, step in sorted(steps.items())) or "none"))
                continue
            plan.exports.append((name, spec, step))
            columns = plan.columns.get(step)
            if columns is None:
                continue  # its step has problems, reported where they are
            names = known[name] = [column for column, _ in columns]
            keys = spec.get("primary_key")
            for position, key in enumerate(keys if isinstance(keys, list) else []):
                if key not in names:
                    self._problem((*path, name, "primary_key", position), "primary_key %r is not a column of step %r "
                                                                          "(columns: %s)" % (key, step,
                                                                                             ", ".join(names)))
        incremental = stream.get("incremental")
        cursor = incremental.get("cursor_field") if isinstance(incremental, dict) else None
        if cursor is not None and known and len(known) == len(plan.exports) and not any(
                cursor in names for names in known.values()):
            self._problem(("streams", plan.index, "incremental", "cursor_field"),
                          "cursor_field %r is not a column of an export (columns: %s)" % (cursor, "; ".join(
                              "%s: %s" % (export, ", ".join(names)) for export, names in known.items())))

    def _check_requests(self, plan):
        """
        Page mode reads one request without partitions. A request partition comes `from:` an earlier request (its
        fields are dotted paths in its records) or a step, whose columns its fields are.
        """
        stream, path = plan.stream, ("streams", plan.index, "requests")
        items = stream.get("requests") if isinstance(stream.get("requests"), list) else []
        if plan.mode == "page" and len(items) > 1:
            self._problem(path, "`transform.mode: page` reads one request: with %d requests, use `mode: run`" %
                          len(items))
        steps = {step["name"].lower(): step["name"] for _, step in plan.steps}
        earlier = set()
        for position, item in plan.requests:
            parts = item.get("partitions")
            if parts and plan.mode == "page":
                self._problem((*path, position, "partitions"), "request partitions need `transform.mode: run`")
            for k, part in mapping_entries(parts):
                source = part.get("from")
                if not isinstance(source, str) or source.lower() in earlier:
                    continue
                part_path = (*path, position, "partitions", k)
                step = steps.get(source.lower())
                if step is None:
                    self._problem((*part_path, "from"), "%r is not an earlier request or a step of this stream" %
                                  source)
                    continue
                columns = plan.columns.get(step)
                if columns is None:
                    continue
                names = [column for column, _ in columns]
                several = isinstance(part.get("fields"), list)
                for f, field in enumerate(partition_fields(part)[0]):
                    if field not in names:
                        self._problem(part_path + (("fields", f) if several else ("field",)),
                                      "%r is not a column of step %r (columns: %s)" % (field, step, ", ".join(names)))
            earlier.add(item["name"].lower())
