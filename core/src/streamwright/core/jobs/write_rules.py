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
The output contract shared by the checker and every writer (SEM-09).

An output is normalized once into an `OutputSpec`. Each writer declares a `WriteTarget`: the modes it supports, the
keys it reads (and which a mode requires), and which modes are safe to replay after a crash. `check_output` applies
the target to a spec, so `validate` and the writer agree: a key the target does not read is an error, never silently
ignored. Table-format rules (an overwrite limited by `replace_where` or explicitly `whole_table`; merge clauses; strict
schema evolution by default; target predicates rendered from pinned values) are plain functions any writer reuses.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, fields
from types import MappingProxyType
from typing import Any

from streamwright.core.jobs.policy import STEP_POLICY_KEYS
from streamwright.core.jobs.refs import step_columns_used, substitute
from streamwright.core.sql.quote import quote_literal

__all__ = [
    "COMMON_OUTPUT_KEYS",
    "MODES",
    "OutputSpec",
    "WriteRuleProblem",
    "WriteTarget",
    "check_merge_clauses",
    "check_output",
    "check_overwrite_scope",
    "check_schema_evolution",
    "evolves_schema",
    "merge_condition",
    "render_target_predicate",
    "warn_merge_scope",
]

MODES = ("append", "overwrite", "merge", "delete", "error")
COMMON_OUTPUT_KEYS = ("name", "description", "connection", "step", "sql", "mode")
_SCHEMA_EVOLUTION = ("strict", "add_columns")
_CLAUSE_ACTIONS = MappingProxyType({"when_matched": ("update", "delete"), "when_not_matched": ("insert",),
                                    "when_not_matched_by_source": ("update", "delete")})


@dataclass(frozen=True)
class OutputSpec:
    """One output, normalized: every writer and the checker read these fields and nothing else."""
    name: str
    connection: str | None = None
    step: str | None = None
    sql: str | None = None
    mode: str | None = None
    description: str | None = None
    table: str | None = None
    path: str | None = None
    format: str | None = None
    primary_key: Sequence[str] = ()
    condition: str | None = None
    target_predicate: str | None = None
    replace_where: str | None = None
    whole_table: bool | None = None
    when_matched: Sequence[Mapping[str, Any]] = ()
    when_not_matched: Sequence[Mapping[str, Any]] = ()
    when_not_matched_by_source: Sequence[Mapping[str, Any]] = ()
    schema_evolution: str | None = None
    partition_by: Sequence[str] = ()
    split_by: Sequence[str] = ()
    file_name: str | None = None
    layout: str | None = None
    options: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, item):
        known = {f.name for f in fields(cls)}
        # the execution-policy keys are honoured by the engine's executor, not by the output's write spec
        unknown = sorted(set(item) - known - set(STEP_POLICY_KEYS))
        if unknown:
            raise ValueError("output %r: unknown keys %s" % (item.get("name"), ", ".join(unknown)))
        values = {key: value for key, value in item.items() if key not in STEP_POLICY_KEYS}
        for key in ("primary_key", "partition_by", "split_by", "when_matched", "when_not_matched",
                    "when_not_matched_by_source"):
            if values.get(key) is None:
                values.pop(key, None)
            else:
                values[key] = tuple(values[key])
        if values.get("options") is None:
            values.pop("options", None)
        return cls(**values)

    def given(self):
        """The keys this output sets (beyond its name)."""
        return [f.name for f in fields(self) if f.name != "name" and getattr(self, f.name) not in (None, (), {})]


class WriteRuleProblem:
    """One rule finding: an error (code, message) or, with warning=True, a warning."""

    def __init__(self, code, message, warning=False):
        self.code, self.message, self.warning = code, message, warning

    def __repr__(self):
        return "%s(%s: %s)" % ("Warning" if self.warning else "Problem", self.code, self.message)

    def __eq__(self, other):
        return isinstance(other, WriteRuleProblem) and (self.code, self.message, self.warning) == (
            other.code, other.message, other.warning)

    def __hash__(self):
        return hash((self.code, self.message, self.warning))


@dataclass(frozen=True)
class WriteTarget:
    """
    What one writer supports.
      name             shown in messages (e.g. "DuckDB table", "files")
      modes            the modes it implements
      keys             OutputSpec keys it reads (besides COMMON_OUTPUT_KEYS)
      required         {mode or "*": keys that must be set}
      replay_safe      modes whose replay after a crash neither duplicates rows nor fails
      checks           extra rules: callables (spec) -> [WriteRuleProblem]
    """
    name: str
    modes: tuple[str, ...]
    keys: tuple[str, ...]
    required: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    replay_safe: tuple[str, ...] = ()
    checks: tuple[Callable[["OutputSpec"], Sequence[WriteRuleProblem]], ...] = ()


def check_output(spec, target):
    """Every problem (and warning) of `spec` against `target`."""
    found = []
    if spec.step is None and spec.sql is None:
        found.append(WriteRuleProblem("missing-key", "an output needs `step` or `sql`"))
    if spec.step is not None and spec.sql is not None:
        found.append(WriteRuleProblem("bad-value", "`step` and `sql` are exclusive"))
    if spec.mode is None:
        found.append(WriteRuleProblem("missing-key", "`mode` is required (%s writes support: %s)" % (
            target.name, ", ".join(target.modes))))
        return found
    if spec.mode not in target.modes:
        found.append(WriteRuleProblem("unsupported-mode", "mode %r is not supported by %s writes (supported: %s)" % (
            spec.mode, target.name, ", ".join(target.modes))))
        return found
    readable = set(COMMON_OUTPUT_KEYS) | set(target.keys)
    for key in spec.given():
        if key not in readable:
            found.append(WriteRuleProblem("unsupported-key", "%s writes do not use `%s`" % (target.name, key)))
    for key in tuple(target.required.get("*", ())) + tuple(target.required.get(spec.mode, ())):
        if getattr(spec, key) in (None, (), {}):
            found.append(WriteRuleProblem("missing-key", "mode %s of %s writes needs `%s`" % (
                spec.mode, target.name, key)))
    if spec.mode not in target.replay_safe:
        found.append(WriteRuleProblem("not-idempotent", "mode %s is not safe to replay: a retry after a crash "
                                      "between the write and its receipt %s" % (
                                          spec.mode, "fails" if spec.mode == "error" else "duplicates rows"),
                                      warning=True))
    for check in target.checks:
        found.extend(check(spec))
    return found


def check_overwrite_scope(spec):
    """An overwrite rewrites only what `replace_where` selects, or explicitly the `whole_table`."""
    if spec.mode != "overwrite":
        return []
    if spec.replace_where and spec.whole_table:
        return [WriteRuleProblem("bad-value", "replace_where and whole_table are exclusive")]
    if not spec.replace_where and spec.whole_table is not True:
        return [WriteRuleProblem("write-guard", "overwrite needs replace_where, or whole_table: true to rewrite the "
                                                "whole table")]
    return []


def check_merge_clauses(spec):
    """A merge needs its `condition` and at least one valid when_* clause."""
    if spec.mode != "merge":
        return []
    found = []
    if not spec.condition:
        found.append(WriteRuleProblem("missing-key", "merge needs a `condition` (using target. and source.)"))
    for clause, allowed in _CLAUSE_ACTIONS.items():
        for number, entry in enumerate(getattr(spec, clause)):
            if not isinstance(entry, Mapping) or entry.get("action") not in allowed:
                found.append(WriteRuleProblem("bad-value", "%s[%d]: action must be one of %s" % (
                    clause, number, "/".join(allowed))))
            elif "set" in entry and not isinstance(entry["set"], Mapping):
                found.append(WriteRuleProblem("bad-value", "%s[%d]: `set` must map target columns to expressions"
                                              % (clause, number)))
            elif set(entry) - {"action", "set", "predicate"}:
                found.append(WriteRuleProblem("unsupported-key", "%s[%d]: unknown keys %s (keys: action, set, "
                                              "predicate)" % (clause, number,
                                                              ", ".join(sorted(set(entry) - {"action", "set",
                                                                                             "predicate"})))))
    if not (spec.when_matched or spec.when_not_matched or spec.when_not_matched_by_source):
        found.append(WriteRuleProblem("missing-key", "merge needs at least one when_* clause"))
    return found


def warn_merge_scope(spec):
    """A merge without a source-derived target predicate scans the whole target and can insert a row twice."""
    if spec.mode != "merge":
        return []
    if not spec.target_predicate:
        return [WriteRuleProblem("merge-scope", "merge without target_predicate scans every target file; add a "
                                                "partition predicate", warning=True)]
    if not step_columns_used(spec.target_predicate):
        return [WriteRuleProblem("merge-scope", "target_predicate is not derived from the source rows; a source row "
                                                "outside it is inserted again even if it exists. Use e.g. "
                                                "target.event_date IN ({{ step.event_date }})", warning=True)]
    return []


def check_schema_evolution(spec):
    if spec.schema_evolution is not None and spec.schema_evolution not in _SCHEMA_EVOLUTION:
        return [WriteRuleProblem("bad-value", "schema_evolution must be one of %s" % ", ".join(_SCHEMA_EVOLUTION))]
    return []


def merge_condition(condition, target_predicate):
    """The merge's ON clause: the keys, AND the (rendered) target predicate that prunes target files."""
    return "(%s) AND (%s)" % (target_predicate, condition) if target_predicate else condition


def evolves_schema(spec):
    """Whether a write may add columns: only with schema_evolution: add_columns (strict by default)."""
    return spec.schema_evolution == "add_columns"


def render_target_predicate(text, values_of, constants, limit, quote_text=quote_literal):
    """
    A predicate with `{{ step.column }}` rendered as escaped literals: `values_of(step, column)` returns the (pinned)
    distinct values; `constants` maps names such as batch.id to one value.
    """
    if text is None:
        return None
    values = {(step, column): values_of(step, column) for step, column in step_columns_used(text)}
    return substitute(text, values, constants, limit, quote_text)
