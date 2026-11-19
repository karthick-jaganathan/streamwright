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
What a service checks before it exposes a job (plan §5.4, §5.5): the job is valid, `set` names its config inputs,
and every input a caller may set is safe where the job uses it.

- Settable: every spec.config input, except those `set` binds (from a claim, or a fixed value) and those with a
  `lookup:` binding (a caller value would skip the lookup, e.g. the tenant-filtered account lookup). spec.secrets is
  never settable.
- SQL (T3): `{{ config.x }}` is spliced into SQL as text. A settable input in a step's or output's `sql`, a read
  `query`, a call's `arguments.query` or a database lookup's `query` must be a number, boolean or date, or text (or
  a list of text) whose `enum` or `pattern` confines it to letters, digits and _ - . : @ +.
- Paths (T12): a settable input in a connection, a read or output `path`, `file_name`, `layout`, `table`, a call's
  `arguments.path` / `destination`, or a lookup's `command` / `secret_id` must be a number or date, or text whose
  `enum` or `pattern` confines it to letters, digits and _ - : @ + = , (no `/`, `\\`, `.`, `*`, `?`, `[`, `{`, `%`).
- A pattern confines an input only when every character it can match is allowed: `.`, negated classes and `\\s` / `\\S`
  never do.
"""

import re
import string
from dataclasses import dataclass
from re import _constants as sre_constants
from re import _parser as sre_parse
from types import MappingProxyType
from typing import Any

from streamwright.core.jobs.apply.rules import apply_operation
from streamwright.core.jobs.duckdb.engine import validate_job
from streamwright.core.jobs.grammar import to_internal
from streamwright.core.jobs.inputs import declared_inputs
from streamwright.core.jobs.manifest import JobError, load_job
from streamwright.core.jobs.payload import declared_input
from streamwright.core.jobs.refs import input_references

__all__ = ["PATH_CHARACTERS", "SQL_CHARACTERS", "ServedJob", "confines", "served_job", "settable_inputs"]

_WORD = frozenset(string.ascii_letters + string.digits + "_")
SQL_CHARACTERS = _WORD | frozenset("-.:@+")
PATH_CHARACTERS = _WORD | frozenset("-:@+=,")
_SAFE_TYPES = frozenset({"integer", "number", "boolean", "date"})
_PATH_TYPES = frozenset({"integer", "date"})
_SQL_KEYS = ("sql", "query")
_PATH_KEYS = ("path", "file_name", "layout", "table")
_CALL_SQL = ("query",)
_CALL_PATH = ("path", "destination")
_UNBOUNDED = "unbounded"


@dataclass(frozen=True)
class ServedJob:
    """A job a service exposes: its entry, the manifest as written, and the inputs a caller may set."""
    entry: Any
    job: dict
    config: dict                                   # spec.config definitions
    settable: frozenset
    input: dict | None                             # spec.input (with defaults), or None
    scope: str | None                              # runtime.state.scope, as written
    applies: bool                                  # an output applies operations: requests need an Idempotency-Key
    secrets: frozenset                             # spec.secrets' names (never settable)
    persistent: bool                               # runtime.state keeps batches (sqlite, connection): job keys work


def _characters(parsed, found):
    """Adds to `found` every character the parsed pattern can match; _UNBOUNDED when that cannot be listed."""
    for op, value in parsed:
        if op is sre_constants.LITERAL:
            found.add(chr(value))
        elif op in (sre_constants.ANY, sre_constants.NOT_LITERAL):
            found.add(_UNBOUNDED)
        elif op is sre_constants.IN:
            _set_characters(value, found)
        elif op in (sre_constants.MAX_REPEAT, sre_constants.MIN_REPEAT):
            _characters(value[2], found)
        elif op is sre_constants.SUBPATTERN:
            _characters(value[-1], found)
        elif op is sre_constants.BRANCH:
            for branch in value[1]:
                _characters(branch, found)
        elif op is sre_constants.CATEGORY:
            _category(value, found)
        elif op in (sre_constants.AT, sre_constants.ASSERT, sre_constants.ASSERT_NOT):
            continue
        else:                                     # back references, conditionals: not listed
            found.add(_UNBOUNDED)


def _category(value, found):
    if value is sre_constants.CATEGORY_DIGIT:
        found.update(string.digits)
    elif value is sre_constants.CATEGORY_WORD:
        found.update(_WORD)
    else:                                         # \s, \S, \D, \W
        found.add(_UNBOUNDED)


def _set_characters(items, found):
    for op, value in items:
        if op is sre_constants.NEGATE:
            found.add(_UNBOUNDED)
        elif op is sre_constants.LITERAL:
            found.add(chr(value))
        elif op is sre_constants.RANGE:
            low, high = value
            if high - low > 0x2FF:                # wider than any alphabet a safe pattern needs
                found.add(_UNBOUNDED)
            else:
                found.update(chr(code) for code in range(low, high + 1))
        elif op is sre_constants.CATEGORY:
            _category(value, found)
        else:
            found.add(_UNBOUNDED)


def confines(definition, allowed):
    """True when the input's enum or pattern keeps every value within the characters `allowed`."""
    definition = definition or {}
    if "enum" in definition:
        return all(set(str(value)) <= allowed for value in definition["enum"] or ())
    pattern = definition.get("pattern")
    if not isinstance(pattern, str):
        return False
    found = set()
    try:
        parsed = sre_parse.parse(pattern)
    except re.error:
        return False
    if parsed.state.flags & (re.IGNORECASE | re.VERBOSE):
        # case folding maps some letters onto characters outside ASCII (e.g. K to the Kelvin sign): not listed
        found.add(_UNBOUNDED)
    _characters(parsed, found)
    return _UNBOUNDED not in found and found <= allowed


def _value_type(definition):
    definition = definition or {}
    return definition.get("items", "string") if definition.get("type") == "list" else definition.get("type",
                                                                                                         "string")


def settable_inputs(job, bound):
    """The spec.config inputs a caller may set: not bound by the service, and without a `lookup:` binding."""
    return frozenset(name for name, definition in declared_inputs(job, "config").items()
                     if name not in bound and not (definition or {}).get("lookup"))


def _items(job):
    for section in ("steps", "outputs"):
        for index, item in enumerate(job.get(section) or ()):
            if isinstance(item, dict):
                yield "%s[%d]" % (section, index), item


def _regions(job):
    """[(where, what, node)]: the parts of the job where an input shapes SQL ('sql') or what is read ('path')."""
    regions = [("connections", "path", job.get("connections") or {})]
    for where, item in _items(job):
        regions.extend(("%s.%s" % (where, key), "sql", item[key]) for key in _SQL_KEYS if key in item)
        regions.extend(("%s.%s" % (where, key), "path", item[key]) for key in _PATH_KEYS if key in item)
        arguments = (item.get("call") or {}).get("arguments") if isinstance(item.get("call"), dict) else None
        if isinstance(arguments, dict):
            regions.extend(("%s.call.arguments.%s" % (where, key), "sql", arguments[key])
                           for key in _CALL_SQL if key in arguments)
            regions.extend(("%s.call.arguments.%s" % (where, key), "path", arguments[key])
                           for key in _CALL_PATH if key in arguments)
    for name, lookup in ((job.get("spec") or {}).get("lookups") or {}).items():
        if isinstance(lookup, dict):
            if "query" in lookup:
                regions.append(("spec.lookups.%s.query" % name, "sql", lookup["query"]))
            regions.extend(("spec.lookups.%s.%s" % (name, key), "path", lookup[key])
                           for key in ("command", "secret_id", "transform") if key in lookup)
    return regions


def _unsafe(definition, what):
    kind = _value_type(definition)
    if what == "sql":
        return kind not in _SAFE_TYPES and not confines(definition, SQL_CHARACTERS)
    return kind not in _PATH_TYPES and not confines(definition, PATH_CHARACTERS)


_RULES = MappingProxyType({
    "sql": "is spliced into SQL as text: give it type integer, number, boolean or date, or an enum or pattern of "
           "letters, digits and _ - . : @ + (e.g. ^[A-Za-z0-9_-]{1,64}$), or bind it in serve.yaml",
    "path": "chooses what is read or written: give it type integer or date, or an enum or pattern of letters, digits "
            "and _ - : @ + = , (no / . * ? [ { %%; e.g. ^[0-9]{1,12}$), or bind it in serve.yaml",
})


def _problems(entry, job, config, settable):
    where = "jobs.%s" % entry.name
    problems = []
    secrets = declared_inputs(job, "secrets")
    for name, (kind, _) in entry.bound.items():
        if name in secrets:
            problems.append("%s.set.%s: a secret is never set by a request" % (where, name))
        elif name not in config:
            problems.append("%s.set.%s: %s declares no config input %r (declared: %s)" % (
                where, name, entry.manifest, name, ", ".join(sorted(config)) or "none"))
        elif kind == "value" and (config[name] or {}).get("lookup"):
            problems.append("%s.set.%s: the input has a lookup; a fixed value would replace it" % (where, name))
    for region, what, node in _regions(job):
        for name in sorted(input_references(node, "config") & settable):
            if _unsafe(config.get(name), what):
                problems.append("%s: config input %r (settable by callers) is used in %s and %s" % (
                    where, name, region, _RULES[what].replace("%%", "%")))
    return problems


def served_job(entry):
    """The ServedJob of a serve.yaml entry: (job, problems, warnings); job is None when there are problems."""
    report = validate_job(entry.manifest)
    if report.job is None or report.problems:
        return None, ["jobs.%s: %s is not a valid job: %s" % (entry.name, entry.manifest, issue)
                      for issue in report.problems or ["unreadable"]], []
    try:
        job = load_job(entry.manifest)            # as written: the checks follow its `{{ config.* }}` references
    except JobError as exc:
        return None, ["jobs.%s: %s" % (entry.name, exc)], []
    config = declared_inputs(job, "config")
    settable = settable_inputs(job, entry.bound)
    problems = _problems(entry, job, config, settable)
    claim_bound = {name for name, (kind, _) in entry.bound.items() if kind == "claim"}
    warnings = ["jobs.%s: lookup %r does not use an input bound from a claim (%s): it is not filtered by the "
                "caller's tenant" % (entry.name, name, ", ".join(sorted(claim_bound)) or "none")
                for name, lookup in ((job.get("spec") or {}).get("lookups") or {}).items()
                if isinstance(lookup, dict) and lookup.get("type") == "database"
                and not input_references(lookup, "config") & claim_bound]
    if problems:
        return None, problems, warnings
    state = (job.get("runtime") or {}).get("state") or {}
    scope = state.get("scope") if isinstance(state, dict) and isinstance(state.get("scope"), str) else None
    persistent = isinstance(state, dict) and bool(state.get("connection") or state.get("sqlite"))
    return ServedJob(entry, job, config, settable, declared_input(job), scope, _applies(job),
                     frozenset(declared_inputs(job, "secrets")), persistent), [], warnings


def _applies(job):
    internal = to_internal(job)
    connections = internal.get("connections") or {}
    return any(apply_operation(output, connections.get(output.get("connection")) or {})[1] is not None
               for output in internal.get("outputs") or () if isinstance(output, dict))

