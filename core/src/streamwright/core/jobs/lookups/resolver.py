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
spec.lookups resolved for a run, shared by every engine.

An input bound to a lookup (`lookup: {from, field}` in spec.config / spec.secrets) gets its value from it unless the
value is given explicitly (--set, --config, --secrets, STREAMWRIGHT_SECRET_*): explicit values win, and a lookup
nothing needs never runs. Lookup templates read `{{ config.NAME }}`, `{{ secrets.NAME }}` (explicit, defaults, or
from other lookups: the order follows the references; a cycle is an error) and, per pass, `{{ <as>.COLUMN }}`.

runtime.fan_out runs the job once per item of `over` (a `rows: many` lookup, or a list input): a pass. Lookups that
read the pass's item run per pass; the others run once per invocation. Every value bound to a secret, every
`secret_fields` column and every transform input and output is masked (`enrol`) before anything connects.
"""

import copy
import re
from dataclasses import dataclass
from types import MappingProxyType

from streamwright.core.config.inputs import resolve_inputs
from streamwright.core.jobs.lookups.extract import LookupValueError, extract, text_value
from streamwright.core.jobs.lookups.providers import LookupFailedError, fetch, transform_row
from streamwright.core.jobs.refs import MOUSTACHE
from streamwright.core.runtime.logs.context import log_context

__all__ = ["FAN_OUT_DEFAULTS", "Lookups", "has_lookups", "lookup_optional"]

FAN_OUT_DEFAULTS = MappingProxyType({"as": "account", "concurrency": 1, "on_failure": "continue", "on_empty": "fail"})
_PATH = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z0-9_.\[\]]+)\s*$")
_SECTIONS = ("config", "secrets")


def has_lookups(job):
    """Whether the job declares spec.lookups or runtime.fan_out."""
    return bool(((job.get("spec") or {}).get("lookups")) or ((job.get("runtime") or {}).get("fan_out")))


def lookup_optional(job):
    """A copy of the job whose lookup-bound inputs are not required (state actions, offline checks)."""
    copied = copy.deepcopy(job)
    for section in _SECTIONS:
        for definition in ((copied.get("spec") or {}).get(section) or {}).values():
            if isinstance(definition, dict) and definition.get("lookup"):
                definition["required"] = False
    return copied


@dataclass
class _Item:
    label: str
    value: object
    source: str | None          # the lookup whose rows these are (its transforms apply per row)


class Lookups:
    """The lookup-bound inputs of a job: per invocation (`inputs()`) or per fan-out item (`inputs(item)`)."""

    def __init__(self, job, config=None, secrets=None, *, allow_exec=False, enrol=None, base_dir=None):
        """`config`, `secrets`: the explicit values; `enrol(value)` masks a secret value."""
        self.job, self.base_dir, self.allow_exec = job, base_dir, allow_exec
        spec = job.get("spec") or {}
        self.lookups = spec.get("lookups") or {}
        self.definitions = {section: spec.get(section) or {} for section in _SECTIONS}
        self.explicit = {"config": dict(config or {}), "secrets": dict(secrets or {})}
        fan_out = (job.get("runtime") or {}).get("fan_out")
        self.fan_out = dict(FAN_OUT_DEFAULTS, **fan_out) if fan_out else None
        self.item_name = self.fan_out["as"] if self.fan_out else None
        self._enrol = enrol or (lambda value: None)
        self._shared = {}

    # --- public ---------------------------------------------------------------------------------------------

    def items(self):
        """The fan-out items, as (label, item); [(None, None)] without runtime.fan_out."""
        if not self.fan_out:
            return [(None, None)]
        over = self.fan_out["over"]
        pass_ = _Pass(self, None)
        if over in self.lookups:
            rows = pass_.lookup(over)
            if not isinstance(rows, list):
                raise LookupFailedError("fan_out.over: lookup %r must have rows: many" % over)
            key = self.fan_out.get("key") or (next(iter(rows[0])) if rows else None)
            items = [_Item(str(row.get(key)), row, over) for row in rows]
        else:
            values = pass_.input_value("config", over)
            values = values if isinstance(values, list) else [] if values is None else [values]
            items = [_Item(str(value), value, None) for value in values]
        if not items and self.fan_out["on_empty"] == "fail":
            raise LookupFailedError("fan_out: %s gave no item (on_empty: fail)" % over)
        return [(item.label, item) for item in items]

    def inputs(self, item=None):
        """(config, secrets): the explicit values plus every lookup-bound input's value, for one pass."""
        pass_ = _Pass(self, item)
        found = {section: dict(self.explicit[section]) for section in _SECTIONS}
        for section in _SECTIONS:
            for name, definition in self.definitions[section].items():
                if found[section].get(name) is None and isinstance(definition, dict) and definition.get("lookup"):
                    found[section][name] = pass_.input_value(section, name)
        return found["config"], found["secrets"]


class _Pass:
    """One pass's resolution: values and per-item lookups cached here, the others in the invocation's cache."""

    def __init__(self, lookups, item):
        self.owner, self.item = lookups, item
        self.local, self.values, self.active = {}, {}, []
        self._row = None

    def input_value(self, section, name):
        key = (section, name)
        if key not in self.values:
            self.values[key] = self._input_value(section, name)
        return self.values[key]

    def _input_value(self, section, name):
        owner = self.owner
        explicit = owner.explicit[section].get(name)
        definition = owner.definitions[section].get(name)
        if not isinstance(definition, dict):
            return explicit
        binding = definition.get("lookup")
        if explicit is not None or not binding:
            raw = explicit if explicit is not None else definition.get("default")
            # typed, so a list input renders as a list; a secret's problem never shows its value
            return None if raw is None else resolve_inputs({name: definition}, {name: raw}, section,
                                                           hide_values=section == "secrets")[name]
        what = "%s %r (lookup %s%s)" % (section, name, binding.get("from"),
                                         "." + binding["field"] if binding.get("field") else "")
        try:
            value = extract(self.lookup(binding["from"]), binding.get("field"))
            if not (definition.get("type") == "list" and isinstance(value, list)):
                value = text_value(value, what)
        except LookupValueError as exc:
            raise LookupFailedError("%s: %s" % (what, exc)) from None
        if section == "secrets":
            owner._enrol(value)
        return value

    def lookup(self, name):
        owner = self.owner
        if name == owner.item_name:
            return self._item_row(name)
        if name not in owner.lookups:
            raise LookupFailedError("lookup %r is not declared in spec.lookups" % name)
        spec = owner.lookups[name]
        cache = self.local if owner.item_name and _reads(spec, owner.item_name) else owner._shared
        if name not in cache:
            if name in self.active:
                raise LookupFailedError("lookups depend on each other: %s" % " -> ".join([*self.active, name]))
            self.active.append(name)
            try:
                with log_context(lookup=name):
                    cache[name] = fetch(name, spec, _Context(self))
            finally:
                self.active.pop()
        return cache[name]

    def _item_row(self, name):
        if self.item is None:
            raise LookupFailedError("lookup %r is the fan_out item: it needs runtime.fan_out" % name)
        if self._row is None:
            source = self.item.source
            self._row = self.item.value if source is None else transform_row(
                source, self.owner.lookups[source], self.item.value, _Context(self))
        return self._row

    def reference(self, namespace, path, row=None):
        if namespace == "row" and row is not None:
            return extract(row, path)
        if namespace in _SECTIONS:
            head, _, rest = path.partition(".")
            value = self.input_value(namespace, head)
            return extract(value, rest) if rest else value
        if namespace == self.owner.item_name:
            return extract(self._item_row(namespace), path)
        raise LookupFailedError("{{ %s.%s }} is not config, secrets%s" % (
            namespace, path, ", row or " + self.owner.item_name if self.owner.item_name else " or row"))


class _Context:
    """What a provider needs: render(node), job, allow_exec, base_dir, enrol(value), with_row(row)."""

    def __init__(self, pass_, row=None):
        self.pass_, self.row = pass_, row
        owner = pass_.owner
        self.job, self.allow_exec, self.base_dir, self.enrol = owner.job, owner.allow_exec, owner.base_dir, \
            owner._enrol

    def with_row(self, row):
        return _Context(self.pass_, row)

    def render(self, node):
        if isinstance(node, str):
            whole = MOUSTACHE.fullmatch(node.strip())
            if whole and _PATH.match(whole.group(1)):
                return self._value(whole.group(1))
            return MOUSTACHE.sub(self._text, node)
        if isinstance(node, list):
            return [self.render(item) for item in node]
        if isinstance(node, dict):
            return {key: self.render(value) for key, value in node.items()}
        return node

    def _value(self, expr):
        namespace, path = _PATH.match(expr).groups()
        try:
            return self.pass_.reference(namespace, path, self.row)
        except LookupValueError as exc:
            raise LookupFailedError("{{ %s }}: %s" % (expr.strip(), exc)) from None

    def _text(self, match):
        hit = _PATH.match(match.group(1))
        if not hit:
            return match.group(0)
        value = self._value(match.group(1))
        return "" if value is None else ",".join(map(str, value)) if isinstance(value, list) else str(value)


def _reads(node, name):
    """Whether templates in `node` read `{{ name.* }}`."""
    if isinstance(node, str):
        return any(hit is not None and hit.group(1) == name for hit in map(_PATH.match, MOUSTACHE.findall(node)))
    if isinstance(node, list):
        return any(_reads(item, name) for item in node)
    if isinstance(node, dict):
        return any(_reads(value, name) for value in node.values())
    return False
