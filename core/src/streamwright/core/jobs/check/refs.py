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
Section: references. `{{ config.NAME }}` must be declared. `{{ step }}` / `{{ step.column }}` must name a step (any
step: the plan orders them) and may only appear under the item keys the engine renders them in
(`JobRules.step_reference_keys`). Anything else must be one of the engine's built-in constants.
"""

from streamwright.core.jobs.check.findings import named_items
from streamwright.core.jobs.refs import INPUT_REF, STEP_REF, references, walk_strings

__all__ = ["check_references"]


def check_references(job, rules, findings):
    spec = job.get("spec")
    config = spec.get("config") if isinstance(spec, dict) else None
    declared = set(config) if isinstance(config, dict) else set()
    steps = {item["name"] for _, item in named_items(job, "steps")}
    connections = job.get("connections") if isinstance(job.get("connections"), dict) else {}
    places = []
    for section in ("connections", "runtime"):
        if isinstance(job.get(section), dict):
            places.extend(((section, key), None, value, frozenset()) for key, value in job[section].items())
    for section in ("steps", "outputs"):
        for index, item in named_items(job, section):
            rendered = (rules.source_reference_keys_of(item, connections) if section == "steps"
                        else rules.source_reference_keys)
            places.extend(((section, index, key), key, value, rendered) for key, value in item.items())
    for path, key, value, rendered in places:
        for text in walk_strings(value):
            for expr in references(text):
                problem = _problem(expr, key, declared, steps, rules, rendered)
                if problem:
                    findings.error(problem[0], path, problem[1])


def _problem(expr, key, declared, steps, rules, rendered):  # noqa: PLR0917 (one check, all its inputs)
    hit = INPUT_REF.match(expr)
    if hit:
        if hit.group(1) == "config" and hit.group(2) not in declared:
            return "undeclared-input", "config %r is not declared in spec.config" % hit.group(2)
        return None
    if expr in rules.builtin_references or key in rendered:
        return None
    hit = STEP_REF.match(expr)
    if hit and hit.group(1) in steps:
        if key in rules.step_reference_keys:
            return None
        return "unsupported-reference", "{{ %s }}: step values can only be used in %s" % (
            expr, ", ".join(sorted(rules.step_reference_keys)) or "no key of this engine")
    return "unknown-reference", "{{ %s }} is not a declared input, a step or one of %s" % (
        expr, ", ".join(sorted(rules.builtin_references)) or "no built-in constants")
