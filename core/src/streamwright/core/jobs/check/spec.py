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

"""Section: spec.config / spec.secrets definitions, by core's source input rules."""

import datetime
import re
from types import MappingProxyType

from streamwright.core.jobs.check.rules import INPUT_NAME_PATTERN

__all__ = ["check_spec"]

_DATE = re.compile(r"^(today|-?[0-9]+d|[0-9]{4}-[0-9]{2}-[0-9]{2})$")
_DEFAULTS = MappingProxyType({
    "string": (lambda v: isinstance(v, str), "text"),
    "integer": (lambda v: isinstance(v, int) and not isinstance(v, bool), "a whole number"),
    "number": (lambda v: isinstance(v, (int, float)) and not isinstance(v, bool), "a number"),
    "boolean": (lambda v: isinstance(v, bool), "true or false"),
    "date": (lambda v: isinstance(v, datetime.date) or (isinstance(v, str) and bool(_DATE.match(v))),
             "a date: YYYY-MM-DD, today or -30d"),
    "list": (lambda v: isinstance(v, list), "a list"),
})


def check_spec(job, rules, findings):
    spec = job.get("spec")
    if not isinstance(spec, dict):
        return
    for section in ("config", "secrets"):
        inputs = spec.get(section)
        if not isinstance(inputs, dict):
            continue
        for name, definition in inputs.items():
            path = ("spec", section, str(name))
            if not isinstance(name, str) or not re.match(INPUT_NAME_PATTERN, name):
                findings.error("bad-name", path, "%r is not a valid input name" % (name,))
            if isinstance(definition, dict):
                _check_definition(section, definition, path, findings)


def _check_definition(section, definition, path, findings):
    kind = definition.get("type", "string")
    if kind == "list" and "items" not in definition:
        findings.error("missing-key", (*path, "items"), "a list input needs `items`")
    if kind != "list" and "items" in definition:
        findings.error("bad-value", (*path, "items"), "`items` only applies to `type: list`")
    if "default" not in definition:
        return
    if section == "secrets":
        findings.warning("secret-default", (*path, "default"), "secrets should not have defaults; pass them at "
                         "run time")
    rule = _DEFAULTS.get(kind)
    value = definition["default"]
    if rule is not None and value is not None and not rule[0](value):
        findings.error("bad-type", (*path, "default"), "expected %s" % rule[1])
