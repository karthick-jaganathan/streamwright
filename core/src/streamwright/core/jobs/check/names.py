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

"""Section: names. Steps and outputs share one namespace; names follow NAME_PATTERN and avoid reserved words."""

from streamwright.core.jobs.check.findings import named_items

__all__ = ["check_names"]


def check_names(job, rules, findings):
    connections = job.get("connections")
    if isinstance(connections, dict):
        for name in connections:
            problem = rules.name_problem(name)
            if problem:
                findings.error("bad-name", ("connections", str(name)), problem)
    seen = {}
    for section in ("steps", "outputs"):
        for index, item in named_items(job, section):
            name, path = item["name"], (section, index, "name")
            if name in rules.reserved_names:
                findings.error("reserved-name", path, "%r is reserved (%s)" % (
                    name, ", ".join(sorted(rules.reserved_names))))
            elif name in seen:
                findings.error("duplicate-name", path, "%r is already the name of %s[%d] (steps and outputs share "
                               "one namespace)" % ((name,) + seen[name]))
            else:
                seen[name] = (section, index)
