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

"""Section: outputs, through the WriteTarget of each output's connection (the writers' own rules)."""

from streamwright.core.jobs import policy
from streamwright.core.jobs.check.findings import connections_of, named_items
from streamwright.core.jobs.write_rules import OutputSpec, check_output

__all__ = ["check_outputs"]


def check_outputs(job, rules, findings):
    connections = connections_of(job)
    steps = {item["name"] for _, item in named_items(job, "steps")}
    for index, item in named_items(job, "outputs"):
        path = ("outputs", index)
        for code, message, is_warning in policy.check_policy(item, rules.output_policy_keys(), rules.engine,
                                                             surface="outputs"):
            findings.add(code, path, message, is_warning)
        if isinstance(item.get("step"), str) and item["step"] not in steps:
            findings.error("unknown-step", (*path, "step"), "unknown step %r" % item["step"])
        conn = connections.get(item.get("connection"))
        if conn is None:
            if isinstance(item.get("connection"), str):
                findings.error("unknown-connection", (*path, "connection"), "unknown connection %r" % (
                    item["connection"],))
            continue
        target = rules.write_target(conn)
        if target is None:
            findings.error("not-writable", (*path, "connection"), "connection %r (provider %s) cannot be written "
                           "to" % (item["connection"], conn["provider"]))
            continue
        try:
            spec = OutputSpec.from_mapping(item)
        except (TypeError, ValueError):
            continue                                       # reported by the structure section
        for problem in check_output(spec, target):
            findings.add(problem.code, path, problem.message, problem.warning)
