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
Section: outputs: an apply output through its connector's operation (streamwright.core.jobs.apply), a publish output
through its broker, any other through the WriteTarget of its connection (the writers' own rules).
"""

from streamwright.core.jobs import policy
from streamwright.core.jobs.apply.log import check_output_log
from streamwright.core.jobs.check.findings import connections_of, named_items
from streamwright.core.jobs.write_rules import OutputSpec, check_output

__all__ = ["check_outputs"]


def check_outputs(job, rules, findings):
    connections = connections_of(job)
    steps = {item["name"] for _, item in named_items(job, "steps")}
    for index, item in named_items(job, "outputs"):
        path = ("outputs", index)
        conn = connections.get(item.get("connection"))
        applied = rules.check_apply(item, conn) if conn is not None else None
        keys = policy.APPLY_POLICY_KEYS if applied is not None else rules.output_policy_keys()
        for code, message, is_warning in policy.check_policy(item, keys, rules.engine, surface="outputs"):
            findings.add(code, path, message, is_warning)
        if isinstance(item.get("step"), str) and item["step"] not in steps:
            findings.error("unknown-step", (*path, "step"), "unknown step %r" % item["step"])
        if conn is None:
            if isinstance(item.get("connection"), str):
                findings.error("unknown-connection", (*path, "connection"), "unknown connection %r" % (
                    item["connection"],))
            continue
        if applied is not None:
            for code, message, is_warning in check_output_log(item.get("log")):
                findings.add(code, (*path, "log"), message, is_warning)
        elif "log" in item:
            findings.error("unsupported-key", (*path, "log"), "only an apply output logs its results (log:); a step's "
                           "log: logs the step's rows")
        published = applied if applied is not None else rules.check_publish(item, conn)
        if published is not None:
            for code, message, is_warning in published:
                findings.add(code, path, message, is_warning)
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
