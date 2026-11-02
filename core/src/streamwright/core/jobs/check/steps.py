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

"""Section: steps. One classification (shared with the executor) decides which keys a step may use."""

from streamwright.core.jobs import policy
from streamwright.core.jobs.check.findings import connections_of, named_items

__all__ = ["check_steps"]


def check_steps(job, rules, findings):
    connections = connections_of(job)
    for index, step in named_items(job, "steps"):
        path = ("steps", index)
        if "connection" in step and step["connection"] not in connections:
            findings.error("unknown-connection", (*path, "connection"), "unknown connection %r" % (
                step["connection"],))
            continue
        kind, problem = rules.classify_step(step, connections)
        if kind is None:
            findings.error("bad-step", path, problem)
            continue
        # the whole policy vocabulary is structurally accepted; the engine's capabilities say which it honours
        allowed = set(rules.step_keys(kind, step, connections)) | set(policy.STEP_POLICY_KEYS)
        for key in step:
            if key not in allowed:
                findings.error("unsupported-key", (*path, key), "a %s step does not use `%s` (keys: %s)" % (
                    kind, key, ", ".join(sorted(allowed))))
        for code, message, is_warning in rules.check_step(kind, step, connections):
            findings.add(code, path, message, is_warning)
        for code, message, is_warning in policy.check_policy(step, rules.step_policy_keys(kind), rules.engine,
                                                             surface="steps"):
            findings.add(code, path, message, is_warning)
