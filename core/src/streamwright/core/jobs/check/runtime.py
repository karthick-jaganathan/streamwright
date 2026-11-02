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

"""Section: runtime (state store placement; the rest is in the rule tables)."""

from streamwright.core.jobs.check.findings import connections_of

__all__ = ["check_engine_rules", "check_runtime"]


def check_runtime(job, rules, findings):
    runtime = job.get("runtime")
    state = runtime.get("state") if isinstance(runtime, dict) else None
    if not isinstance(state, dict):
        return
    for code, message, is_warning in rules.check_state(state, connections_of(job)):
        findings.add(code, ("runtime", "state"), message, is_warning)


def check_engine_rules(job, rules, findings):
    """Section: what only the engine knows (its JobRules.check_engine)."""
    for code, path, message, is_warning in rules.check_engine(job):
        findings.add(code, path, message, is_warning)
