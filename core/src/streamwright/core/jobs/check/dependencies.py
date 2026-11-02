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

"""Section: the dependency graph (queries that cannot be read, cycles)."""

from streamwright.core.jobs.check.findings import named_items
from streamwright.core.jobs.plan import PlanError, build_plan

__all__ = ["check_dependencies"]


def check_dependencies(job, rules, findings):
    if findings.errors:
        return                                    # a malformed job cannot be planned meaningfully
    steps = [item for _, item in named_items(job, "steps")]
    outputs = [item for _, item in named_items(job, "outputs")]
    try:
        build_plan({"steps": steps, "outputs": outputs}, rules.dependencies_of)
    except PlanError as exc:
        findings.error("dependency", ("steps",), str(exc))
