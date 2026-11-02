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
Checking a `kind: *Job` manifest without running it: no engine, broker, network or secret value is needed.

`check_job(job, rules)` runs one function per section and returns `streamwright.core.validation.Issue` objects. The
engine supplies its `JobRules`: the rule tables (also the source of its JSON schema), how a step is classified, and
what each connection's adapter accepts. Every section is defensive: a malformed part is reported once (by the
structure section) and skipped by the others.
"""

from streamwright.core.jobs.check.rules import JobRules
from streamwright.core.jobs.check.sections import SECTIONS, Findings

__all__ = ["SECTIONS", "JobRules", "check_job"]


def check_job(job, rules, filename=None):
    """Every problem and warning of `job` (a mapping, with config already resolved) as Issue objects."""
    findings = Findings(filename)
    if not isinstance(job, dict):
        findings.error("bad-type", (), "the job must be a mapping")
        return findings.issues
    for section in SECTIONS:
        section(job, rules, findings)
    return findings.issues
