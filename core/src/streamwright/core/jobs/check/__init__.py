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

from streamwright.core.jobs.check.findings import connections_of, named_items
from streamwright.core.jobs.check.rules import JobRules, install_hint
from streamwright.core.jobs.check.sections import SECTIONS, Findings
from streamwright.core.validation.checker import ERROR
from streamwright.core.validation.issues import Issue

__all__ = ["SECTIONS", "JobRules", "check_job", "install_hint"]


def check_job(job, rules, filename=None):
    """Every problem and warning of `job` (a mapping, with config already resolved) as Issue objects."""
    findings = Findings(filename)
    if not isinstance(job, dict):
        findings.error("bad-type", (), "the job must be a mapping")
        return findings.issues
    for section in SECTIONS:
        section(job, rules, findings)
    return _not_installed(job, rules, findings.issues)


def _not_installed(job, rules, issues):
    """
    A step or output whose connection's provider has no adapter or connector installed: its checks of what that
    adapter would accept (read formats, keys) are noise, so they give way to one issue that says what to install.
    """
    connections = connections_of(job)
    users = {}                                       # connection -> the steps and outputs that use it
    for section in ("steps", "outputs"):
        for index, item in named_items(job, section):
            connection = connections.get(item.get("connection"))
            if connection is not None and not rules.provider_installed(connection["provider"]):
                users.setdefault(item["connection"], []).append("%s[%d]" % (section, index))
    if not users:
        return issues
    noise = [path for paths in users.values() for path in paths] + ["connections.%s" % name for name in users]
    kept = [issue for issue in issues if not any(issue.path == path or issue.path.startswith(path + ".")
                                                 for path in noise)]
    filename = issues[0].file if issues else None
    for name, paths in users.items():
        provider = connections[name]["provider"]
        kept.append(Issue(ERROR, "connector-not-installed", "provider %r of connection %r (used by %s) is not "
                          "installed: %s" % (provider, name, ", ".join(paths), install_hint(provider)), filename,
                          paths[0]))
    return kept
