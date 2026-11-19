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
from streamwright.core.jobs.check.structure import check_structure
from streamwright.core.jobs.fields import path_text
from streamwright.core.jobs.grammar import grammar_problems, to_internal
from streamwright.core.runtime.issues import Issue
from streamwright.core.validation.checker import ERROR

__all__ = ["SECTIONS", "JobRules", "check_job", "install_hint"]

_KEY_CODES = ("unknown-key", "unsupported-key")


def check_job(job, rules, filename=None):
    """
    Every problem and warning of `job` (a public manifest, config resolved) as Issue objects: its structure as
    written (unknown keys refused; streamwright.core.jobs.grammar checks the operations), then
    every other section on the engines' internal shape, with the paths mapped back to the manifest.
    """
    findings = Findings(filename)
    if not isinstance(job, dict):
        findings.error("bad-type", (), "the job must be a mapping")
        return findings.issues
    problems = grammar_problems(job)
    structure = Findings(filename)
    check_structure(job, rules, structure)
    replaced = {path_text(path) for _, path, _ in problems}
    mixed = {path_text(path) for code, path, _ in problems if code == "bad-value" and path[-1] == "operation"}
    findings.issues = [issue for issue in structure.issues
                       if issue.path not in replaced and not _under(issue.path, mixed)]
    for code, path, message in problems:
        findings.error(code, path, message)
    internal = to_internal(job)
    sections = Findings(filename, public=job)
    for section in SECTIONS:
        if section is not check_structure:
            section(internal, rules, sections)
    # a key the structure refused is reported once: the internal sections' findings drop it, and every finding at
    # or under a form the grammar refused (fixing that form comes first)
    refused = {issue.path for issue in findings.issues if issue.code == "unknown-key"}
    return _not_installed(internal, rules, findings.issues + [
        issue for issue in sections.issues
        if not (issue.path in refused and issue.code in _KEY_CODES) and not _under(issue.path, replaced)])


def _under(path, paths):
    return any(path == other or path.startswith((other + ".", other + "[")) for other in paths)


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
