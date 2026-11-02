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
Credential-free validation of job manifests (`validate`, `plan`, the MCP server): config values are resolved,
secret references are checked against spec.secrets but never resolved, and every finding is a
`streamwright.core.validation.Issue`.
"""

import copy
import os
from collections.abc import Mapping
from dataclasses import dataclass, field

from streamwright.core.config.inputs import InputError, resolve_inputs
from streamwright.core.jobs.check import check_job
from streamwright.core.jobs.inputs import declared_inputs
from streamwright.core.jobs.manifest import JobError, load_job, load_job_text, merge_overlay
from streamwright.core.jobs.plan import PlanError, build_plan
from streamwright.core.jobs.refs import input_references, resolve
from streamwright.core.validation import Issue
from streamwright.core.validation.checker import ERROR, NON_STRICT_CODES, WARNING

__all__ = ["JobReport", "resolve_for_checks", "validate_job", "validate_job_text"]


@dataclass
class JobReport:
    """The verdict on one manifest."""
    valid: bool
    issues: list[Issue] = field(default_factory=list)
    job: dict | None = None                   # config resolved, secret references kept; None if unreadable
    plan: str | None = None                   # the plan's description, when valid

    @property
    def problems(self):
        return [issue for issue in self.issues if issue.severity == ERROR]

    @property
    def warnings(self):
        return [issue for issue in self.issues if issue.severity == WARNING]

    def to_dict(self):
        return {"valid": self.valid, "issues": [issue.to_dict() for issue in self.issues], "job": self.job,
                "plan": self.plan}


def resolve_for_checks(raw, config=None):
    """
    (job, problems): spec.config resolved with `config`. A required config input without a value stays a
    `{{ config.* }}` reference, as every `{{ secrets.* }}` reference does: a run supplies it; the values given are
    type-checked.
    """
    declared = declared_inputs(raw, "config")
    values = resolve_inputs({name: dict(definition or {}, required=False) for name, definition in declared.items()},
                            dict(config or {}), "config")
    values.update({name: "{{ config.%s }}" % name for name, definition in declared.items()
                   if values[name] is None and (definition or {}).get("required", True)})
    kept = {name: "{{ secrets.%s }}" % name for name in input_references(raw, "secrets")}
    return resolve(raw, values, kept)


def _report(read, rules_for, config, strict, filename):
    try:
        raw = read()
    except JobError as exc:
        return JobReport(False, [Issue(ERROR, "unreadable", str(exc), filename)])
    try:
        job, missing = resolve_for_checks(raw, config)
    except InputError as exc:
        return JobReport(False, [Issue(ERROR, "bad-input", str(exc), filename, "spec.config")], raw)
    rules = rules_for(job)
    issues = [Issue(ERROR, "missing-input", text, filename) for text in missing] + check_job(job, rules, filename)
    if not strict:
        for issue in issues:
            if issue.severity == ERROR and issue.code in NON_STRICT_CODES:
                issue.severity = WARNING
    errors = [i for i in issues if i.severity == ERROR or (strict and i.severity == WARNING)]
    report = JobReport(not errors, issues, job)
    if report.valid:
        try:
            report.plan = build_plan(job, rules.dependencies_of).describe()
        except PlanError as exc:
            report.issues.append(Issue(ERROR, "dependency", str(exc), filename))
            report.valid = False
    return report


def _from_mapping_or_path(job, overlays):
    if isinstance(job, Mapping):
        loaded = copy.deepcopy(dict(job))
        for overlay in overlays:
            loaded = merge_overlay(loaded, load_job(overlay))
        return loaded
    if isinstance(job, (str, os.PathLike)):
        return load_job(job, overlays=overlays)
    raise JobError("a job is a mapping or a path, not %s (YAML text: validate_job_text)" % type(job).__name__)


def validate_job(job, rules_for, overlays=(), config=None, strict=False):
    """
    Validates a job given as a mapping or a path (job.yaml or its folder) without secret values.
    `rules_for(job)` returns the engine's JobRules; `config`: values for spec.config; `strict`: warnings and
    missing connectors fail too.
    """
    filename = str(job) if isinstance(job, (str, os.PathLike)) else None
    return _report(lambda: _from_mapping_or_path(job, list(overlays)), rules_for, config, strict, filename)


def validate_job_text(text, rules_for, overlays=(), config=None, strict=False):
    """validate_job() for a job given as YAML text (overlays are still locations)."""
    return _report(lambda: load_job_text(text, overlays=list(overlays)), rules_for, config, strict, None)
