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
runtime.log.labels: a job's own names on its log lines (`[job tenant=t1 region=eu account=a1 step=s]`), next to the
log context's fixed keys. A label's value may use only `{{ config.* }}` inputs (never secrets), and is rendered when a
run (or a runtime.fan_out pass) starts, from that run's config.
"""

from streamwright.core.config.inputs import InputError, resolve_inputs
from streamwright.core.jobs.fields import Field
from streamwright.core.jobs.inputs import declared_inputs
from streamwright.core.jobs.refs import INPUT_REF, MOUSTACHE, resolve
from streamwright.core.runtime.logs.context import CONTEXT_FIELDS, LABEL_PATTERN, MAX_LABELS, log_context

__all__ = ["LOG_FIELD", "check_log_labels", "job_log_context"]

LOG_FIELD = Field("object", fields={
    "labels": Field("object", values=Field("string"),
                    description="name -> value shown on every log line of the run (text: after the job name; JSON: "
                                "`labels`), e.g. client: \"{{ config.client_id }}\"; values use {{ config.* }} only; "
                                "at most %d" % MAX_LABELS),
}, description="what the run's log lines carry besides the log context (job, account, batch, step, output)")


def _labels(job):
    log = (job.get("runtime") or {}).get("log") if isinstance(job.get("runtime"), dict) else None
    return log.get("labels") if isinstance(log, dict) and isinstance(log.get("labels"), dict) else {}


def check_log_labels(raw):
    """Problems of runtime.log.labels, as (path, message)."""
    labels, found = _labels(raw), []
    if len(labels) > MAX_LABELS:
        found.append((("runtime", "log", "labels"), "at most %d labels" % MAX_LABELS))
    config = set((raw.get("spec") or {}).get("config") or {}) if isinstance(raw.get("spec"), dict) else set()
    for name, value in labels.items():
        path = ("runtime", "log", "labels", name)
        if not LABEL_PATTERN.match(str(name)) or name in CONTEXT_FIELDS:
            found.append((path, "a label name is lowercase letters, digits and _ (at most 32), not one of %s"
                          % ", ".join(CONTEXT_FIELDS)))
        for expr in MOUSTACHE.findall(str(value)):
            hit = INPUT_REF.match(expr)
            if not hit or hit.group(1) != "config" or hit.group(2) not in config:
                found.append((path, "{{ %s }}: a label uses only declared {{ config.* }} inputs" % expr))
    return found


def job_log_context(job, config=None):
    """The log context of a run of `job` with these config values: its name and its rendered labels."""
    labels = _labels(job)
    rendered = {}
    if labels:
        try:
            definitions = {name: dict(d or {}, required=False) for name, d in declared_inputs(job, "config").items()}
            values = resolve_inputs(definitions, {k: v for k, v in (config or {}).items() if k in definitions},
                                    "config")
        except InputError:
            values = {}                     # the run reports it; the labels just stay out
        resolved, _ = resolve(dict(labels), values, {})
        rendered = {name: value for name, value in resolved.items() if "{{" not in str(value)}
    return log_context(job=job.get("name"), labels=rendered)
