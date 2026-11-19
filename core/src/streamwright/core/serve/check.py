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
`streamwright validate` of a serve.yaml: what can be checked without the service's environment - its structure, each
job entry (name, bindings, limits, principals) and each manifest it lists, through serve's own rules (what a request
may set, the SQL and path rules). The environment's values (API key digests, secrets) are the start's to check.
"""

from streamwright.core.serve.config import ServiceError, load_service
from streamwright.core.serve.rules import served_job

__all__ = ["is_serve_config", "validate_service"]


def is_serve_config(data):
    """Whether a YAML document is a serve.yaml: no `kind`, and `jobs` maps names to entries with a `manifest`."""
    jobs = data.get("jobs") if isinstance(data, dict) and "kind" not in data else None
    return isinstance(jobs, dict) and bool(jobs) and all(isinstance(entry, dict) and "manifest" in entry
                                                         for entry in jobs.values())


def validate_service(path):
    """The problems and warnings of serve.yaml at `path`: ([message], [message])."""
    try:
        service = load_service(path, environ={}, static=True)
    except ServiceError as exc:
        return list(exc.problems), []
    problems, warnings = [], []
    for entry in service.jobs.values():
        _, found, cautions = served_job(entry)
        problems.extend(found)
        warnings.extend(cautions)
    return problems, warnings
