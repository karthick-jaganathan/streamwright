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
Section: secrets (decision D8). `{{ secrets.NAME }}` must be declared in spec.secrets and may appear only in the
credential keys a connection's adapter declares (`rules.secret_keys`) and in runtime.env.STREAMWRIGHT_SECRET_*;
those credential keys must hold such a reference, never a literal. Every engine shares this rule.
"""

import re

from streamwright.core.config.inputs import SECRET_ENV_PREFIX
from streamwright.core.jobs.check.findings import connections_of
from streamwright.core.jobs.refs import references

__all__ = ["check_secrets"]

_SECRET_REF = re.compile(r"^\{\{\s*secrets\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}$")


def _declared(job):
    spec = job.get("spec")
    secrets = spec.get("secrets") if isinstance(spec, dict) else None
    return set(secrets) if isinstance(secrets, dict) else set()


def _credential_keys(job, rules):
    """{connection name: the keys its adapter declares as credentials}."""
    return {name: set(rules.secret_keys(conn)) for name, conn in connections_of(job).items()}


def _allowed_place(path, credential_keys):
    in_connection = (len(path) >= 3 and path[0] == "connections" and "<key>" not in path
                     and path[2] in credential_keys.get(path[1], ()))
    in_env = len(path) == 3 and path[:2] == ["runtime", "env"] and str(path[2]).startswith(SECRET_ENV_PREFIX)
    return in_connection or in_env


def check_secrets(job, rules, findings):
    declared = _declared(job)
    credential_keys = _credential_keys(job, rules)

    def walk(node, path):
        if isinstance(node, dict):
            for key, value in node.items():
                walk(key, [*path, "<key>"])
                walk(value, [*path, key])
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, [*path, index])
        elif isinstance(node, str):
            for expr in references(node):
                if not expr.startswith("secrets."):
                    continue
                where = tuple(p for p in path if p != "<key>")
                hit = _SECRET_REF.match("{{ %s }}" % expr)
                if not hit or hit.group(1) not in declared:
                    findings.error("undeclared-secret", where, "secret %r is not declared in spec.secrets" % expr)
                if not _allowed_place(path, credential_keys):
                    findings.error("secret-placement", where, "{{ secrets.* }} is only allowed in a connection's "
                                   "declared credential keys (and runtime.env.%s*); pass values to SQL as "
                                   "parameters" % SECRET_ENV_PREFIX)
    walk({k: v for k, v in job.items() if k != "spec"}, [])
    for name, conn in connections_of(job).items():
        for key in credential_keys[name]:
            value = conn.get(key)
            if value is not None and not (isinstance(value, str) and _SECRET_REF.match(value.strip())):
                findings.error("literal-secret", ("connections", name, key), "must be a {{ secrets.NAME }} "
                               "reference, not a literal")
