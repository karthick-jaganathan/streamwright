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
Config and secret values for a job run, through core's input handling (`streamwright.core.config.inputs`).

`spec.config` and `spec.secrets` entries are typed definitions (`type`, `items`, `required`, `default`,
`description`), as in a source: values are coerced to their type, required inputs must have a value and names that
are not declared are rejected. Every engine and the MCP server use these functions, so a manifest and its flags give
the same values everywhere.
"""

from streamwright.core.config.inputs import (
    InputError,
    read_config_file,
    read_values_file,
    resolve_inputs,
    secrets_from_env,
)

__all__ = [
    "InputError",
    "declared_inputs",
    "effective_config",
    "job_inputs",
    "parse_set",
    "read_config_values",
    "secret_values",
]


def parse_set(items):
    """`--set NAME=VALUE` items as a mapping; raises InputError for an item without `=` or a name."""
    values = {}
    for item in items or ():
        name, separator, value = str(item).partition("=")
        name = name.strip()
        if not separator or not name:
            raise InputError("--set expects NAME=VALUE, got %r" % item)
        values[name] = value.strip()
    return values


def read_config_values(path):
    """The `config:` values of a --config file; a job runs all its steps, so `streams` is rejected."""
    values, streams = read_config_file(path)
    if streams is not None:
        raise InputError("%r: `streams` does not apply to a job (a job runs all its steps)" % path)
    return values


def declared_inputs(job, section):
    """The definitions of spec.<section> (config or secrets); raises InputError when they are not mappings."""
    spec = job.get("spec") or {}
    if not isinstance(spec, dict):
        raise InputError("spec must be a mapping")
    definitions = spec.get(section) or {}
    if not isinstance(definitions, dict):
        raise InputError("spec.%s must be a mapping of input names to definitions" % section)
    for name, definition in definitions.items():
        if definition is not None and not isinstance(definition, dict):
            raise InputError("spec.%s.%s must be a mapping (type, required, default, ...)" % (section, name))
    return definitions


def effective_config(job, provided=None):
    """spec.config resolved with the `provided` values: defaults, required checks, types, unknown names."""
    return resolve_inputs(declared_inputs(job, "config"), dict(provided or {}), "config")


def secret_values(job, provided=None, environ=None):
    """
    spec.secrets resolved from `provided` values (a --secrets file) and STREAMWRIGHT_SECRET_<NAME> variables (a
    provided value wins). Messages never include values.
    """
    declared = declared_inputs(job, "secrets")
    values = dict(provided or {})
    for name, value in secrets_from_env(list(declared), environ).items():
        values.setdefault(name, value)
    return resolve_inputs(declared, values, "secrets", hide_values=True)


def job_inputs(job, config_file=None, set_items=(), secrets_file=None, *, environ=None, warn=None):
    """(config, secrets) for a run: --config FILE, --set items, --secrets FILE and the environment."""
    provided = read_config_values(config_file) if config_file else {}
    provided.update(parse_set(set_items))
    secrets = read_values_file(secrets_file, warn) if secrets_file else {}
    return effective_config(job, provided), secret_values(job, secrets, environ)
