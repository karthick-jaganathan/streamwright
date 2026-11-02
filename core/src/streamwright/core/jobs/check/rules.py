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

"""What an engine tells the shared checker (and the schema generator) about its manifests."""

import re
from types import MappingProxyType

from streamwright.core.jobs.fields import Field
from streamwright.core.jobs.plan import item_dependencies
from streamwright.core.validation import vocabulary as source_spec

__all__ = ["INPUT_NAME_PATTERN", "NAME_PATTERN", "RESERVED_NAMES", "JobRules", "input_field"]

NAME_PATTERN = r"^[a-z][a-z0-9_]{0,62}$"
INPUT_NAME_PATTERN = source_spec.NAME_PATTERN
RESERVED_NAMES = frozenset(["input", "batch", "config", "secrets", "runtime", "driver", "_commits", "_file_errors",
                            "_schema_report", "_streamwright_state"])


def input_field():
    """The rule of one spec.config / spec.secrets definition: core's source input rules."""
    return Field("object", fields={
        "type": Field("string", enum=source_spec.INPUT_TYPES),
        "items": Field("string", enum=source_spec.LIST_ITEM_TYPES),
        "required": Field("boolean"),
        "default": Field(),
        "description": Field("string"),
    })


class JobRules:
    """
    The engine-specific part of checking a job. Subclasses set `kind` and the rule tables and override the hooks.
      kind                 the manifest kind (e.g. DuckDBJob)
      builtin_references   `{{ name }}` constants the engine renders (e.g. batch.id)
      step_reference_keys  item keys that may hold `{{ step.column }}` references
      source_reference_keys  step keys passed to the source engine, which renders their other references
      step_kinds           {step kind: allowed step keys}
    """
    kind = None
    reserved_names = RESERVED_NAMES
    builtin_references = frozenset()
    step_reference_keys = frozenset()      # item keys whose {{ step.column }} references the engine renders
    source_reference_keys = frozenset()    # step keys whose other references the source engine renders
    step_kinds = MappingProxyType({})

    @property
    def engine(self):
        """The engine key used in messages and capabilities (`DuckDBJob` -> `duckdb`), per contract §6a."""
        return (self.kind or "").removesuffix("Job").lower()

    def manifest_field(self):
        """The Field table of the whole manifest (structure checks and the JSON schema)."""
        raise NotImplementedError

    def classify_step(self, step, connections):
        """(step kind, None) or (None, problem message)."""
        raise NotImplementedError

    def source_reference_keys_of(self, step, connections):
        """The keys of a step whose other references the source engine renders (default: source_reference_keys)."""
        return self.source_reference_keys

    def step_keys(self, kind, step, connections):
        """The keys a step of `kind` may use (its connection's adapter may narrow them)."""
        return self.step_kinds[kind]

    def check_step(self, kind, step, connections):
        """Engine problems of one step: [(code, message, is_warning)]."""
        return []

    def step_policy_keys(self, kind):
        """Execution-policy keys (contract v1.2 §6a) honoured on a step of `kind`; others name the engine."""
        return ()

    def output_policy_keys(self):
        """Execution-policy keys honoured on an output; others are an `unsupported-argument` error."""
        return ()

    def check_connection(self, name, connection):
        """Problems of one connection, asked of its adapter: [(code, message, is_warning)]."""
        return []

    def write_target(self, connection):
        """The WriteTarget for outputs through `connection`, or None when it cannot be written to."""
        return

    def secret_keys(self, connection):
        """Connection keys whose values must be `{{ secrets.NAME }}` references."""
        return ()

    def check_state(self, state, connections):
        """Problems of runtime.state: [(code, message, is_warning)]."""
        return []

    def check_engine(self, job):
        """Engine-only rules no shared section covers (e.g. platform settings): [(code, path, message,
        is_warning)], `path` a tuple of keys and indexes."""
        return []

    def dependencies_of(self, item):
        """What a step or output reads (plan edges)."""
        return item_dependencies(item)

    @staticmethod
    def name_problem(name):
        if not isinstance(name, str) or not re.match(NAME_PATTERN, name):
            return "%r is not a valid name (lower case letters, digits and _, starting with a letter)" % (name,)
        return None
