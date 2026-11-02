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
The `JobEngine` protocol: what a `kind: *Job` engine registers in the `streamwright.job_kinds` entry-point group.

The entry point is named by the manifest kind and targets a module-level engine OBJECT (not a class or function),
e.g. `DuckDBJob = "streamwright.core.jobs.duckdb.plugin:JOB_ENGINE"`. The core CLI and the MCP server use only this
protocol; they never probe the engine's package. The connectors an engine can use are the connector definitions
whose `adapters` name the engine (`engine_name(kind)`: `DuckDBJob` -> `duckdb`), see
`streamwright.core.runtime.definitions.engine_connectors`.
"""

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from streamwright.core.jobs.validate import JobReport
from streamwright.core.runtime import plugins

__all__ = ["JobEngine", "engine_name", "load_job_engine"]


@runtime_checkable
class JobEngine(Protocol):
    """A job engine: its manifest kind, command line, credential-free validation, schema, guide and connectors."""

    kind: str  # the manifest `kind` it runs, e.g. "DuckDBJob" (= the entry-point name)

    def main(self, argv: Sequence[str] | None = None) -> int:
        """
        Runs the engine's command line on the original arguments (`validate|plan|run JOB ...`); the exit code. An engine
        may also take `prog`, the command's name in its usage lines (`streamwright duckdb` for the explicit form).
        """

    def validate_job(self, job: str | Mapping[str, Any], *, overlays: Sequence[str] = (),
                     config: Mapping[str, Any] | None = None, strict: bool = False) -> JobReport:
        """Validates a manifest path or mapping without secret values."""

    def validate_job_text(self, text: str, *, overlays: Sequence[str] = (),
                          config: Mapping[str, Any] | None = None, strict: bool = False) -> JobReport:
        """validate_job() for manifest YAML text."""

    def json_schema(self) -> dict[str, Any]:
        """The Draft 7 JSON schema of the manifest, including the installed connectors' connection fields."""

    def guide(self, section: str | None = None) -> str:
        """The engine's markdown guide, or its `## ` section with that title (KeyError for an unknown section)."""

    def describe_connector(self, name: str) -> dict[str, Any] | None:
        """The declared contract of connector `name` for this engine, without connecting; None if unavailable."""


def engine_name(kind: str) -> str:
    """The engine a job kind runs on, as connector definitions name it in `adapters` (`DuckDBJob` -> `duckdb`)."""
    return kind.removesuffix("Job").lower()


def load_job_engine(kind: str, *, allowed: Sequence[str] | None = None,
                    entry_points: Callable[[], Any] | None = None) -> JobEngine:
    """The JobEngine registered for manifest `kind` (PluginError when missing, incompatible or not a JobEngine)."""
    engine = plugins.load_plugin(plugins.JOB_KINDS, kind, allowed=allowed, expected_type=JobEngine,
                                 entry_points=entry_points)
    if engine.kind != kind:
        raise plugins.PluginError("%s %r targets an engine of kind %r" % (plugins.JOB_KINDS, kind, engine.kind))
    return engine
