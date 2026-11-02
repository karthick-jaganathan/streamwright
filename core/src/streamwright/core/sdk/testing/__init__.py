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
Certification suites and test helpers for connectors: the first-party connectors pass them, and so can a third-party
connector built on streamwright.core.sdk. Needs pytest.

- certify(CONNECTOR): the spec well formed, its JSON Schema, secret fields enforced and redacted, each engine's
  arguments; SourceAdapterContract, DuckDBAdapterContract: the engine adapters (streamwright.core.sdk.testing.contract).
  A connector's test module subclasses a suite and sets its class attributes (pytest collects the `Test*` subclass).

Helpers for connector tests: FakeApi, MemoryOutput, FakeClock, page_stream, SourceRunner, SourceError, load_source,
render, resolve_inputs, RunMetrics, validate_text, registered(), run_cli(), and the DuckDB-job entry points of
the DuckDBJob engine (run_job, JobInvalidError).
"""

from streamwright.core import cli
from streamwright.core.config.inputs import resolve_inputs
from streamwright.core.config.loader import load_source
from streamwright.core.engine.runner import SourceError, SourceRunner
from streamwright.core.jobs import OutputSpec, check_output
from streamwright.core.jobs.check import check_job
from streamwright.core.jobs.duckdb import JobInvalidError, Session, run_job
from streamwright.core.jobs.duckdb.rules import DuckDBRules
from streamwright.core.jobs.state import SqlStateStore, open_sqlite
from streamwright.core.outputs.output import open_output
from streamwright.core.runtime import components, plugins
from streamwright.core.runtime.logs import RunMetrics
from streamwright.core.runtime.templates import render
from streamwright.core.runtime.testing import FakeApi, FakeClock, MemoryOutput, page_stream
from streamwright.core.sdk.testing.contract import DuckDBAdapterContract, SourceAdapterContract, certify
from streamwright.core.sdk.testing.helpers import extension_installed, registered, run_cli
from streamwright.core.sdk.testing.storage import Listener, ObjectStorage, patch_object_storage
from streamwright.core.validation.engine import ERROR, validate_text

__all__ = [
           "ERROR",
           "DuckDBAdapterContract",
           "DuckDBRules",
           "FakeApi",
           "FakeClock",
           "JobInvalidError",
           "Listener",
           "MemoryOutput",
           "ObjectStorage",
           "OutputSpec",
           "RunMetrics",
           "Session",
           "SourceAdapterContract",
           "SourceError",
           "SourceRunner",
           "SqlStateStore",
           "certify",
           "check_job",
           "check_output",
           "cli",
           "components",
           "extension_installed",
           "load_source",
           "open_output",
           "open_sqlite",
           "page_stream",
           "patch_object_storage",
           "plugins",
           "registered",
           "render",
           "resolve_inputs",
           "run_cli",
           "run_job",
           "validate_text",
]
