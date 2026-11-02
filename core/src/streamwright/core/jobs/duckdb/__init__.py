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

"""The DuckDBJob engine (`kind: DuckDBJob`): the shared jobs framework run in an in-process DuckDB."""

from streamwright.core.jobs.duckdb.adapter import AdapterError, ReadContext, WriteContext, check_location
from streamwright.core.jobs.duckdb.engine import (
                                                   JobInvalidError,
                                                   RunResult,
                                                   json_schema,
                                                   run_job,
                                                   validate_job,
                                                   validate_job_text,
)
from streamwright.core.jobs.duckdb.reads import compile_file_read, quote_relation
from streamwright.core.jobs.duckdb.rules import KIND
from streamwright.core.jobs.duckdb.session import Session, SessionError
from streamwright.core.jobs.duckdb.writers import TABLE_TARGET, write_table

__all__ = [
                                                   "KIND",
                                                   "TABLE_TARGET",
                                                   "AdapterError",
                                                   "JobInvalidError",
                                                   "ReadContext",
                                                   "RunResult",
                                                   "Session",
                                                   "SessionError",
                                                   "WriteContext",
                                                   "check_location",
                                                   "compile_file_read",
                                                   "json_schema",
                                                   "quote_relation",
                                                   "run_job",
                                                   "validate_job",
                                                   "validate_job_text",
                                                   "write_table",
]
