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
The engine-neutral framework for `kind: *Job` manifests (DuckDBJob here; SparkJob in streamwright-spark): reading
and overlaying manifests, inputs, checking and the generated JSON schema, the dependency plan, the bookkeeping state
store, the batch Runner with its Executor protocol, ingest planning, write rules and file naming.
"""

from streamwright.core.jobs.check import JobRules, check_job
from streamwright.core.jobs.ingest import FileReject, Group, IngestError, SchemaReport, plan_ingest
from streamwright.core.jobs.inputs import (
                                           InputError,
                                           effective_config,
                                           job_inputs,
                                           parse_set,
                                           read_config_values,
                                           secret_values,
)
from streamwright.core.jobs.manifest import JobError, load_job, load_job_text, merge_overlay
from streamwright.core.jobs.naming import NamingError, parse_template, plan_names
from streamwright.core.jobs.plan import Plan, PlanError, build_plan, sql_table_reads
from streamwright.core.jobs.runner import BrokerCall, RelayError, Runner, RunnerError
from streamwright.core.jobs.schema import json_schema
from streamwright.core.jobs.state import (
                                           LeaseHeldError,
                                           SqlStateStore,
                                           StaleLeaseError,
                                           StateError,
                                           StateStore,
                                           open_sqlite,
                                           open_store,
)
from streamwright.core.jobs.types import (
                                           BatchContext,
                                           Broker,
                                           ExecutionHooks,
                                           ExecutionResult,
                                           Executor,
                                           Lease,
                                           Message,
                                           WriteReceipt,
)
from streamwright.core.jobs.validate import JobReport, validate_job, validate_job_text
from streamwright.core.jobs.write_rules import OutputSpec, WriteTarget, check_output

__all__ = [
                                           "BatchContext",
                                           "Broker",
                                           "BrokerCall",
                                           "ExecutionHooks",
                                           "ExecutionResult",
                                           "Executor",
                                           "FileReject",
                                           "Group",
                                           "IngestError",
                                           "InputError",
                                           "JobError",
                                           "JobReport",
                                           "JobRules",
                                           "Lease",
                                           "LeaseHeldError",
                                           "Message",
                                           "NamingError",
                                           "OutputSpec",
                                           "Plan",
                                           "PlanError",
                                           "RelayError",
                                           "Runner",
                                           "RunnerError",
                                           "SchemaReport",
                                           "SqlStateStore",
                                           "StaleLeaseError",
                                           "StateError",
                                           "StateStore",
                                           "WriteReceipt",
                                           "WriteTarget",
                                           "build_plan",
                                           "check_job",
                                           "check_output",
                                           "effective_config",
                                           "job_inputs",
                                           "json_schema",
                                           "load_job",
                                           "load_job_text",
                                           "merge_overlay",
                                           "open_sqlite",
                                           "open_store",
                                           "parse_set",
                                           "parse_template",
                                           "plan_ingest",
                                           "plan_names",
                                           "read_config_values",
                                           "secret_values",
                                           "sql_table_reads",
                                           "validate_job",
                                           "validate_job_text",
]
