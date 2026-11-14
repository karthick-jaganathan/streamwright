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

"""Table sw_jobs: Execution history, status machine, checkpoint, tokens, and metrics."""

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
)

from streamwright.core.jobs.state.tables.base import (
    JSON_TYPE,
    KEY_LEN,
    STATUS_LEN,
    created_at_column,
    metadata,
    updated_at_column,
)

__all__ = ["sw_jobs"]

sw_jobs = Table(
    "sw_jobs",
    metadata,
    Column("job_name", String(KEY_LEN), nullable=False, comment="Pipeline (job manifest) name."),
    Column("job_id", String(KEY_LEN), nullable=False, comment="Unique run (batch) identifier."),
    Column("scope", String(KEY_LEN), nullable=False, server_default="",
           comment="State scope (runtime.state.scope) the run belongs to; empty for a job without scopes."),
    Column("sequence", BigInteger, nullable=False,
           comment="Run number within the job and scope; increases and is never reused (Delta txnVersion)."),
    Column("job_key", String(KEY_LEN), nullable=True,
           comment="Optional idempotency key (runtime.state.job_key, --job-key), unique per job and scope."),
    Column("status", String(STATUS_LEN), nullable=False,
           comment="Lifecycle state: received, processing, committed, acked, completed, failed, abandoned, skipped."),
    Column("tokens", JSON_TYPE, nullable=True,
           comment="Lock tokens of the run: {accepted: [...], skipped: [...], rejected: [...]}."),
    Column("checkpoint", JSON_TYPE, nullable=False,
           comment="Run manifest: pinned inputs and values, carried bookmarks and broker messages."),
    Column("failed_from", String(STATUS_LEN), nullable=True,
           comment="Status the run failed in; a retry resumes from it."),
    Column("started_at", DateTime(timezone=True), nullable=False, comment="When the run was received."),
    Column("finished_at", DateTime(timezone=True), nullable=True,
           comment="When the run reached a terminal status (completed, abandoned, skipped)."),
    Column("duration_ms", BigInteger, nullable=True, comment="Milliseconds from started_at to finished_at."),
    Column("rows_written", BigInteger, nullable=False, server_default="0",
           comment="Rows written across all outputs (the sum of the run's receipts)."),
    Column("error_message", Text, nullable=True, comment="The redacted error of a failed run."),
    Column("metadata", JSON_TYPE, nullable=True, comment="Engine details (e.g. Spark application id, driver host)."),
    Column("attempts", Integer, nullable=False, server_default="1",
           comment="How many times the run was started: 1, plus one per retry (state retry, or a run's automatic "
                   "retry under runtime.state.failed_job_policy: retry)."),
    created_at_column(),
    updated_at_column(),
    PrimaryKeyConstraint("job_name", "job_id", name="pk_sw_jobs"),
    UniqueConstraint("job_name", "scope", "job_key", name="uq_sw_jobs__name_scope_key"),
    Index("ix_sw_jobs__name_scope_status", "job_name", "scope", "status"),
    comment="Run history: lifecycle, pinned manifest, lock tokens and metrics, per job and scope.",
)
