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
"""Table sw_requests: the queue of `streamwright serve`: each row is one job run a caller requested over HTTP."""

from sqlalchemy import Column, DateTime, Index, Integer, PrimaryKeyConstraint, String, Table, Text, UniqueConstraint

from streamwright.core.jobs.state.tables.base import (
    JSON_TYPE,
    KEY_LEN,
    STATUS_LEN,
    created_at_column,
    metadata,
    updated_at_column,
)

__all__ = ["sw_requests"]

sw_requests = Table(
    "sw_requests",
    metadata,
    Column("run_id", String(KEY_LEN), nullable=False, comment="The run's id (GET /v1/runs/{run_id})."),
    Column("job_name", String(KEY_LEN), nullable=False, comment="The job as serve.yaml names it."),
    Column("principal", String(KEY_LEN), nullable=False, comment="The caller (an API key id or a JWT subject)."),
    Column("idempotency_key", String(KEY_LEN), nullable=True,
           comment="The caller's Idempotency-Key, unique per job and principal."),
    Column("body_sha256", String(64), nullable=False, comment="SHA-256 of the canonical request body."),
    Column("scope_key", String(KEY_LEN), nullable=False, server_default="",
           comment="What the run works on (its rendered state scope): one active run per job and scope key."),
    Column("status", String(STATUS_LEN), nullable=False, comment="queued, running, succeeded or failed."),
    Column("inputs", JSON_TYPE, nullable=False, comment="The run's config values (--set), bound and checked."),
    Column("payload", JSON_TYPE, nullable=True,
           comment="The run's input rows (--input); removed when the run ends."),
    Column("request_id", String(KEY_LEN), nullable=False, comment="The request id of the run's log lines."),
    Column("attempts", Integer, nullable=False, server_default="0", comment="How many times a worker claimed it."),
    Column("worker_id", String(KEY_LEN), nullable=True, comment="The worker running it."),
    Column("claim_expires_at", DateTime(timezone=True), nullable=True,
           comment="The worker's claim ends then unless renewed: another worker may take the run over."),
    Column("result", JSON_TYPE, nullable=True, comment="The run summary (as --summary writes it)."),
    Column("error_code", String(KEY_LEN), nullable=True, comment="Why the run failed, as a code."),
    Column("error_message", Text, nullable=True, comment="The failure, secrets redacted."),
    Column("started_at", DateTime(timezone=True), nullable=True, comment="When a worker first claimed it."),
    Column("finished_at", DateTime(timezone=True), nullable=True, comment="When it ended."),
    created_at_column(),
    updated_at_column(),
    PrimaryKeyConstraint("run_id", name="pk_sw_requests"),
    UniqueConstraint("job_name", "principal", "idempotency_key", name="uq_sw_requests__idempotency"),
    Index("ix_sw_requests__status_created", "status", "created_at"),
    Index("ix_sw_requests__job_scope_status", "job_name", "scope_key", "status"),
    Index("ix_sw_requests__principal_job_created", "principal", "job_name", "created_at"),   # limits.rate
    Index("ix_sw_requests__finished", "finished_at"),                                       # metrics, purge
    comment="Runs requested over HTTP (streamwright serve), claimed and run by workers (streamwright worker).",
)
