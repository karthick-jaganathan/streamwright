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

"""Table sw_leases: Distributed fencing locks, resource slices, and worker heartbeats."""

from sqlalchemy import BigInteger, Column, DateTime, PrimaryKeyConstraint, String, Table

from streamwright.core.jobs.state.tables.base import KEY_LEN, created_at_column, metadata, updated_at_column

__all__ = ["sw_leases"]

sw_leases = Table(
    "sw_leases",
    metadata,
    Column("job_name", String(KEY_LEN), nullable=False, comment="Unique pipeline or job identifier."),
    Column("token", String(KEY_LEN), nullable=False, server_default="",
           comment="The scope token (scope:<scope>, empty without one) or a lock token."),
    Column("worker_id", String(KEY_LEN), nullable=False,
           comment="Identifier of the active worker instance holding the lease."),
    Column("epoch", BigInteger, nullable=False,
           comment="Monotonically increasing fencing token to prevent split-brain writes."),
    Column("acquired_at", DateTime(timezone=True), nullable=False,
           comment="Timestamp when the lease was acquired or last renewed."),
    Column("expires_at", DateTime(timezone=True), nullable=True,
           comment="Expiration timestamp after which the lease may be acquired by another worker."),
    created_at_column(),
    updated_at_column(),
    PrimaryKeyConstraint("job_name", "token", name="pk_sw_leases"),
    comment="Distributed concurrency leases, partition slices, and worker heartbeats.",
)
