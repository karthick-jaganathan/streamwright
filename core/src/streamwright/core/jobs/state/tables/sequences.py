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

"""Table sw_sequences: the run counter of each job and scope (never lowered, even when runs are purged)."""

from sqlalchemy import BigInteger, Column, PrimaryKeyConstraint, String, Table

from streamwright.core.jobs.state.tables.base import KEY_LEN, created_at_column, metadata, updated_at_column

__all__ = ["sw_sequences"]

sw_sequences = Table(
    "sw_sequences",
    metadata,
    Column("job_name", String(KEY_LEN), nullable=False, comment="Pipeline (job manifest) name."),
    Column("scope", String(KEY_LEN), nullable=False, server_default="", comment="State scope; empty for none."),
    Column("last_sequence", BigInteger, nullable=False, comment="The highest run sequence issued so far."),
    created_at_column(),
    updated_at_column(),
    PrimaryKeyConstraint("job_name", "scope", name="pk_sw_sequences"),
    comment="Run counters per job and scope; Delta transaction versions come from them.",
)
