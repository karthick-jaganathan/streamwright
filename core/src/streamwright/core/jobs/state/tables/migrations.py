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

"""Table sw_migrations: Migration version history registry and persistent instance identity store."""

from sqlalchemy import BigInteger, Column, DateTime, PrimaryKeyConstraint, String, Table

from streamwright.core.jobs.state.tables.base import KEY_LEN, created_at_column, metadata, updated_at_column

__all__ = ["sw_migrations"]

sw_migrations = Table(
    "sw_migrations",
    metadata,
    Column("version", BigInteger, autoincrement=False, nullable=False,
           comment="Sequential integer migration version number."),
    Column("name", String(KEY_LEN), nullable=False, comment="Descriptive migration identifier name."),
    Column("applied_at", DateTime(timezone=True), nullable=False, comment="Timestamp when the migration was applied."),
    Column("store_id", String(KEY_LEN), nullable=False,
           comment="Persistent unique store identifier UUID for cross-environment coordination."),
    created_at_column(),
    updated_at_column(),
    PrimaryKeyConstraint("version", name="pk_sw_migrations"),
    comment="State database schema migration history and persistent store instance identity.",
)
