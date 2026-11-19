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

"""StreamWright modular state tables (SQLAlchemy Core)."""

from sqlalchemy import MetaData, Table

from streamwright.core.jobs.state.tables.base import (
    DEFAULT_SCHEMA,
    KEY_LEN,
    NAMING_CONVENTION,
    PATH_LEN,
    STATUS_LEN,
    metadata,
    sqlalchemy_dialect,
)
from streamwright.core.jobs.state.tables.events import sw_events
from streamwright.core.jobs.state.tables.jobs import sw_jobs
from streamwright.core.jobs.state.tables.leases import sw_leases
from streamwright.core.jobs.state.tables.migrations import sw_migrations
from streamwright.core.jobs.state.tables.receipts import sw_receipts
from streamwright.core.jobs.state.tables.requests import sw_requests
from streamwright.core.jobs.state.tables.sequences import sw_sequences
from streamwright.core.jobs.state.tables.signatures import sw_signatures


def get_tables(schema: str | None = None) -> dict[str, Table]:
    """Return all StreamWright state tables bound to the optional schema."""
    if schema is None:
        return {table.name: table for table in metadata.sorted_tables}
    target_meta = MetaData(naming_convention=dict(NAMING_CONVENTION))
    return {table.name: table.to_metadata(target_meta, schema=schema) for table in metadata.sorted_tables}


__all__ = [
    "DEFAULT_SCHEMA",
    "KEY_LEN",
    "NAMING_CONVENTION",
    "PATH_LEN",
    "STATUS_LEN",
    "get_tables",
    "metadata",
    "sqlalchemy_dialect",
    "sw_events",
    "sw_jobs",
    "sw_leases",
    "sw_migrations",
    "sw_receipts",
    "sw_requests",
    "sw_sequences",
    "sw_signatures",
]
