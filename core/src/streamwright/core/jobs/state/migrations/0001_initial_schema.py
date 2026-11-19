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

"""Migration 0001: the sw_* state tables, created with their indexes and comments."""

import uuid

from streamwright.core.jobs.state.migrations import ddl
from streamwright.core.jobs.state.tables import get_tables

VERSION = 1
NAME = "0001_initial_schema"
DESCRIPTION = ("Create the state tables (sw_leases, sw_jobs, sw_sequences, sw_receipts, sw_events, sw_signatures, "
               "sw_requests, sw_migrations)")

TABLE_ORDER = ("sw_migrations", "sw_leases", "sw_jobs", "sw_sequences", "sw_receipts", "sw_events", "sw_signatures",
               "sw_requests")


def get_statements(dialect_name, schema=None):
    """The ordered DDL statements of this migration."""
    tables = get_tables(schema=schema)
    return [statement for name in TABLE_ORDER for statement in ddl.create_statements(dialect_name, tables[name])]


def apply(conn, dialect, schema=None):
    """Creates the tables; returns the new store's identity."""
    ddl.execute(conn, dialect, get_statements(dialect.name, schema=schema))
    return str(uuid.uuid4())
