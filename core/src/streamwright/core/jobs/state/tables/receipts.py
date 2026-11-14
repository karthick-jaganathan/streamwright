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

"""Table sw_receipts: Sink write commit receipts for idempotency and replay safety."""

from sqlalchemy import BigInteger, Column, DateTime, PrimaryKeyConstraint, String, Table, Text

from streamwright.core.jobs.state.tables.base import JSON_TYPE, KEY_LEN, created_at_column, metadata, updated_at_column

__all__ = ["sw_receipts"]

sw_receipts = Table(
    "sw_receipts",
    metadata,
    Column("job_name", String(KEY_LEN), nullable=False, comment="Unique pipeline or job identifier."),
    Column("job_id", String(KEY_LEN), nullable=False,
           comment="Execution run identifier that generated this write receipt."),
    Column("target", String(KEY_LEN), nullable=False, comment="Logical name of the output destination."),
    Column("rows_written", BigInteger, nullable=True, comment="Number of records committed to this destination."),
    Column("destination", Text, nullable=False,
           comment="Physical destination table name, file path, or object storage URI."),
    Column("files", JSON_TYPE, nullable=True, comment="JSON list of written file paths or storage object keys."),
    Column("details", JSON_TYPE, nullable=True,
           comment="Detailed commit metadata including table versions, merge predicates, and metrics."),
    Column("committed_at", DateTime(timezone=True), nullable=False,
           comment="Timestamp when the output write transaction was committed."),
    created_at_column(),
    updated_at_column(),
    PrimaryKeyConstraint("job_name", "job_id", "target", name="pk_sw_receipts"),
    comment="Write receipts and commit metadata for job output destinations.",
)
