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

"""Table sw_events: the transactional outbox: publish outputs' events and apply outputs' row operations."""

from sqlalchemy import BigInteger, Column, DateTime, Index, Integer, PrimaryKeyConstraint, String, Table, Text

from streamwright.core.jobs.state.tables.base import (
    JSON_TYPE,
    KEY_LEN,
    STATUS_LEN,
    created_at_column,
    metadata,
    updated_at_column,
)

__all__ = ["sw_events"]

sw_events = Table(
    "sw_events",
    metadata,
    Column("job_name", String(KEY_LEN), nullable=False, comment="Unique pipeline or job identifier."),
    Column("job_id", String(KEY_LEN), nullable=False, comment="Execution run identifier that produced this event."),
    Column("target", String(KEY_LEN), nullable=False, comment="Logical output destination that published the event."),
    Column("sequence", BigInteger, nullable=False, comment="Ordered event sequence number within the execution run."),
    Column("destination", Text, nullable=False, comment="Target message topic, queue, or endpoint address."),
    Column("payload", JSON_TYPE, nullable=False, comment="Serialized JSON event message body to be dispatched."),
    Column("status", String(STATUS_LEN), nullable=False,
           comment="Dispatch status: pending, sent (an event), in_flight, applied or failed (an apply row)."),
    Column("sent_at", DateTime(timezone=True), nullable=True,
           comment="Timestamp when the event was confirmed delivered to the target destination."),
    Column("attempts", Integer, nullable=False, server_default="0",
           comment="Apply rows: how many calls carried the row (a call is recorded before it is made)."),
    Column("result", JSON_TYPE, nullable=True, comment="Apply rows: what the vendor returned for the row."),
    Column("error_code", String(KEY_LEN), nullable=True, comment="Apply rows: the vendor's error code of a failure."),
    Column("error_message", Text, nullable=True, comment="Apply rows: the failure, secrets redacted."),
    created_at_column(),
    updated_at_column(),
    PrimaryKeyConstraint("job_name", "job_id", "target", "sequence", name="pk_sw_events"),
    Index("ix_sw_events__name_status", "job_name", "status"),
    comment="Transactional outbox: events to publish and row operations to apply, with their outcomes.",
)
