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

"""Table sw_events: Transactional outbox pattern message queue for guaranteed dispatching."""

from sqlalchemy import BigInteger, Column, DateTime, Index, PrimaryKeyConstraint, String, Table, Text

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
    Column("status", String(STATUS_LEN), nullable=False, comment="Dispatch status: pending, sent, or failed."),
    Column("sent_at", DateTime(timezone=True), nullable=True,
           comment="Timestamp when the event was confirmed delivered to the target destination."),
    created_at_column(),
    updated_at_column(),
    PrimaryKeyConstraint("job_name", "job_id", "target", "sequence", name="pk_sw_events"),
    Index("ix_sw_events__name_status", "job_name", "status"),
    comment="Transactional outbox for reliable event and notification dispatching.",
)
