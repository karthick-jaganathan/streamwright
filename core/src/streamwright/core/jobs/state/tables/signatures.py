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

"""Table sw_signatures: Data contract and schema fingerprint registry for drift detection."""

from sqlalchemy import Column, PrimaryKeyConstraint, String, Table, Text

from streamwright.core.jobs.state.tables.base import KEY_LEN, PATH_LEN, created_at_column, metadata, updated_at_column

__all__ = ["sw_signatures"]

sw_signatures = Table(
    "sw_signatures",
    metadata,
    Column("job_name", String(KEY_LEN), nullable=False, comment="Unique pipeline or job identifier."),
    Column("path", String(PATH_LEN), nullable=False, comment="File path, directory URI, or storage location pattern."),
    Column("hash", String(KEY_LEN), nullable=False,
           comment="Deterministic hash of column names and structural data types."),
    Column("signature", Text, nullable=False,
           comment="Serialized schema definition including field names, data types, and nullability."),
    created_at_column(),
    updated_at_column(),
    PrimaryKeyConstraint("job_name", "path", name="pk_sw_signatures"),
    comment="Schema contracts and structural signatures for detecting schema drift in data sources.",
)
