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

"""Shared SQLAlchemy MetaData, naming conventions, and column primitives for state tables."""

from types import MappingProxyType

from sqlalchemy import JSON, Column, DateTime, MetaData
from sqlalchemy.dialects.mysql import dialect as mysql_dialect
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import dialect as postgres_dialect
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect

__all__ = [
    "DEFAULT_SCHEMA",
    "JSON_TYPE",
    "KEY_LEN",
    "NAMING_CONVENTION",
    "PATH_LEN",
    "STATUS_LEN",
    "created_at_column",
    "metadata",
    "sqlalchemy_dialect",
    "updated_at_column",
]

DEFAULT_SCHEMA = "streamwright"
KEY_LEN = 191
PATH_LEN = 500  # (job_name, path) is a key: within InnoDB's 3072 bytes in utf8mb4
STATUS_LEN = 50
JSON_TYPE = JSON().with_variant(JSONB, "postgresql")

NAMING_CONVENTION = MappingProxyType({
    "ix": "ix_%(table_name)s__%(column_0_name)s",
    "uq": "uq_%(table_name)s__%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
})

metadata = MetaData(naming_convention=dict(NAMING_CONVENTION))


def created_at_column() -> Column:
    """Returns a new created_at audit column."""
    return Column("created_at", DateTime(timezone=True), nullable=False,
           comment="Timestamp when the record was created.")


def updated_at_column() -> Column:
    """Returns a new updated_at audit column."""
    return Column("updated_at", DateTime(timezone=True), nullable=False,
           comment="Timestamp when the record was last updated.")


class _DuckDBDialect(postgres_dialect):
    """DuckDB speaks PostgreSQL's DDL with qmark parameters."""
    name = "duckdb"
    paramstyle = "qmark"


def sqlalchemy_dialect(name):
    """The SQLAlchemy compiler of a state dialect (for the migrations' DDL)."""
    if name == "sqlite":
        return sqlite_dialect(paramstyle="qmark")
    if name == "postgres":
        return postgres_dialect(paramstyle="pyformat")
    if name == "mysql":
        return mysql_dialect(paramstyle="format")
    if name == "duckdb":
        return _DuckDBDialect()
    raise ValueError("unsupported state dialect %r" % (name,))
