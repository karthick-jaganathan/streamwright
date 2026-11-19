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
"""The DDL of creating a state table (its indexes and comments), shared by the migrations that create tables."""

from sqlalchemy.schema import CreateIndex, CreateTable, SetColumnComment, SetTableComment

from streamwright.core.jobs.state.tables import sqlalchemy_dialect

__all__ = ["create_statements", "execute"]

_MYSQL_DUPLICATE_KEY = 1061


def create_statements(dialect_name, table):
    """CREATE TABLE (if not exists), its indexes and, where the database has them, its comments."""
    compiler = sqlalchemy_dialect(dialect_name)
    statements = [str(CreateTable(table, if_not_exists=True).compile(dialect=compiler)).strip()]
    # MySQL has no CREATE INDEX IF NOT EXISTS: execute() skips an index that exists (error 1061)
    statements.extend(str(CreateIndex(index, if_not_exists=dialect_name != "mysql").compile(
        dialect=compiler)).strip() for index in table.indexes)
    if dialect_name in ("postgres", "duckdb"):  # COMMENT ON
        if table.comment:
            statements.append(str(SetTableComment(table).compile(dialect=compiler)).strip())
        statements.extend(str(SetColumnComment(column).compile(dialect=compiler)).strip()
                          for column in table.columns if column.comment)
    return statements


def execute(conn, dialect, statements):
    """Runs `statements`; on MySQL an index left by an interrupted run already exists, and is skipped."""
    for statement in statements:
        try:
            conn.execute(statement)
        except Exception as exc:
            if dialect.name == "mysql" and statement.startswith("CREATE INDEX") and \
                    exc.args[:1] == (_MYSQL_DUPLICATE_KEY,):
                continue
            raise
