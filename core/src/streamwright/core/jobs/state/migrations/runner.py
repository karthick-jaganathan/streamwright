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

"""
Applies the state store's migrations: modules `NNNN_name.py` in this package, each with VERSION, NAME, DESCRIPTION,
get_statements(dialect, schema) and apply(conn, dialect, schema), in version order, each once,
recorded in sw_migrations under a database lock so concurrent runners serialize.
"""

import importlib
import logging
import os
import pkgutil
import re
import time
from collections.abc import Callable
from typing import Any

from sqlalchemy.schema import CreateTable

from streamwright.core.jobs.state.dialects import DEFAULT_SCHEMA, DIALECTS, Dialect
from streamwright.core.jobs.state.tables import get_tables, sqlalchemy_dialect
from streamwright.core.runtime.logs.lines import fields

__all__ = ["MigrationRunner"]

LOG = logging.getLogger("streamwright.jobs.state.migrations")

_MIGRATION_PATTERN = re.compile(r"^(\d{4})_(.+)$")


class MigrationRunner:
    """Ordered, idempotent migrations of a state database."""

    def __init__(self, connect: Callable[[], Any], dialect: Dialect | str, schema: str | None = DEFAULT_SCHEMA,
                 catalog: str | None = None, *, target: str | None = None) -> None:
        """`target`: which database this is, without secrets (store.target), for the log lines."""
        if isinstance(dialect, str):
            if dialect not in DIALECTS:
                raise ValueError("unknown state dialect %r" % (dialect,))
            dialect = DIALECTS[dialect]()
        self._connect, self._dialect, self._schema, self._catalog = connect, dialect, schema, catalog
        self.target = target or "%s database" % dialect.name
        prefix, self._create_schema = dialect.namespace(schema, catalog)
        # SQLite keeps the sw_* tables unprefixed; elsewhere they live in the schema
        self._table_schema = None if dialect.name == "sqlite" else schema
        self._migrations_table = ("" if dialect.name == "sqlite" else prefix) + "sw_migrations"

    @property
    def dialect(self) -> Dialect:
        return self._dialect

    @property
    def schema(self) -> str | None:
        return self._schema

    def _run(self, work: Callable[[Any], Any]) -> Any:
        conn = self._connect()
        try:
            if self._catalog and self._dialect.name == "duckdb":
                conn.execute('USE "%s"' % self._catalog)
            if self._dialect.begin:
                conn.execute(self._dialect.begin)
            result = work(conn)
            conn.commit()
            return result
        except BaseException as error:
            try:
                conn.rollback()
            except Exception as exc:
                LOG.warning("state migration rollback failed after %s: %s", type(error).__name__, exc)
            raise
        finally:
            try:
                conn.close()
            except Exception as exc:
                LOG.warning("state migration connection close failed: %s", exc)

    def discover_migrations(self) -> list[Any]:
        """The migration modules of this package, in version order."""
        found = []
        for _, name, _ in pkgutil.iter_modules([os.path.dirname(__file__)]):
            match = _MIGRATION_PATTERN.match(name)
            if match:
                found.append((int(match.group(1)), importlib.import_module("%s.%s" % (__package__, name))))
        return [module for _, module in sorted(found, key=lambda item: item[0])]

    def latest_version(self) -> int:
        return self.discover_migrations()[-1].VERSION

    def _select(self, conn: Any, columns: str, suffix: str = "") -> list[tuple]:
        """Rows of sw_migrations, or [] while it does not exist."""
        try:
            return conn.execute("SELECT %s FROM %s%s" % (columns, self._migrations_table, suffix)).fetchall()
        except Exception:
            return []

    def get_applied_migrations(self, conn: Any = None) -> list[dict[str, Any]]:
        """The applied migrations: version, name, applied_at, store_id."""
        def work(c):
            return [{"version": int(row[0]), "name": str(row[1]), "applied_at": self._dialect.read_timestamp(row[2]),
                     "store_id": None if row[3] is None else str(row[3])}
                    for row in self._select(c, "version, name, applied_at, store_id", " ORDER BY version")]

        return work(conn) if conn is not None else self._run(work)

    def apply(self, dry_run: bool = False) -> list[dict[str, Any]]:
        """Applies the pending migrations; with dry_run, returns their statements instead."""
        migrations = self.discover_migrations()
        if dry_run:
            applied = {m["version"] for m in self.get_applied_migrations()}
            return [{"version": module.VERSION, "name": module.NAME, "description": getattr(module, "DESCRIPTION", ""),
                     "statements": module.get_statements(self._dialect.name, schema=self._table_schema)}
                    for module in migrations if module.VERSION not in applied]

        def work(conn):
            if self._dialect.migration_lock:
                row = conn.execute(self._dialect.placeholders(self._dialect.migration_lock),
                                   (self._schema or "",)).fetchone()
                if not self._dialect.migration_locked(row):
                    raise RuntimeError("could not take the state schema migration lock")
            if self._create_schema:
                conn.execute(self._create_schema)
            table = get_tables(schema=self._table_schema)["sw_migrations"]
            conn.execute(str(CreateTable(table, if_not_exists=True).compile(
                dialect=sqlalchemy_dialect(self._dialect.name))).strip())
            applied = {m["version"] for m in self.get_applied_migrations(conn)}
            pending = [module for module in migrations if module.VERSION not in applied]
            if pending:
                LOG.info("applying %d state migration(s) to %s: %s", len(pending), self.target,
                         ", ".join(module.NAME for module in pending),
                         extra=fields(event="state_migrations", database=self.target, status="applying"))
            records = []
            for module in pending:
                started = time.monotonic()
                store_id = module.apply(conn, self._dialect, schema=self._table_schema)
                conn.execute(self._dialect.placeholders(
                    "INSERT INTO %s (version, name, applied_at, store_id, created_at, updated_at) VALUES (?, ?, %s, ?, "
                    "%s, %s)" % (self._migrations_table, self._dialect.now, self._dialect.now, self._dialect.now)),
                    (module.VERSION, module.NAME, store_id))
                elapsed = time.monotonic() - started
                LOG.info("state migration %s (version %d) applied to %s in %.2f s; store id %s", module.NAME,
                         module.VERSION, self.target, elapsed, store_id,
                         extra=fields(event="state_migration", database=self.target, status="applied",
                                      duration_ms=round(elapsed * 1000)))
                records.append({"version": module.VERSION, "name": module.NAME, "store_id": store_id})
            return records

        return self._run(work)

    def show(self) -> list[dict[str, Any]]:
        """Every migration with whether (and when) it was applied."""
        applied = {m["version"]: m for m in self.get_applied_migrations()}
        return [{"version": module.VERSION, "name": module.NAME, "description": getattr(module, "DESCRIPTION", ""),
                 "applied": module.VERSION in applied,
                 "applied_at": applied[module.VERSION]["applied_at"] if module.VERSION in applied else None,
                 "store_id": applied[module.VERSION]["store_id"] if module.VERSION in applied else None}
                for module in self.discover_migrations()]

    def check(self) -> bool:
        """True when every migration is applied."""
        applied = {m["version"] for m in self.get_applied_migrations()}
        return {module.VERSION for module in self.discover_migrations()} <= applied

    def store_id(self) -> str | None:
        """The store's persistent identity (recorded by migration 1), or None before it is applied."""
        rows = self._run(lambda conn: self._select(conn, "store_id", " WHERE version = 1"))
        return str(rows[0][0]) if rows and rows[0][0] is not None else None

    def schema_version(self) -> int | None:
        """The highest applied migration version, or None."""
        rows = self._run(lambda conn: self._select(conn, "MAX(version)"))
        return int(rows[0][0]) if rows and rows[0][0] is not None else None
