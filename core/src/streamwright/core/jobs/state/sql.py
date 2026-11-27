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

"""The SQL plumbing of the state store: table names per dialect, one transaction per operation, lease fencing."""

import json
import logging
import time
from collections.abc import Sequence
from typing import Any

from streamwright.core.jobs.state.dialects import DEFAULT_SCHEMA, Dialect, RowUpdate
from streamwright.core.jobs.state.migrations.runner import MigrationRunner
from streamwright.core.jobs.state.protocol import MigrationsPendingError, StaleLeaseError, StateError
from streamwright.core.jobs.types import Lease
from streamwright.core.runtime.logs.sql import STATE_SQL, log_statement

__all__ = ["LOG", "SqlBase", "json_text", "json_value"]

LOG = logging.getLogger("streamwright.jobs.state")
_TABLES = ("sw_leases", "sw_jobs", "sw_sequences", "sw_receipts", "sw_events", "sw_signatures", "sw_migrations",
           "sw_requests")


def json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes)) else value



def _size(value: Any) -> int:
    """About how many bytes a parameter (or a row of them) takes in a statement."""
    if isinstance(value, (str, bytes, bytearray)):
        return len(value) + 2
    if isinstance(value, (list, tuple)):
        return sum(_size(item) for item in value)
    return 16

class SqlBase:
    """Table names, transactions, fencing and migrations of a SqlStateStore."""

    def __init__(self, connect: Any, dialect: Dialect, schema: str | None = DEFAULT_SCHEMA,
                 catalog: str | None = None, *, sleep: Any = time.sleep, target: str | None = None) -> None:
        """
        `connect()` returns a new DB-API connection (closed after each operation); `catalog` is DuckDB only; `target`
        says which database it is, without secrets, for logs and messages (default: the dialect's name).
        """
        self._connect, self._dialect, self._sleep = connect, dialect, sleep
        self.target = target or "%s database" % dialect.name
        prefix, _ = dialect.namespace(schema, catalog)
        self._schema, self._catalog = schema, catalog
        table_prefix = "" if dialect.name == "sqlite" else prefix
        self._tokens = {name: table_prefix + name for name in _TABLES}
        self._tokens.update(now=dialect.now, json=dialect.json_parameter, bigint=dialect.bigint_parameter,
                            expires=dialect.expires_in("?"))
        self._identity = None

    # --- plumbing -------------------------------------------------------------------------------------------

    def _sql(self, text: str) -> str:
        return self._dialect.placeholders(text.format(**self._tokens))

    def _x(self, conn: Any, text: str, args: Sequence[Any] = ()) -> Any:
        if not STATE_SQL.isEnabledFor(logging.INFO):   # streamwright.sql.state: a line per statement, when it logs
            return conn.execute(self._sql(text), tuple(args))
        statement, start = self._sql(text), time.perf_counter()
        try:
            result = conn.execute(statement, tuple(args))
        except Exception as error:
            log_statement("state", text.format(**self._tokens), time.perf_counter() - start, args, error=error,
                          logger=STATE_SQL)
            raise
        log_statement("state", text.format(**self._tokens), time.perf_counter() - start, args, logger=STATE_SQL)
        return result

    def _run(self, work: Any) -> Any:
        conn = self._connect()
        try:
            if self._dialect.begin:
                conn.execute(self._dialect.begin)
            result = work(conn)
            conn.commit()
        except BaseException as error:
            for cleanup in (conn.rollback, conn.close):
                try:
                    cleanup()
                except Exception as exc:
                    LOG.warning("state store %s failed after %s: %s", cleanup.__name__, type(error).__name__, exc)
            raise
        conn.close()
        return result

    @staticmethod
    def _marks(values: Sequence[Any]) -> str:
        return ", ".join("?" for _ in values)

    # --- many rows: a statement per chunk of rows, as many as max_parameters and max_statement_bytes allow -----

    def _chunks(self, rows: Sequence[Any], per_row: int, fixed: int = 0) -> Any:
        """`rows` in chunks of at most max_parameters parameters and about max_statement_bytes of values (a row
        larger than that alone)."""
        size = max(1, (self._dialect.max_parameters - fixed) // max(1, per_row))
        budget = self._dialect.max_statement_bytes
        start = 0
        while start < len(rows):
            end, used = start, 0
            while end < len(rows) and end - start < size:
                used += _size(rows[end])
                if end > start and used > budget:
                    break
                end += 1
            yield rows[start:end]
            start = end

    def _parameters(self, text: str) -> int:
        return text.format(**self._tokens).count("?")

    def _insert_rows(self, conn: Any, head: str, row: str, rows: Sequence[Sequence[Any]], tail: str = "") -> None:
        """`head` VALUES `row`, `row`, ... `tail`: `rows` (each a row's parameters) in multi-row INSERTs."""
        for chunk in self._chunks(rows, self._parameters(row)):
            self._x(conn, "%s VALUES %s %s" % (head, ", ".join([row] * len(chunk)), tail),
                    [value for values in chunk for value in values])

    def _update_in(self, conn: Any, update: str, args: Sequence[Any], column: str, values: Sequence[Any]) -> None:
        """`update` (an UPDATE ... WHERE ...; its parameters `args`) AND `column` IN `values`, a statement per chunk."""
        for chunk in self._chunks(values, 1, len(args)):
            self._x(conn, "%s AND %s IN (%s)" % (update, column, self._marks(chunk)), (*args, *chunk))

    def _update_rows(self, conn: Any, update: RowUpdate, args: Sequence[Any], rows: Sequence[Sequence[Any]]) -> bool:
        """
        Each of `rows` (the values of `update`'s columns) updates its row, `args` being the parameters of its `where`:
        a statement per chunk (Dialect.update_from_rows). False, with nothing done, where the database cannot: the
        caller then updates a row at a time.
        """
        per_row = sum(self._parameters(value) for _, value in update.columns)
        for chunk in self._chunks(rows, per_row, len(args)):
            statement = self._dialect.update_from_rows(update, len(chunk))
            if statement is None:              # known before the first chunk: nothing was updated
                return False
            self._x(conn, statement, (*(value for values in chunk for value in values), *args))
        return True

    def _lease_rows(self, conn: Any, job_name: str, tokens: Sequence[str], lock: str = "") -> dict[str, tuple]:
        """{token: (owner, epoch, expires_at, held)} of the lease rows that exist."""
        rows = self._x(conn, "SELECT token, worker_id, epoch, expires_at, CASE WHEN expires_at > {now} THEN 1 ELSE 0 "
                             "END FROM {sw_leases} WHERE job_name = ? AND token IN (" + self._marks(tokens) + ")"
                       + lock, (job_name, *tokens)).fetchall()
        return {row[0]: (row[1], int(row[2]), row[3], bool(int(row[4]))) for row in rows}

    def _check(self, conn: Any, lease: Lease, *, unexpired: bool = False, lock: str | None = None) -> dict:
        """Raises StaleLeaseError unless every token of `lease` still carries its epoch (and owner)."""
        tokens = lease.tokens or ("",)
        rows = self._lease_rows(conn, lease.job_name, tokens, self._dialect.share_lock if lock is None else lock)
        for token in tokens:
            row = rows.get(token)
            if row is None or row[1] != lease.epoch or row[0] != lease.owner_id or (unexpired and not row[3]):
                raise StaleLeaseError("lease for %s (token %s, epoch %s) was taken over%s" % (
                    lease.job_name, token or "<job>", lease.epoch, " or has expired" if unexpired else ""))
        return rows

    # --- schema & migrations ---------------------------------------------------------------------------------

    def migrations(self) -> MigrationRunner:
        """The migration runner of this store (apply, show, check, a dry run's statements)."""
        return MigrationRunner(self._connect, self._dialect, schema=self._schema, catalog=self._catalog,
                               target=self.target)

    def initialize(self) -> None:
        """Applies the pending migrations (idempotent; concurrent callers serialize on a migration lock)."""
        self.migrations().apply()

    def prepare(self, migrate: bool = True) -> None:
        """Applies the pending migrations (`migrate`), or raises MigrationsPendingError naming them."""
        if migrate:
            self.initialize()
            return
        pending = [record["name"] for record in self.migrations().show() if not record["applied"]]
        if pending:
            raise MigrationsPendingError(
                "the state tables in %s need migrations: %s; apply them first with `streamwright state migrate "
                "--url URL`, or let the run apply them: --apply-state-migrations, or runtime.state.apply_migrations: "
                "true" % (self.target, ", ".join(pending)))

    def check_schema(self) -> bool:
        """True when every migration is applied."""
        return self.migrations().check()

    def schema_version(self) -> int | None:
        return self.migrations().schema_version()

    def store_identity(self) -> str:
        """The store's persistent identity (seeded by migration 1), part of Delta idempotency keys."""
        if self._identity is None:
            self._identity = self.migrations().store_id()
            if self._identity is None:
                raise StateError("the state store is not initialized (run streamwright state migrate)")
        return self._identity
