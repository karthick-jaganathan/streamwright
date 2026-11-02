"""
Attached databases for the `source` and `duckdb` engines, composed by a connector's adapters (connector contract v1):
functions over a frozen DatabaseDialect (the database's facts, from the connector's client) and a resolved connection.

- source engine: `open_database_session(dialect, connection, context)` attaches the database READ_ONLY on a DuckDB
  sandbox of the connector's own - its connection parameters in a temporary secret (never the ATTACH text, which
  DuckDB's system views show), then external access off and the configuration locked - and returns a
  DatabaseSession whose `read` runs `database.query` (one checked, read-only SELECT with bound `$name` parameters)
  and `database.table` reads in pages, under the connection's statement timeout. `QueryReader` and `read_pages` are
  the parts other query sources (e.g. a lakehouse's sandbox) compose.
- duckdb engine: `configure_database` (a scoped secret and the ATTACH, writable only when an output or the state store
  uses the connection), `read_sql` (`read: query` runs on the server through `<extension>_query`, `read: table`)
  and `write_table` (append, overwrite, merge: INSERT ... ON CONFLICT on the primary key).

Records are JSON: decimals stay exact (decimal.Decimal), dates and timestamps are text (timestamps with time zone in
UTC); steps cast, e.g. `(record->>'amount')::DECIMAL(12,2)`.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator, Mapping
from typing import Any, Final

import duckdb

from streamwright.core.io.database_connections import DatabaseDialect, Parameters, connection_parameters, password_forms
from streamwright.core.io.queries import ALIAS, QueryPolicy, check_query, identifier_problem, table_query, timeout_ms
from streamwright.core.io.sandbox import (
    ConnectError,
    DuckDBSandbox,
    QueryError,
    StatementTimeoutError,
    decode_record,
    load_extension,
    message,
    record_query,
)
from streamwright.core.jobs.duckdb.reads import quote_relation
from streamwright.core.jobs.duckdb.writers.tables import write_table as write_catalog_table
from streamwright.core.jobs.types import WriteReceipt
from streamwright.core.runtime import logs
from streamwright.core.runtime.connector_types import ConnectorContext, ConnectorError
from streamwright.core.runtime.constants import PAGE_SIZE
from streamwright.core.runtime.contract import Operation, ResolvedConnection
from streamwright.core.sql.quote import quote_literal

__all__ = ["DatabaseSession", "QueryReader", "attach_database", "configure_database", "database_error",
           "open_database_session", "read_pages", "read_sql", "write_table"]

Page = list[dict[str, Any]]
SECRET_NAME: Final = "streamwright_src"
_TRANSIENT: Final = ("could not connect", "connection refused", "server closed the connection",
                     "connection timed out", "timeout expired", "connection reset", "ssl syscall",
                     "terminating connection", "no connection to the server", "could not receive data",
                     "could not send data", "lost connection", "can't connect", "server has gone away")


# * -------------------------------------------------
# * the source engine: checked queries read in pages
# * -------------------------------------------------

class QueryReader:
    """
    Runs checked queries (check_query under `policy`) on a sandbox with bound parameters, a row a JSON object, in
    pages of at most `page_size` records. `statement_timeout` (milliseconds): a statement still running after it is
    interrupted (StatementTimeoutError) - DuckDB has no server-side timeout of its own.
    """

    def __init__(self, sandbox: DuckDBSandbox, policy: QueryPolicy, *, page_size: int = PAGE_SIZE,
                 statement_timeout: int | None = None):
        self.sandbox, self.policy = sandbox, policy
        self.page_size, self.statement_timeout = page_size, statement_timeout

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self.sandbox.connection

    def check(self, query: str, parameters: Mapping[str, Any] | None = None) -> str:
        """The query to run (see check_query); QueryError unless it is one read-only SELECT."""
        statement, problems = check_query(query, parameters if parameters is not None else {}, self.policy)
        if problems:
            raise QueryError("; ".join(problems))
        return statement

    def _timed(self, function: Any, *arguments: Any) -> Any:
        if not self.statement_timeout:
            return function(*arguments)
        timer = threading.Timer(self.statement_timeout / 1000.0, self.connection.interrupt)
        timer.daemon = True
        timer.start()
        try:
            return function(*arguments)
        except duckdb.InterruptException:
            raise StatementTimeoutError("the statement ran past the statement timeout (%d ms) and was interrupted" %
                                   self.statement_timeout) from None
        finally:
            timer.cancel()

    def pages(self, query: str, parameters: Mapping[str, Any] | None = None) -> Iterator[Page]:
        """
        The rows of a checked query as lists of at most page_size records, its `parameters` bound. QueryError for a
        query that is not one read-only SELECT, StatementTimeoutError past the timeout, DuckDB's errors as they are.
        """
        if self.sandbox.closed:
            raise QueryError("the database connection is closed")
        statement = self.check(query, parameters)
        result = self._timed(self.connection.execute, record_query(statement), dict(parameters or {}))
        while True:
            rows = self._timed(result.fetchmany, self.page_size)
            if not rows:
                return
            yield [decode_record(row[0]) for row in rows]

    def close(self) -> None:
        self.sandbox.close()


def database_error(connector: str, exc: BaseException, redact: Any = str) -> ConnectorError | None:
    """A ConnectorError for a reader's and DuckDB's errors (redacted with `redact`), or None for others."""
    known = ((QueryError, "QUERY_REFUSED"), (StatementTimeoutError, "STATEMENT_TIMEOUT"))
    for kind, code in known:
        if isinstance(exc, kind):
            return ConnectorError("%s: %s" % (connector, redact(exc)), code=code)
    if isinstance(exc, ConnectError):
        text = str(exc)
        return ConnectorError("%s: %s" % (connector, redact(text)), code="CONNECT_ERROR",
                              retryable=any(phrase in text.lower() for phrase in _TRANSIENT))
    if not isinstance(exc, duckdb.Error):
        return None
    text = message(exc)
    lower = text.lower()
    if "read-only" in lower or "read only transaction" in lower:
        return ConnectorError("%s: the database is read-only: %s" % (connector, redact(text)), code="READ_ONLY")
    if "statement timeout" in lower:
        return ConnectorError("%s: %s" % (connector, redact(text)), code="STATEMENT_TIMEOUT")
    if any(phrase in lower for phrase in _TRANSIENT):
        return ConnectorError("%s: %s" % (connector, redact(text)), code="CONNECTION_ERROR", retryable=True)
    return ConnectorError("%s: %s" % (connector, redact(text)), code="QUERY_ERROR")


def _raise_translated(connector: str, exc: Exception, context: ConnectorContext) -> None:
    error = database_error(connector, exc, context.redact)
    if error is None:
        raise exc
    raise error from None


def read_pages(reader: QueryReader, context: ConnectorContext, *, connector: str, call: str, label: str,
               query: str, parameters: Mapping[str, Any]) -> Iterator[Page]:
    """
    The pages of one query: the first is read through context.call() (rate limit and retries), then one
    `streamwright.network` line. A failure after the first page is not retried: its rows were already read.
    """
    def start() -> tuple[Page | None, Iterator[Page]]:
        try:
            pages = reader.pages(query, parameters)
            return next(pages, None), pages
        except ConnectorError:
            raise
        except Exception as exc:
            _raise_translated(connector, exc, context)
            raise
    rows, started = 0, time.monotonic()
    page, pages = context.call(start)
    try:
        while page is not None:
            rows += len(page)
            yield page
            page = next(pages, None)
    except Exception as exc:
        _raise_translated(connector, exc, context)
    seconds, where = time.monotonic() - started, context.where
    text = logs.describe(where) if where else ""
    logs.NETWORK.info("%s%s %s: %s row(s), %.2f s", text + ": " if text else "", connector, label,
                      f"{rows:,}", seconds,
                      extra=logs.fields(event="db_query", connector=connector, call=call, records=rows,
                                        duration_ms=round(seconds * 1000), **(where or {})))


def attach_database(dialect: DatabaseDialect, parameters: Parameters, *, page_size: int = PAGE_SIZE,
                    statement_timeout: int | None = None, masked: tuple[Any, ...] = ()) -> QueryReader:
    """
    A QueryReader on a sandbox where the database is attached READ_ONLY as `src`, its default catalog; then external
    access is off and the configuration locked: a query reads the database only. ConnectError when it cannot be.
    """
    sandbox = DuckDBSandbox(masked=masked, provider=dialect.name)
    try:
        connection = sandbox.connection
        connection.execute("SET TimeZone = 'UTC'")
        load_extension(connection, dialect.extension)
        for setting, value in dialect.settings:
            connection.execute("SET %s = %s" % (setting, value))
        options = dialect.secret_options(parameters)
        try:
            connection.execute("CREATE TEMPORARY SECRET %s (TYPE %s, %s)" % (
                SECRET_NAME, dialect.extension, ", ".join("%s ?" % name for name, _ in options)),
                [value for _, value in options])
            connection.execute("ATTACH '' AS %s (TYPE %s, SECRET %s, READ_ONLY)" % (
                ALIAS, dialect.extension, SECRET_NAME))
        except duckdb.Error as exc:
            raise ConnectError("cannot attach the database: %s" % sandbox.mask(message(exc))) from None
        connection.execute("USE %s" % ALIAS)
        sandbox.lock()
    except Exception:
        sandbox.close()
        raise
    return QueryReader(sandbox, dialect.policy, page_size=page_size, statement_timeout=statement_timeout)


class DatabaseSession:
    """A source session: `database.query` and `database.table` reads on an attached database."""

    def __init__(self, reader: QueryReader, dialect: DatabaseDialect):
        self.reader, self.dialect = reader, dialect

    def _table_call(self, arguments: Mapping[str, Any]) -> tuple[str, dict[str, Any], str]:
        schema = arguments.get("schema") or None
        names = [("schema", schema), ("table", arguments["table"])] + [
            ("column", column) for column in arguments.get("columns") or ()]
        problems = ["`%s` %r %s" % (what, name, problem) for what, name in names if name is not None
                    for problem in [identifier_problem(name, self.dialect.identifier_limit)] if problem]
        if problems:
            raise ConnectorError("%s: %s" % (self.dialect.name, "; ".join(problems)), code="QUERY_REFUSED")
        query, parameters = table_query(arguments["table"], schema, arguments.get("columns"), arguments.get("where"))
        return query, parameters, "table %s" % (arguments["table"] if not schema else "%s.%s" % (
            schema, arguments["table"]))

    def read(self, operation: Operation, arguments: Mapping[str, Any], context: ConnectorContext) -> Iterator[Page]:
        if operation.method == "query":
            query, parameters, label = arguments["query"], dict(arguments.get("params") or {}), "query"
        else:
            query, parameters, label = self._table_call(arguments)
        return read_pages(self.reader, context, connector=self.dialect.name, call=operation.name, label=label,
                          query=query, parameters=parameters)

    def close(self) -> None:
        self.reader.close()


def open_database_session(dialect: DatabaseDialect, connection: ResolvedConnection, context: ConnectorContext, *,
                          page_size: int = PAGE_SIZE) -> DatabaseSession:
    """Attaches the connection's database (see attach_database); ConnectorError (redacted) when it cannot be."""
    timeout = connection.get("statement_timeout")
    milliseconds = None if timeout is None else timeout_ms(timeout)
    try:
        parameters = connection_parameters(dialect, connection)
    except ValueError as exc:
        raise ConnectorError("%s: %s" % (dialect.name, exc), code="CONNECT_ERROR") from None
    if milliseconds is not None and dialect.server_timeout is not None:
        parameters, milliseconds = dialect.server_timeout(parameters, milliseconds), None
    passwords = [value for key, value in parameters if key == "password" and value]
    passwords += password_forms(connection["dsn"], dialect.url_schemes) if connection.get("dsn") else []
    for value in passwords:
        context.secret(value)
    try:
        reader = attach_database(dialect, parameters, page_size=page_size, statement_timeout=milliseconds,
                                 masked=tuple(passwords))
    except Exception as exc:
        _raise_translated(dialect.name, exc, context)
        raise
    return DatabaseSession(reader, dialect)


# * ------------------------------------------------
# * the duckdb engine: the database in the job's session
# * ------------------------------------------------

def configure_database(dialect: DatabaseDialect, session: Any, name: str, connection: Mapping[str, Any]) -> None:
    """Attaches the connection's database as `name`: its parameters in a scoped secret, read-only unless written."""
    session.load_extension(dialect.extension)
    secret = session.create_secret(name, dialect.extension,
                                   dialect.secret_options(connection_parameters(dialect, connection)))
    session.attach(name, "", kind=dialect.extension, secret=secret, read_only=not session.is_writable(name))


def read_sql(dialect: DatabaseDialect, context: Any, operation: Operation, arguments: Mapping[str, Any], *,
             default_schema: str | None) -> str:
    """The SQL of a step: `database.query` runs on the server (`<extension>_query`), `database.table` reads a table."""
    if operation.method == "query":
        return "SELECT * FROM %s_query(%s, %s)" % (dialect.extension, quote_literal(context.name),
                                                   quote_literal(arguments["query"]))
    return "SELECT * FROM %s" % quote_relation(context.name, arguments["table"],
                                               arguments.get("schema") or default_schema)


def write_table(context: Any, *, default_schema: str | None) -> WriteReceipt:
    """Writes the output into its table (a missing table is created from the data); merge: ON CONFLICT on the key."""
    return write_catalog_table(context, context.name, merge_strategy="on_conflict", default_schema=default_schema)
