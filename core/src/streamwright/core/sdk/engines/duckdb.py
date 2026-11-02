"""
The `duckdb` engine adapter protocol (kind: DuckDBJob): a connector's `engines/duckdb.py` sets a connection up in the
job's DuckDB session and compiles operations: reads to SQL, writes to a WriteReceipt. The connection's credentials go
into scoped DuckDB secrets as bound parameters, never into SQL text.
"""

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from streamwright.core.jobs.duckdb.adapter import AdapterError, ReadContext, WriteContext
from streamwright.core.jobs.duckdb.session import Session as DuckDBSession
from streamwright.core.jobs.state import StateStore, open_store
from streamwright.core.jobs.types import WriteReceipt
from streamwright.core.jobs.write_rules import evolves_schema, merge_condition, render_target_predicate
from streamwright.core.runtime.contract import Operation, ResolvedConnection

__all__ = ["AdapterError", "DuckDBAdapter", "DuckDBSession", "DuckDBStateAdapter", "DuckDBWriteAdapter", "ReadContext",
           "StateStore", "WriteContext", "WriteReceipt", "evolves_schema", "merge_condition", "open_store",
           "render_target_predicate"]


@runtime_checkable
class DuckDBAdapter(Protocol):
    def configure(self, session: DuckDBSession, name: str, connection: ResolvedConnection) -> None:
        """Sets the connection `name` up in the session (extensions, scoped secrets, ATTACH)."""
        ...

    def read_sql(self, context: ReadContext, operation: Operation, arguments: Mapping[str, Any]) -> str:
        """The SQL of a read operation's step; AdapterError when it cannot be read as asked."""
        ...


@runtime_checkable
class DuckDBWriteAdapter(DuckDBAdapter, Protocol):
    """An adapter of a connector that writes on this engine (one of its operations it runs there is a write)."""

    def write(self, context: WriteContext, operation: Operation, arguments: Mapping[str, Any]) -> WriteReceipt:
        """Writes `context.source` (write operations only)."""
        ...


@runtime_checkable
class DuckDBStateAdapter(DuckDBAdapter, Protocol):
    """An adapter whose connections can hold the job's bookkeeping state (`runtime.state.connection`)."""

    def open_state(self, session: DuckDBSession, name: str, connection: ResolvedConnection,
                   schema: str) -> StateStore:
        """The StateStore in `schema` of the connection (e.g. `open_store(connect, "postgres", schema)`)."""
        ...
