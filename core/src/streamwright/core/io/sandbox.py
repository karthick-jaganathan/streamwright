"""
DuckDBSandbox: a connector's own in-memory DuckDB connection (never the run's transform sandbox), with its
configuration lockable and its messages masked, plus the errors and record decoding of what reads through one. Object
stores (`store_reader`) and databases (`databases`) compose it.
"""

from __future__ import annotations

import decimal
import json
import shutil
import tempfile
import weakref
from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Any, Final
from urllib.parse import quote, quote_plus

import duckdb

from streamwright.core.sql.quote import quote_literal

__all__ = ["LIMITS", "ConnectError", "DuckDBSandbox", "QueryError", "StatementTimeoutError", "decode_record",
           "load_extension", "message", "record_query"]

LIMITS: Final = MappingProxyType({"memory_limit": "1GB", "threads": 1})


class QueryError(Exception):
    """A query the connector does not run: not one read-only SELECT, or parameters that do not match."""


class ConnectError(Exception):
    """The database or store cannot be reached (the DuckDB extension, the connection parameters or the server)."""


class StatementTimeoutError(Exception):
    """A statement that ran past the statement timeout and was interrupted."""


def message(exc: BaseException) -> str:
    """DuckDB's message on one line, without its query excerpt (LINE 1: ...)."""
    text = str(exc).strip().split("\n\n")[0]
    lines = [line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith("LINE ")]
    return " ".join(lines) or type(exc).__name__


def decode_record(text: str | None) -> Any:
    """A JSON text as a record, its numbers with fractions exact (decimal.Decimal)."""
    return None if text is None else json.loads(text, parse_float=decimal.Decimal)


def record_query(query: str) -> str:
    """A checked query whose rows become JSON objects, one `record` each (a comment ending it stays inside)."""
    return "SELECT to_json(t) AS record FROM (\n%s\n) t" % query


def load_extension(connection: duckdb.DuckDBPyConnection, name: str) -> None:
    """Loads a DuckDB extension, installing it first (once, from DuckDB's repository) when it is missing."""
    try:
        installed = connection.execute("SELECT installed FROM duckdb_extensions() WHERE extension_name = ?",
                                       [name]).fetchone()
        if not (installed and installed[0]):
            connection.execute("INSTALL %s" % name)
        connection.execute("LOAD %s" % name)
    except duckdb.Error as exc:
        raise ConnectError("cannot load DuckDB's %s extension (install it with: INSTALL %s): %s" % (
            name, name, message(exc))) from None


def _cleanup(connection: duckdb.DuckDBPyConnection, folder: str) -> None:
    try:
        connection.close()
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def _masked_forms(values: Iterable[Any]) -> list[str]:
    """Each credential as written, stripped and URL-encoded (4 characters or more), longest first."""
    forms = set()
    for value in values:
        if value in (None, ""):
            continue
        text = str(value)
        forms.update(form for form in (text, text.strip(), quote(text, safe=""), quote_plus(text)) if len(form) >= 4)
    return sorted(forms, key=lambda form: (-len(form), form))


class DuckDBSandbox:
    """
    A DuckDB connection of the connector's own, in memory, spilling to a private temporary folder (removed by close(),
    or when the sandbox is collected). Extensions are never installed or loaded behind the connector's back. lock()
    turns external access off and locks the configuration; mask() hides the credentials in messages.
    """

    def __init__(self, limits: Mapping[str, Any] | None = None, masked: Iterable[Any] = (),
                 provider: str | None = None):
        self.duckdb = duckdb
        self.provider = provider
        self.masked = _masked_forms(masked)
        self.folder = tempfile.mkdtemp(prefix="streamwright-%s-" % provider)
        try:
            config = dict(limits or LIMITS, autoinstall_known_extensions=False, autoload_known_extensions=False,
                          python_enable_replacements=False, temp_directory=self.folder)
            self.connection = duckdb.connect(":memory:", config=config)
        except Exception:
            shutil.rmtree(self.folder, ignore_errors=True)
            raise
        self._finalizer = weakref.finalize(self, _cleanup, self.connection, self.folder)

    def lock(self, allowed_directories: Iterable[str] | None = None) -> None:
        """
        Turns external access off for good. `allowed_directories` (paths or URL prefixes) stay readable: DuckDB takes
        them only while external access is still on, so they are set first.
        """
        if allowed_directories is not None:
            self.connection.execute("SET allowed_directories = [%s]" % ", ".join(
                quote_literal(location) for location in allowed_directories))
        self.connection.execute("SET enable_external_access = false")
        self.connection.execute("SET lock_configuration = true")

    def mask(self, text: Any) -> str:
        """`text` with every credential masked as ***."""
        text = str(text)
        for value in self.masked:
            text = text.replace(value, "***")
        return text

    def add_mask(self, values: Iterable[Any]) -> None:
        """Masks more credentials from now on."""
        self.masked = _masked_forms([*self.masked, *values])

    def __enter__(self) -> DuckDBSandbox:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self._finalizer()

    @property
    def closed(self) -> bool:
        return not self._finalizer.alive
