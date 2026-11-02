"""
The source engine's reader of one object store: a locked DuckDB connection of its own (never the run's transform
sandbox) with the store's extension and a temporary secret scoped to the roots, reading only inside them. Every URL a
path names or a listing returns is checked (`locations`) before anything is read; pages are fetched with fetchmany,
so memory stays bounded however large a file is. Local files have the same reader without an extension or a secret
(LocalReader).
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import duckdb

from streamwright.core.io.file_formats import MissingError, ReadError, file_query, option_problems
from streamwright.core.io.local import LocalLocation
from streamwright.core.io.locations import FileLocation, ObjectStore, secret_parameters
from streamwright.core.io.sandbox import ConnectError, DuckDBSandbox, decode_record, load_extension, message
from streamwright.core.runtime.constants import PAGE_SIZE

__all__ = ["LocalReader", "Page", "StoreReader", "create_scoped_secret", "list_objects", "load_store_extension",
           "open_local_reader", "open_store_reader", "read_pages"]

Page = list[dict[str, Any]]


def load_store_extension(sandbox: DuckDBSandbox, extension: str) -> None:
    try:
        load_extension(sandbox.connection, extension)
    except ConnectError as exc:
        raise ReadError(sandbox.mask(exc)) from None


def create_scoped_secret(sandbox: DuckDBSandbox, provider: str, secret_type: str,
                         parameters: list[tuple[str, Any]], roots: tuple[str, ...] | list[str]) -> None:
    """The store's credentials and settings as a temporary secret scoped to the roots (bound parameters only)."""
    names = ", ".join("%s ?" % parameter for parameter, _ in parameters)
    statement = "CREATE OR REPLACE TEMPORARY SECRET streamwright_%s (TYPE %s%s, SCOPE ?)" % (
        provider, secret_type, ", " + names if names else "")
    try:
        sandbox.connection.execute(statement, [value for _, value in parameters] + [list(roots)])
    except duckdb.Error as exc:
        raise ReadError("cannot set the %s credentials: %s" % (provider, sandbox.mask(message(exc)))) from None


def list_objects(sandbox: DuckDBSandbox, url: str, not_found: Any) -> list[str]:
    """The objects a (checked) glob over object storage matches; none when nothing does."""
    try:
        return [row[0] for row in sandbox.connection.execute("SELECT file FROM glob(?)", [url]).fetchall()]
    except duckdb.Error as exc:
        text = sandbox.mask(message(exc))
        if not_found.search(text):
            return []
        raise ReadError("cannot list %s: %s" % (url, text)) from None


def read_pages(sandbox: DuckDBSandbox, location: str, file_format: str, options: Mapping[str, Any] | None = None, *,
               page_size: int, filename: str | None = None, not_found: Any = None) -> Iterator[Page]:
    """
    The rows of one (checked) file as lists of at most page_size records; `filename`: the text the `filename`
    option adds to each record. DuckDB errors are ReadErrors (credentials masked); a location that names no file is a
    MissingError where `not_found` (a regex over DuckDB's message) knows it.
    """
    problems = option_problems(file_format, options)
    if problems:
        raise ReadError("%s: %s" % (location, "; ".join(problems)))
    query, parameters = file_query(location, file_format, options)
    add_name = bool((options or {}).get("filename"))
    try:
        result = sandbox.connection.execute(query, parameters)
        while rows := result.fetchmany(page_size):
            page = [decode_record(row[0]) for row in rows]
            for record in page if add_name else ():
                if isinstance(record, dict):
                    record["filename"] = filename if filename is not None else location
            yield page
    except duckdb.Error as exc:
        text = sandbox.mask(message(exc))
        if not_found is not None and not_found.search(text):
            raise MissingError("no file at %s: %s" % (location, text)) from None
        raise ReadError("cannot read %s as %s: %s" % (location, file_format, text)) from None


@dataclass(frozen=True)
class StoreReader:
    """The source engine's reader of one connection: a locked DuckDB sandbox and the location it may read."""
    sandbox: DuckDBSandbox
    location: FileLocation
    page_size: int = PAGE_SIZE

    @property
    def closed(self) -> bool:
        return self.sandbox.closed

    def list_files(self, url: str) -> list[str]:
        if self.closed:
            raise ReadError("the reader is closed")
        self.location.check(url, glob=True)
        return list_objects(self.sandbox, url, self.location.store.not_found)

    def resolve(self, path: str) -> list[tuple[str, str]]:
        return self.location.resolve_files(path, self.list_files)

    def match(self, path: str, pattern: str, recursive: bool = False) -> list[tuple[str, str]]:
        return self.location.match_files(path, pattern, recursive, self.list_files)

    def pages(self, location: str, file_format: str, options: Mapping[str, Any] | None = None,
              page_size: int | None = None, filename: str | None = None) -> Iterator[Page]:
        if self.closed:
            raise ReadError("the reader is closed")
        self.location.check(location)
        yield from read_pages(self.sandbox, location, file_format, options, page_size=page_size or self.page_size,
                              filename=filename, not_found=self.location.store.not_found)

    def close(self) -> None:
        self.sandbox.close()


def open_store_reader(store: ObjectStore, connection: Mapping[str, Any], *, page_size: int = PAGE_SIZE) -> StoreReader:
    """A reader confined to the connection's roots, its credentials in a DuckDB secret scoped to them."""
    location = FileLocation(store, tuple(connection["roots"]))
    sandbox = DuckDBSandbox(masked=store.masked_values(connection), provider=store.provider)
    try:
        sandbox.connection.execute("SET TimeZone = 'UTC'")
        load_store_extension(sandbox, store.extension)
        create_scoped_secret(sandbox, store.provider, store.secret_type, secret_parameters(store, connection),
                             location.roots)
        sandbox.lock(allowed_directories=list(location.roots))
    except Exception:
        sandbox.close()
        raise
    return StoreReader(sandbox, location, page_size)


@dataclass(frozen=True)
class LocalReader:
    """The source engine's reader of local files: a locked DuckDB sandbox that can only read inside the roots."""
    sandbox: DuckDBSandbox
    location: LocalLocation
    page_size: int = PAGE_SIZE

    @property
    def closed(self) -> bool:
        return self.sandbox.closed

    def resolve(self, path: str) -> list[tuple[str, str]]:
        return self.location.resolve_files(path)

    def match(self, path: str, pattern: str, recursive: bool = False) -> list[tuple[str, str]]:
        return self.location.match_files(path, pattern, recursive)

    def pages(self, location: str, file_format: str, options: Mapping[str, Any] | None = None,
              page_size: int | None = None, filename: str | None = None) -> Iterator[Page]:
        if self.closed:
            raise ReadError("the reader is closed")
        self.location.check(location)
        yield from read_pages(self.sandbox, location, file_format, options, page_size=page_size or self.page_size,
                              filename=filename)

    def close(self) -> None:
        self.sandbox.close()


def open_local_reader(roots: tuple[str, ...] | list[str], *, provider: str, page_size: int = PAGE_SIZE) -> LocalReader:
    """
    A reader confined to the roots (DuckDB's allowed_directories, external access off, configuration locked) for the
    connector `provider`.
    """
    location = LocalLocation(tuple(roots))
    sandbox = DuckDBSandbox(provider=provider)
    try:
        sandbox.connection.execute("SET TimeZone = 'UTC'")
        sandbox.lock(allowed_directories=[root.rstrip(os.sep) + os.sep for root in location.roots])
    except Exception:
        sandbox.close()
        raise
    return LocalReader(sandbox, location, page_size)
