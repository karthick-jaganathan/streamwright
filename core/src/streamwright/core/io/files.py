"""
Files for the `source` and `duckdb` engines, composed by a connector's adapters (connector contract v1; the
counterpart of `streamwright.spark.io.files`): functions and frozen values over an ObjectStore (the store's facts, from
the connector's client) and a FileLocation (a connection's roots), or over a LocalLocation for local files.

- source engine: `open_file_session(store, connection, context)` (`open_local_session(connection, context)` for local
  files) - a FileSession whose `read` resolves every file a rendered `object.read` call names (a path, a glob, a list
  of them, or a folder and a `match` regex) BEFORE any is read, then reads each on a locked DuckDB connection of its
  own, one `streamwright.network` line per file.
- duckdb engine: `configure_store_secrets` (one DuckDB secret per root, scoped to it), `read_sql` (a step's read,
  through the shared ingest planner), `locate_local` (the `locate` of local files: paths relative to the job's folder)
  and `write_files` (COPY: csv, tsv, json, jsonl, parquet; `file_name` and `layout`, the templates of
  `streamwright.core.jobs.naming`, name the file - or with `split_by` one file per split - inside the folder `path`;
  `partition_by` writes Hive-style folders; mode `overwrite` replaces, `error` refuses to write over a file).
- what each engine supports of the file operations: SOURCE_CAPABILITIES and DUCKDB_CAPABILITIES (`file_rules`).

Records are JSON: decimals stay exact (decimal.Decimal), dates and timestamps are text, and csv/tsv values are the
file's text (all_varchar, empty cells null) unless the `columns` option types them.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final

import duckdb

from streamwright.core.io.file_formats import AccessError, MissingError, ReadError, format_of
from streamwright.core.io.file_rules import (
    DUCKDB_CAPABILITIES,
    ENGINE_CAPABILITIES,
    SOURCE_CAPABILITIES,
    check_read,
    check_write,
)
from streamwright.core.io.local import LocalLocation
from streamwright.core.io.locations import (
    FileLocation,
    ObjectStore,
    allowed_roots,
    path_problem,
    root_problems,
    secret_parameters,
    url_problem,
)
from streamwright.core.io.store_reader import (
    LocalReader,
    Page,
    StoreReader,
    create_scoped_secret,
    list_objects,
    load_store_extension,
    open_local_reader,
    open_store_reader,
    read_pages,
)
from streamwright.core.jobs import naming
from streamwright.core.jobs.duckdb.adapter import check_location
from streamwright.core.jobs.duckdb.reads import compile_file_read, sql_value
from streamwright.core.jobs.refs import sql_literal
from streamwright.core.jobs.types import WriteReceipt
from streamwright.core.runtime import logs
from streamwright.core.runtime.connector_types import ConnectorContext, ConnectorError
from streamwright.core.runtime.constants import PAGE_SIZE
from streamwright.core.runtime.contract import Operation, ResolvedConnection
from streamwright.core.sql.quote import quote_ident, quote_literal

__all__ = [
    "DUCKDB_CAPABILITIES",
    "ENGINE_CAPABILITIES",
    "SOURCE_CAPABILITIES",
    "AccessError",
    "FileLocation",
    "FileSession",
    "FileSource",
    "LocalLocation",
    "LocalReader",
    "MissingError",
    "ObjectStore",
    "ReadError",
    "StoreReader",
    "allowed_roots",
    "check_read",
    "check_write",
    "configure_store_secrets",
    "connector_error",
    "create_scoped_secret",
    "list_objects",
    "load_store_extension",
    "locate_local",
    "open_file_session",
    "open_local_session",
    "open_store_reader",
    "path_problem",
    "read_files",
    "read_pages",
    "read_sql",
    "root_problems",
    "secret_parameters",
    "url_problem",
    "write_files",
]

# format -> COPY options (an option the output's `options` set is left to them); json and jsonl: one object a line
_COPY_FORMATS: Final = MappingProxyType({
    "csv": ("FORMAT CSV", "HEADER true"), "tsv": ("FORMAT CSV", "HEADER true", "DELIMITER '\t'"),
    "json": ("FORMAT JSON",), "jsonl": ("FORMAT JSON",), "parquet": ("FORMAT PARQUET",)})


# * -------------------------------------
# * the source engine: files as pages
# * -------------------------------------

def strip_root(location: str, root: str) -> str:
    """The text the `filename` option adds: the file's location below its root."""
    return location[len(root):]


def skip_extra_fields(location: str) -> tuple[str, dict[str, Any]]:
    return "", {}


@dataclass(frozen=True)
class FileSource:
    """Who reads (for messages and log lines): the connector, its service and what a file is called."""
    connector: str
    service: str = "object"
    noun: str = "object"
    clock: Callable[[], float] = time.monotonic
    name_of: Callable[[str, str], str] = strip_root
    extra_fields: Callable[[str], tuple[str, dict[str, Any]]] = skip_extra_fields


def connector_error(connector: str, exc: BaseException) -> ConnectorError | None:
    """The ConnectorError of a reading failure (codes NOT_FOUND, ACCESS_DENIED, READ_ERROR), or None."""
    codes = ((MissingError, "NOT_FOUND"), (ReadError, "READ_ERROR"), (AccessError, "ACCESS_DENIED"),
             (duckdb.Error, "READ_ERROR"), (OSError, "READ_ERROR"))
    code = next((code for kind, code in codes if isinstance(exc, kind)), None)
    return None if code is None else ConnectorError("%s: %s" % (connector, exc), code=code)


def _wrap_read_failure(connector: str, exc: AccessError | ReadError) -> ConnectorError:
    code = "ACCESS_DENIED" if isinstance(exc, AccessError) else "READ_ERROR"
    return ConnectorError("%s: %s" % (connector, exc), code=code)


def _format_where(where: Mapping[str, Any] | None) -> str:
    text = logs.describe(where) if where else ""
    return text + ": " if text else ""


def _report_missing(source: FileSource, path: Any, match: str | None, on_missing: str,
                    context: ConnectorContext) -> None:
    """Nothing matches a path: on_missing `error` fails the request, `skip` logs a warning."""
    what = "matches %r" % (path,) if match is None else "in %r matches %r" % (path, match)
    if on_missing == "error":
        raise ConnectorError("%s: no %s %s" % (source.connector, source.noun, what), code="NOT_FOUND")
    where = context.where
    context.log.warning("%s%s: no %s %s; skipping it (on_missing: skip)", _format_where(where), source.connector,
                        source.noun, what, extra=logs.fields(event="file_missing", connector=source.connector,
                                                             path=path, **(where or {})))


def _resolve(source: FileSource, reader: Any, arguments: Mapping[str, Any],
             context: ConnectorContext) -> list[tuple[str, str]]:
    """[(location, root)] of every file the call names, each once, checked before any is read."""
    path, match = arguments["path"], arguments.get("match")
    files: list[tuple[str, str]] = []
    for item in list(path) if isinstance(path, (list, tuple)) else [path]:
        try:
            found = reader.resolve(item) if match is None else reader.match(item, match,
                                                                             bool(arguments.get("recursive")))
        except (AccessError, ReadError) as exc:
            raise _wrap_read_failure(source.connector, exc) from None
        if not found:
            _report_missing(source, item, match, arguments.get("on_missing") or "skip", context)
        files.extend(entry for entry in found if entry[0] not in {location for location, _ in files})
    return files


def read_files(source: FileSource, reader: Any, arguments: Mapping[str, Any],
               context: ConnectorContext) -> Iterator[Page]:
    """
    The pages of every file a rendered, checked call names, file after file. `reader` resolves, matches and pages
    files: a StoreReader (or a class-based connector's FileReader).
    """
    file_format = arguments.get("format") or "auto"
    for location, root in _resolve(source, reader, arguments, context):
        kind = format_of(location) if file_format == "auto" else file_format
        if kind is None:
            raise ConnectorError("%s: cannot tell the format of %s from its extension: set `format`" % (
                source.connector, location), code="READ_ERROR")
        yield from _read_file(source, reader, (location, root), kind=kind, arguments=arguments, context=context)


def _read_file(source: FileSource, reader: Any, file: tuple[str, str], *, kind: str,
               arguments: Mapping[str, Any], context: ConnectorContext) -> Iterator[Page]:
    """The pages of one file; the first through context.call(). A URL that names no file is missing (on_missing)."""
    location, root = file
    options = arguments.get("options") or {}

    def start() -> tuple[Page | None, Iterator[Page] | None]:
        """The first page; failures are ConnectorErrors here, so the stream's retry policy sees their codes."""
        pages = reader.pages(location, kind, options, filename=source.name_of(location, root))
        try:
            return next(pages, None), pages
        except MissingError:
            return None, None
        except (AccessError, ReadError) as exc:
            raise _wrap_read_failure(source.connector, exc) from None

    rows, seconds, started = 0, 0.0, source.clock()
    try:
        page, pages = context.call(start)
        if pages is None:
            _report_missing(source, location, None, arguments.get("on_missing") or "skip", context)
            return
        seconds += source.clock() - started
        while page is not None:
            rows += len(page)
            yield page
            started = source.clock()
            page = next(pages, None)
            seconds += source.clock() - started
    except (AccessError, ReadError) as exc:
        raise _wrap_read_failure(source.connector, exc) from None
    text, fields = source.extra_fields(location)
    where = context.where or {}
    logs.NETWORK.info("%s%s read %s: %s row(s)%s, %.2f s", _format_where(where), source.connector, location,
                      f"{rows:,}", text, seconds,
                      extra=logs.fields(event="file_read", connector=source.connector, call="%s.read" % source.service,
                                        path=location, records=rows, duration_ms=round(seconds * 1000),
                                        **dict(fields, **where)))


@dataclass
class FileSession:
    """The contract v1 SourceSession of an object store: reads `object.read` calls inside the roots."""
    source: FileSource
    reader: StoreReader | LocalReader
    closed: bool = False

    def read(self, operation: Operation, arguments: Mapping[str, Any], context: ConnectorContext) -> Iterator[Page]:
        try:
            yield from read_files(self.source, self.reader, arguments, context)
        except (duckdb.Error, OSError) as exc:
            raise ConnectorError("%s: %s: %s" % (self.source.connector, operation.name, exc),
                                 code="READ_ERROR") from None

    def close(self) -> None:
        if not self.closed:
            self.reader.close()
            self.closed = True


def open_file_session(store: ObjectStore, connection: ResolvedConnection, context: ConnectorContext, *,
                      page_size: int = PAGE_SIZE) -> FileSession:
    """A session reading the store inside the connection's roots, with its credentials in a scoped DuckDB secret."""
    try:
        reader = open_store_reader(store, connection, page_size=page_size)
    except (AccessError, ReadError) as exc:
        raise ConnectorError(context.redact("%s: %s" % (store.provider, exc)), code="CONNECT_ERROR") from None
    return FileSession(FileSource(store.provider), reader)


def _relative_name(location: str, root: str) -> str:
    return os.path.relpath(location, root)


def _file_size(location: str) -> tuple[str, dict[str, Any]]:
    size = os.path.getsize(location)
    return ", %s bytes" % f"{size:,}", {"bytes": size}


def open_local_session(connection: ResolvedConnection, context: ConnectorContext, *,
                       page_size: int = PAGE_SIZE) -> FileSession:
    """A session reading local files inside the connection's roots; `filename` is the path below its root."""
    try:
        reader = open_local_reader(tuple(connection["roots"]), provider=connection.connector, page_size=page_size)
    except (AccessError, ReadError) as exc:
        raise ConnectorError(context.redact("%s: %s" % (connection.connector, exc)), code="CONNECT_ERROR") from None
    source = FileSource(connection.connector, noun="file", name_of=_relative_name, extra_fields=_file_size)
    return FileSession(source, reader)


# * -----------------------------------------
# * the duckdb engine: secrets, reads, writes
# * -----------------------------------------

def configure_store_secrets(store: ObjectStore, session: Any, name: str, connection: Mapping[str, Any]) -> None:
    """Loads the store's extension and creates one secret per root, scoped to it (values are bound parameters)."""
    session.load_extension(store.extension)
    parameters = secret_parameters(store, connection)
    for index, root in enumerate(FileLocation(store, tuple(connection["roots"])).roots):
        session.create_secret("%s_%d" % (name, index), store.secret_type, parameters, scope=root)


def read_sql(context: Any, arguments: Mapping[str, Any], *, locate: Callable[[str], str]) -> str:
    """
    The SQL of a file read step: its files (a path, a glob or `{{ step.column }}`) read as `format`; `locate` keeps
    every path inside the roots (e.g. a FileLocation's resolve_path; AccessError otherwise).
    """
    step = {key: value for key, value in arguments.items() if key != "format"}
    step.update(name=context.step["name"], read=arguments.get("format") or "auto")
    return compile_file_read(context, step, locate)


def locate_local(session: Any, roots: Iterable[str], path: str) -> str:
    """
    The location of a local file or folder: `path` (absolute, or relative to the first root) inside one of `roots`
    (relative to the job's folder); AdapterError otherwise.
    """
    inside = [os.path.normpath(session.local_path(root)) for root in roots]
    return check_location(path if os.path.isabs(path) else os.path.join(inside[0], path), inside)


def _format_copy_options(arguments: Mapping[str, Any], overwrite: bool) -> str:
    given = {str(key).upper() for key in arguments.get("options") or {}}
    options = [item for item in _COPY_FORMATS[arguments["format"]]
               if item.startswith("FORMAT ") or item.split(" ")[0] not in given]
    if arguments.get("partition_by"):
        options.append("PARTITION_BY (%s)" % ", ".join(quote_ident(column) for column in arguments["partition_by"]))
        if overwrite:
            options.append("OVERWRITE true")
    options += ["%s %s" % (quote_ident(key), sql_value(value))
                for key, value in sorted((arguments.get("options") or {}).items())]
    return ", ".join(options)


def _exists(con: Any, location: str) -> bool:
    return bool(con.execute("SELECT 1 FROM glob(?) LIMIT 1", [location]).fetchall())


def _prepare(con: Any, locations: list[str], mode: str, make_folders: bool) -> None:
    if mode == "error":
        taken = [location for location in locations if _exists(con, location)]
        if taken:
            raise FileExistsError("output exists already (mode error): %s" % ", ".join(taken[:5]))
    for location in locations if make_folders else ():
        folder = os.path.dirname(location)
        if folder:
            os.makedirs(folder, exist_ok=True)


def _select_splits(con: Any, source: str, split_by: list[str]) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
    columns = ", ".join(quote_ident(column) for column in split_by)
    rows = con.execute("SELECT DISTINCT %s FROM %s" % (columns, source)).fetchall()
    if any(value is None for row in rows for value in row):
        raise ValueError("a split column is null in some rows (%s); filter or coalesce it in the output's sql" %
                         ", ".join(split_by))
    return [(tuple(row), dict(zip(split_by, row, strict=True))) for row in rows]


def write_files(context: Any, arguments: Mapping[str, Any], *, locate: Callable[[str], str],
                make_folders: bool = False) -> WriteReceipt:
    """
    Writes context.source to `path` as `format`: `path` is the file, or with `file_name` (one file, or with split_by
    one per split) the folder the named files go into; `locate` keeps every path inside the roots (e.g. a
    FileLocation's resolve_path); `make_folders` creates local folders.
    """
    con, mode, split_by = context.session.con, arguments.get("mode") or "error", list(arguments.get("split_by") or ())
    named = bool(arguments.get("file_name"))
    root = locate(arguments["path"].rstrip("/") + "/" if named else arguments["path"])  # named files: a folder
    rows = con.execute("SELECT COUNT(*) FROM %s" % context.source).fetchall()[0][0]
    options = _format_copy_options(arguments, overwrite=mode == "overwrite")
    if not named:
        _prepare(con, [root], mode, make_folders and not arguments.get("partition_by"))
        if make_folders and arguments.get("partition_by"):
            os.makedirs(root, exist_ok=True)
        con.execute("COPY (SELECT * FROM %s) TO %s (%s)" % (context.source, quote_literal(root), options))
        return WriteReceipt(context.spec.name, root, rows, files=(root,))
    file_template = naming.parse_template(arguments["file_name"], split_by, name="file_name")
    layout = (naming.parse_template(arguments["layout"], split_by, allow_slash=True, name="layout")
              if arguments.get("layout") else None)
    splits = _select_splits(con, context.source, split_by) if split_by else [((), {})]  # unsplit: one file
    names = naming.plan_names(file_template, layout, splits, context.batch.batch_ts, context.batch.batch_id)
    targets = {key: locate(root.rstrip("/") + "/" + relative) for key, relative in names.items()}
    _prepare(con, list(targets.values()), mode, make_folders)
    for key, values in splits:
        where = " AND ".join("%s IS NOT DISTINCT FROM %s" % (quote_ident(column), sql_literal(values[column]))
                             for column in split_by) or "TRUE"
        con.execute("COPY (SELECT * FROM %s WHERE %s) TO %s (%s)" % (
            context.source, where, quote_literal(targets[key]), options))
    return WriteReceipt(context.spec.name, root, rows, files=tuple(sorted(targets.values())))
