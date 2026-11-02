"""File formats and read options: what a file read may ask for, and the DuckDB read query it becomes."""

import re
from types import MappingProxyType
from typing import Final

from streamwright.core.sql.quote import sql_string

__all__ = ["COMPRESSIONS", "FORMATS", "ON_MISSING", "OPTIONS", "AccessError", "MissingError", "ReadError",
           "duckdb_option_name", "file_query", "format_of", "option_problems", "regex_problem"]

FORMATS: Final = ("csv", "tsv", "json", "jsonl", "parquet")
COMPRESSIONS: Final = ("auto", "none", "gzip", "zstd")
ON_MISSING: Final = ("skip", "error")
# file name endings -> format (a compression ending first is ignored: a.csv.gz is csv)
_EXTENSIONS: Final = MappingProxyType({".csv": "csv", ".tsv": "tsv", ".tab": "tsv", ".json": "json", ".jsonl": "jsonl",
                                       ".ndjson": "jsonl", ".parquet": "parquet", ".pq": "parquet"})
_COMPRESSED: Final = (".gz", ".gzip", ".zst", ".zstd")
# DuckDB's names for read_json_objects' compression (read_csv takes ours as they are)
_JSON_COMPRESSIONS: Final = MappingProxyType({"auto": "auto_detect", "none": "uncompressed", "gzip": "gzip",
                                             "zstd": "zstd"})
_CSV_NAMES: Final = MappingProxyType({"delimiter": "delim"})  # option -> read_csv's parameter
_TYPE: Final = re.compile(r"^[A-Za-z][A-Za-z0-9_ ]*(\([0-9 ,]+\))?(\[\])?$")  # a column type, e.g. DECIMAL(12,2)


class AccessError(Exception):
    """A read the allowed roots do not allow, refused before anything is read."""


class ReadError(Exception):
    """A file that cannot be read: its format, options or contents; a request that cannot be served."""


class MissingError(ReadError):
    """A URL that names no object (object storage is only asked when the object is read)."""


# * --------------------
# * formats and options
# * --------------------

def _boolean(value):
    return None if isinstance(value, bool) else "expected true or false"


def _text(value):
    return None if isinstance(value, str) and value != "" else "expected a non-empty text"


def _count(value):
    ok = isinstance(value, int) and not isinstance(value, bool) and value >= 0
    return None if ok else "expected a whole number >= 0"


def _one_of(*choices):
    def check(value):
        return None if value in choices else "expected one of: %s" % ", ".join(choices)
    return check


def _texts(value):
    if isinstance(value, str) or (isinstance(value, list) and value and all(isinstance(item, str) for item in value)):
        return None
    return "expected a text or a list of texts"


def _columns(value):
    if not isinstance(value, dict) or not value:
        return "expected a mapping of column names to DuckDB types, e.g. {id: BIGINT, amount: DECIMAL(12,2)}"
    wrong = [str(name) for name, kind in value.items() if not isinstance(kind, str) or not _TYPE.match(kind.strip())]
    return "not a column type: %s" % ", ".join(wrong) if wrong else None


_CSV_OPTIONS: Final = MappingProxyType({
    "header": _boolean, "delimiter": _text, "quote": _text, "escape": _text, "columns": _columns,
    "compression": _one_of(*COMPRESSIONS), "null_padding": _boolean, "ignore_errors": _boolean, "skip": _count,
    "nullstr": _texts, "all_varchar": _boolean, "dateformat": _text, "timestampformat": _text, "filename": _boolean})
_JSONL_OPTIONS: Final = MappingProxyType({"compression": _one_of(*COMPRESSIONS), "ignore_errors": _boolean,
                                         "filename": _boolean})
# format -> option -> check(value): the options whose meaning or types we translate, kept typed. This is NOT an
# allow-list: an option absent here is forwarded verbatim to the engine's reader (DuckDB/Spark is the authority).
OPTIONS: Final = MappingProxyType({
    "csv": _CSV_OPTIONS, "tsv": _CSV_OPTIONS,
    "json": MappingProxyType(dict(_JSONL_OPTIONS, format=_one_of("auto", "array", "newline_delimited"))),
    "jsonl": _JSONL_OPTIONS, "parquet": MappingProxyType({"filename": _boolean})})


def format_of(path):
    """A file's format from its name (a.csv, a.jsonl.gz), or None."""
    name = str(path).rsplit("/", 1)[-1].lower()
    for ending in _COMPRESSED:
        if name.endswith(ending):
            name = name[:-len(ending)]
            break
    dot = name.rfind(".")
    return _EXTENSIONS.get(name[dot:]) if dot > 0 else None


def _has_reference(value):
    if isinstance(value, str):
        return "{{" in value
    if isinstance(value, dict):
        return any(isinstance(item, str) and "{{" in item for item in value.values())
    return False


def option_problems(file_format, options):
    """
    Problems with the reader options of a format ("auto": options some format takes). Options we translate or type
    (`file_formats.OPTIONS`) are checked; an unknown option is forwarded verbatim to the engine's reader, which is
    the authority (contract v1.2 §6a). `columns` is inlined into the read query as a STRUCT literal, so it alone must
    be a literal, never a reference.
    """
    if options is None:
        return []
    if not isinstance(options, dict):
        return ["`options` must be a mapping of options"]
    known = OPTIONS.get(file_format)
    problems = []
    for name, value in options.items():
        if name == "columns" and _has_reference(value):
            problems.append("option 'columns': takes literal values, not references (it is inlined into the query)")
            continue
        checks = [known.get(name)] if known is not None else [table[name] for table in OPTIONS.values()
                                                              if name in table]
        checks = [check for check in checks if check]
        if not checks:
            continue  # an unknown option: forwarded to the engine's reader
        messages = [check(value) for check in checks]
        if all(messages):
            problems.append("option %r: %s" % (name, messages[0]))
    return problems


def regex_problem(pattern):
    """Why a `match` regex cannot be used, or None: not a non-empty text, a reference, or not a valid regex."""
    if not isinstance(pattern, str) or not pattern:
        return "must be a non-empty text (a regex)"
    if "{{" in pattern:
        return "is a literal regex: it takes no references"
    try:
        re.compile(pattern)
    except (re.error, OverflowError, RecursionError) as exc:
        return "is not a valid regex: %s" % exc
    return None


def _arguments(values):
    """(SQL of named arguments `, name=?`, parameters) of a table function."""
    return "".join(", %s=?" % name for name in values), list(values.values())


def duckdb_option_name(file_format, name):
    """The DuckDB read_* parameter of an option: a shared-vocabulary name translated (csv `delimiter` -> `delim`),
    any other (vendor) name verbatim. Every DuckDB reader uses it, so the translation does not depend on the DuckDB
    version's aliases."""
    return _CSV_NAMES.get(name, name) if file_format in ("csv", "tsv") else name


def file_query(location, file_format, options=None):
    """
    (query, parameters) that read one file of a format (not auto) with its options (without `filename`, which the
    reader adds): each row a JSON text, a record. The location is a bound parameter.
    """
    options = {name: value for name, value in (options or {}).items() if name != "filename"}
    if file_format in ("csv", "tsv"):
        values = {}
        if file_format == "tsv":
            values["delim"] = "\t"
        values["all_varchar"] = "columns" not in options
        values["hive_partitioning"] = False
        columns = options.pop("columns", None)
        for name, value in options.items():
            values[duckdb_option_name(file_format, name)] = value
        names, parameters = _arguments(values)
        if columns:  # (a constant STRUCT, which DuckDB takes as no parameter; types are checked by option_problems)
            names += ", columns={%s}" % ", ".join("%s: %s" % (sql_string(name), sql_string(kind.strip()))
                                                  for name, kind in columns.items())
        return "SELECT to_json(t) FROM read_csv(?%s) t" % names, [[location], *parameters]
    if file_format in ("json", "jsonl"):
        values = {"format": "newline_delimited" if file_format == "jsonl" else options.pop("format", "auto")}
        for name, value in options.items():
            values[name] = _JSON_COMPRESSIONS[value] if name == "compression" else value
        names, parameters = _arguments(values)
        return "SELECT json FROM read_json_objects(?%s)" % names, [[location], *parameters]
    if file_format == "parquet":
        names, parameters = _arguments(dict({"hive_partitioning": False}, **options))
        return "SELECT to_json(t) FROM read_parquet(?%s) t" % names, [[location], *parameters]
    raise ReadError("unknown file format %r (formats: %s)" % (file_format, ", ".join(FORMATS)))
