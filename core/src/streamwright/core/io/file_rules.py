"""
What the `source` and `duckdb` engines support of the file operations (`streamwright.core.sdk.operations.files`),
declared once as their EngineCapabilities, and the semantic checks of those operations' arguments - what the
declarations cannot say. Importable without DuckDB (validation imports it), as `streamwright.spark.io.file_rules`.
"""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Final

from streamwright.core.io.file_formats import option_problems, regex_problem
from streamwright.core.jobs import naming
from streamwright.core.runtime.contract import EngineCapabilities, OperationKind

__all__ = ["DUCKDB_CAPABILITIES", "DUCKDB_WRITE_MODES", "ENGINE_CAPABILITIES", "SOURCE_CAPABILITIES", "check_read",
           "check_write"]

READ_NAME: Final = "object.read"  # the catalog's names (streamwright.core.sdk.operations.files)
WRITE_NAME: Final = "object.write"
DUCKDB_WRITE_MODES: Final = ("overwrite", "error")
INGEST_ARGUMENTS: Final = ("schema_mode", "columns", "column_groups", "vocabulary", "on_unmapped_column",
                           "path_pattern", "row_index")


def check_read(arguments: Mapping[str, Any]) -> list[str]:
    """
    The `options` are the format's reader options (the vocabulary of `file_formats.OPTIONS`); `recursive` goes with
    `match`; `match` is a literal regex; a list of paths names at least one.
    """
    problems = option_problems(arguments.get("format") or "auto", arguments.get("options"))
    if arguments.get("path") in ([], ()):
        problems.append("`path` is an empty list")
    if "match" in arguments:
        problem = regex_problem(arguments["match"])
        if problem:
            problems.append("`match` %r %s" % (arguments["match"], problem))
    if arguments.get("recursive") and arguments.get("match") is None:
        problems.append("`recursive` goes with `match` (the folder to look in is `path`)")
    if arguments.get("vocabulary") is not None and arguments.get("columns") is None:
        problems.append("`vocabulary` only applies together with `columns`")
    return problems


def check_write(arguments: Mapping[str, Any]) -> list[str]:
    """
    `file_name` (and `layout`, its folders) name the written files inside the folder `path`: one file, or with
    `split_by` one file per split (templates of streamwright.core.jobs.naming); checked before writing.
    """
    split_by, problems = arguments.get("split_by") or (), []
    named = arguments.get("file_name") or arguments.get("layout")
    if (split_by or named) and arguments.get("partition_by"):
        problems.append("split_by, file_name and layout (named files) and partition_by (Hive folders) are exclusive")
    if (split_by or arguments.get("layout")) and not arguments.get("file_name"):
        problems.append("split_by and layout need a file_name template, e.g. orders_{country}_{seq}.csv")
    for key, allow_slash in (("file_name", False), ("layout", True)):
        if arguments.get(key) is not None:
            try:
                naming.parse_template(arguments[key], list(split_by), allow_slash=allow_slash, name=key)
            except naming.NamingError as exc:
                problems.append(str(exc))
    return problems


SOURCE_CAPABILITIES: Final = EngineCapabilities(
    engine="source",
    operation_kinds=frozenset({OperationKind.READ}),
    arguments=MappingProxyType({
        READ_NAME: dict.fromkeys(("path", "format", "options", "on_missing", "match", "recursive")),
    }),
    checks=MappingProxyType({READ_NAME: check_read}),
)

DUCKDB_CAPABILITIES: Final = EngineCapabilities(
    engine="duckdb",
    operation_kinds=frozenset({OperationKind.READ, OperationKind.WRITE}),
    arguments=MappingProxyType({
        READ_NAME: dict.fromkeys(("path", "format", "options", *INGEST_ARGUMENTS)),
        WRITE_NAME: dict(dict.fromkeys(("path", "format", "options", "split_by", "layout", "file_name",
                                        "partition_by")), mode=DUCKDB_WRITE_MODES),
    }),
    checks=MappingProxyType({READ_NAME: check_read, WRITE_NAME: check_write}),
)

ENGINE_CAPABILITIES: Final = (SOURCE_CAPABILITIES, DUCKDB_CAPABILITIES)
