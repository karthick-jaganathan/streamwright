"""
What the `source` and `duckdb` engines support of the database and table operations
(`streamwright.core.sdk.operations.databases` and `.tables`), declared once as their EngineCapabilities, and the
semantic checks of those operations' arguments - what the declarations cannot say. Spark's
(`streamwright.spark.io.database_rules`) reuse the checks.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Final

from streamwright.core.io.queries import WHOLE_REFERENCE, QueryPolicy, check_query, identifier_problem, where_problems
from streamwright.core.jobs.write_rules import OutputSpec, check_merge_clauses, check_overwrite_scope
from streamwright.core.runtime.contract import EngineCapabilities, OperationKind

__all__ = ["DUCKDB_CAPABILITIES", "DUCKDB_TABLE_MODES", "DUCKDB_WRITE_MODES", "QUERY_NAME", "SOURCE_CAPABILITIES",
           "TABLE_NAME", "TABLE_READ_NAME", "TABLE_WRITE_NAME", "WRITE_NAME", "check_database_write",
           "check_query_call", "check_table_call", "check_table_read", "check_table_write"]

QUERY_NAME: Final = "database.query"  # the catalog's names (streamwright.core.sdk.operations.databases, .tables)
TABLE_NAME: Final = "database.table"
WRITE_NAME: Final = "database.write"
TABLE_READ_NAME: Final = "table.read"
TABLE_WRITE_NAME: Final = "table.write"
DUCKDB_WRITE_MODES: Final = ("append", "overwrite", "merge")
DUCKDB_TABLE_MODES: Final = ("append", "overwrite", "merge")
KEYED_MODES: Final = ("merge", "delete")
_PARAMETER: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_GENERIC_POLICY: Final = QueryPolicy()


def _is_reference(value: Any) -> bool:
    return isinstance(value, str) and "{{" in value


def check_query_call(arguments: Mapping[str, Any]) -> list[str]:
    """`params` names are $name names; the query is one read-only SELECT using exactly them (dialects add rules)."""
    params = arguments.get("params")
    if isinstance(params, Mapping):
        wrong = [str(name) for name in params if not isinstance(name, str) or not _PARAMETER.match(name)]
        if wrong:
            return ["`params` names must be letters, digits and _ (not starting with a digit): %s" % ", ".join(wrong)]
    return check_query(arguments.get("query"), params if isinstance(params, Mapping) else {}, _GENERIC_POLICY)[1]


def _identifier_problems(arguments: Mapping[str, Any], keys: tuple[str, ...]) -> list[str]:
    problems = []
    for key in keys:
        value = arguments.get(key)
        if value is not None and not (_is_reference(value) and WHOLE_REFERENCE.match(value)):
            problems += ["`%s` %r %s" % (key, value, problem) for problem in [identifier_problem(value)] if problem]
    return problems


def _column_problems(arguments: Mapping[str, Any], key: str = "columns") -> list[str]:
    columns = arguments.get(key)
    if columns is None:
        return []
    if not columns:
        return ["`%s` must be a non-empty list of column names" % key]
    return ["%s: column %r %s" % (key, column, problem) for column in columns
            for problem in [identifier_problem(column) or (
                "is a reference: columns are literal names" if _is_reference(column) else None)] if problem]


def check_table_call(arguments: Mapping[str, Any]) -> list[str]:
    """The schema, table and column names, and the `where` conditions of a table read."""
    return (_identifier_problems(arguments, ("schema", "table")) + _column_problems(arguments) +
            where_problems(arguments.get("where")))


def check_database_write(arguments: Mapping[str, Any]) -> list[str]:
    """merge and delete need `primary_key`; `update` is merge's; key and update columns are column names."""
    mode = arguments.get("mode", "append")
    problems = []
    if mode in KEYED_MODES and not arguments.get("primary_key"):
        problems.append("mode %s needs `primary_key` (the key columns)" % mode)
    if arguments.get("update") is not None and mode != "merge":
        problems.append("`update` goes with mode merge")
    if arguments.get("staging_table") is not None and mode not in KEYED_MODES:
        problems.append("`staging_table` goes with mode merge or delete")
    return problems + _column_problems(arguments, "primary_key") + _column_problems(arguments, "update")


def check_table_read(arguments: Mapping[str, Any]) -> list[str]:
    """Column names, `where` conditions; `limit` and `version` are not negative."""
    problems = _column_problems(arguments) + where_problems(arguments.get("where"))
    return problems + ["`%s` must be a whole number >= 0" % key for key in ("limit", "version")
                       if isinstance(arguments.get(key), int) and arguments[key] < 0]


def check_table_write(arguments: Mapping[str, Any]) -> list[str]:
    """An overwrite names `replace_where` or the `whole_table`; a merge has its `condition` and valid when_* clauses."""
    keys = ("mode", "replace_where", "whole_table", "condition", "when_matched", "when_not_matched",
            "when_not_matched_by_source")
    values = {key: arguments[key] for key in keys if arguments.get(key) is not None}
    spec = OutputSpec.from_mapping(dict(values, name="output", mode=values.get("mode", "append")))
    return [problem.message for problem in check_overwrite_scope(spec) + check_merge_clauses(spec)]


SOURCE_CAPABILITIES: Final = EngineCapabilities(
    engine="source",
    operation_kinds=frozenset({OperationKind.READ}),
    arguments=MappingProxyType({
        QUERY_NAME: dict.fromkeys(("query", "params")),
        TABLE_NAME: dict.fromkeys(("table", "schema", "columns", "where")),
        TABLE_READ_NAME: dict.fromkeys(("path", "format", "columns", "where", "limit")),
    }),
    checks=MappingProxyType({QUERY_NAME: check_query_call, TABLE_NAME: check_table_call,
                             TABLE_READ_NAME: check_table_read}),
)

DUCKDB_CAPABILITIES: Final = EngineCapabilities(
    engine="duckdb",
    operation_kinds=frozenset({OperationKind.READ, OperationKind.WRITE}),
    arguments=MappingProxyType({
        QUERY_NAME: dict.fromkeys(("query",)),
        TABLE_NAME: dict.fromkeys(("table", "schema")),
        WRITE_NAME: dict(dict.fromkeys(("table", "primary_key")), mode=DUCKDB_WRITE_MODES),
        TABLE_READ_NAME: dict.fromkeys(("path", "format")),
        TABLE_WRITE_NAME: dict(dict.fromkeys((
            "path", "partition_by", "schema_evolution", "replace_where", "whole_table", "condition", "target_predicate",
            "when_matched", "when_not_matched", "when_not_matched_by_source")), mode=DUCKDB_TABLE_MODES),
    }),
    checks=MappingProxyType({TABLE_NAME: check_table_call, WRITE_NAME: check_database_write,
                             TABLE_WRITE_NAME: check_table_write}),
)
