"""
A source's query must be ONE read-only SELECT. It is checked twice before it runs: its text (one statement, no write
keyword, no references or positional parameters) and DuckDB's parse tree (only the allowed table functions, only the
database's own tables - no DuckDB system view or schema, no file as a table - and no function that runs text of its
own). What a dialect allows differs through a frozen QueryPolicy. Also: the SELECT of a `database.table` read, its
identifier and condition checks, and statement timeouts.
"""

from __future__ import annotations

import decimal
import functools
import json
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final

import duckdb

from streamwright.core.sql.conditions import check_conditions, render_conditions
from streamwright.core.sql.quote import quote_ident

__all__ = ["ALIAS", "DEFAULT_QUERY_POLICY", "WRITE_KEYWORDS", "QueryPolicy", "check_query", "identifier_problem",
           "query_parameters", "select_query", "table_query", "timeout_ms", "where_problems"]

ALIAS: Final = "src"  # the attached database's name in DuckDB
# statements a read-only query never holds (DuckDB's parse refuses every other non-SELECT statement too); a column
# named like one must be double-quoted, e.g. "update"
WRITE_KEYWORDS: Final = ("INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "COPY", "CREATE", "DROP", "ALTER", "GRANT",
                         "REVOKE", "TRUNCATE", "CALL", "EXECUTE", "PREPARE", "DEALLOCATE", "ATTACH", "DETACH",
                         "INSTALL", "VACUUM", "CHECKPOINT", "PRAGMA", "REINDEX", "REFRESH", "LISTEN", "NOTIFY", "LOCK")
WHOLE_REFERENCE: Final = re.compile(r"^\s*\{\{[^{}]*\}\}\s*$")
_TIMEOUT_UNITS: Final = MappingProxyType({"ms": 1, "s": 1000, "min": 60000, "h": 3600000, None: 1000})
_TIMEOUT: Final = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(ms|s|min|h)?\s*$")
_DOLLAR_TAG: Final = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")
_STARTS: Final = ("SELECT", "WITH", "FROM", "VALUES")
_WRITE_PATTERN: Final = re.compile(r"\b(%s)\b" % "|".join(WRITE_KEYWORDS), re.IGNORECASE)


@dataclass(frozen=True)
class QueryPolicy:
    """
    What a query may use. `table_functions`: the table functions it may call; `denied_functions` and
    `denied_prefixes`: functions it never calls (they run text of their own, read files or settings);
    `system_schemas` / `system_prefixes`: DuckDB's own catalogs, schemas and system views, which are no tables of the
    database (some show its path, for Postgres the DSN); `catalog`: the one catalog it may name (the attached
    database), or None; `views`: bare names it may read besides the database's tables; `check_table_function`:
    why a call of an allowed table function (its name and parse-tree node) is refused, or None.
    """
    table_functions: tuple[str, ...] = ("unnest", "range", "generate_series")
    denied_functions: tuple[str, ...] = ("query", "query_table", "getenv", "glob", "current_setting")
    denied_prefixes: tuple[str, ...] = ("postgres_", "sqlite_", "mysql_", "read_", "parquet_", "iceberg_", "delta_",
                                        "duckdb_", "ducklake_", "pragma_")
    system_schemas: tuple[str, ...] = ("system", "temp", "memory", "pg_catalog", "information_schema")
    system_prefixes: tuple[str, ...] = ("duckdb_", "pragma_", "sqlite_")
    catalog: str | None = ALIAS
    views: tuple[str, ...] = ()
    check_table_function: Callable[[str, Mapping[str, Any]], str | None] | None = None

    def denied(self, name: str) -> bool:
        name = name.lower()
        return name in self.denied_functions or name.startswith(self.denied_prefixes)

    def table_function_problem(self, name: str, call: Mapping[str, Any]) -> str | None:
        """Why the call of the table function `name` (a parse-tree node) is refused, or None."""
        if self.denied(name):
            return "the query cannot call %s" % name
        if name.lower() not in self.table_functions:
            return "the query cannot call the table function %s (only %s)" % (name, ", ".join(self.table_functions))
        return self.check_table_function(name.lower(), call) if self.check_table_function else None

    def table_problem(self, catalog: str, schema: str, table: str) -> str | None:
        """Why a table reference (its catalog and schema empty when not written) is not a table, or None."""
        catalog, schema, name = catalog.lower(), schema.lower(), table.lower()
        if not catalog and not schema and name in self.views:
            return None
        if catalog and catalog != self.catalog:
            return "the query reads the database only: %r is not it (write schema.table or table)" % (catalog,)
        if re.search(r"[/\\:.]", table):
            return "the query cannot read files: %r is not a table name" % table
        if schema in self.system_schemas:  # (`system.x`: a catalog when the parser saw a schema)
            return "the query reads the database's tables only: %r is a system catalog or schema" % (schema,)
        if name.startswith(self.system_prefixes) or (not catalog and schema in ("", "main") and
                                                     name in _system_views()):
            return "the query reads the database's tables only: %r is a DuckDB system view" % (table,)
        return None


DEFAULT_QUERY_POLICY: Final = QueryPolicy()


def _bare_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:", config={"enable_external_access": False, "autoinstall_known_extensions": False,
                                               "autoload_known_extensions": False})


@functools.cache
def _system_views() -> frozenset[str]:
    """The names of DuckDB's system views a bare `name` (or `main.name`) reaches: system.main's and pg_catalog's."""
    connection = _bare_connection()
    try:
        return frozenset(row[0] for row in connection.execute(
            "SELECT DISTINCT lower(view_name) FROM duckdb_views() WHERE internal AND database_name = 'system' "
            "AND schema_name IN ('main', 'pg_catalog')").fetchall())
    finally:
        connection.close()


def _parse(text: str) -> dict[str, Any]:
    """DuckDB's parse of a query (json_serialize_sql: SELECT statements only; nothing runs), as a dict."""
    connection = _bare_connection()
    try:
        return json.loads(connection.execute("SELECT json_serialize_sql(?)", [text]).fetchone()[0])
    finally:
        connection.close()


def _walk(node: Any) -> Iterator[dict[str, Any]]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value)


def _skip_quoted(text: str, index: int) -> int:
    """The index after the quoted text (a string literal or quoted identifier) starting at `index`."""
    quote = text[index]
    escapes = quote == "'" and index > 0 and text[index - 1] in "eE" and (
        index < 2 or not (text[index - 2].isalnum() or text[index - 2] == "_"))
    index += 1
    while index < len(text):
        if escapes and text[index] == "\\":
            index += 2
            continue
        if text[index] == quote:
            if text.startswith(quote * 2, index):
                index += 2
                continue
            break
        index += 1
    return index + 1


def _skip_comment_or_dollar(text: str, index: int) -> int | None:
    """The index after a comment or dollar-quoted string starting at `index`, or None when none starts there."""
    if text.startswith("--", index):
        end = text.find("\n", index)
        return len(text) if end < 0 else end
    if text.startswith("/*", index):
        end = text.find("*/", index + 2)
        return len(text) if end < 0 else end + 2
    tag = _DOLLAR_TAG.match(text, index)
    if tag and (index == 0 or not (text[index - 1].isalnum() or text[index - 1] == "_")):
        end = text.find(tag.group(0), index + len(tag.group(0)))
        return len(text) if end < 0 else end + len(tag.group(0))
    return None


def _scan(text: str) -> tuple[str, list[int]]:
    """
    (code, semicolons): the text with string literals blanked to '', quoted identifiers to "", dollar-quoted strings
    to '' and comments to a space - what is left is SQL code - and the positions (in `text`) of the semicolons in it.
    """
    code, semicolons = [], []
    index = 0
    while index < len(text):
        character = text[index]
        skipped = _skip_comment_or_dollar(text, index) if character in "-/$" else None
        if skipped is not None:
            code.append("''" if character == "$" else " ")
            index = skipped
        elif character in "'\"":
            code.append(character * 2)
            index = _skip_quoted(text, index)
        else:
            if character == ";":
                semicolons.append(index)
            code.append(character)
            index += 1
    return "".join(code), semicolons


def _statement(text: str) -> tuple[str, list[str]]:
    """(the query without a trailing `;`, problems) by the text alone."""
    code, semicolons = _scan(text)
    problems = []
    if semicolons:
        last = semicolons[-1]
        rest, _ = _scan(text[last + 1:])
        if len(semicolons) > 1 or rest.strip():
            problems.append("the query must be a single statement (a `;` separates statements)")
        else:
            text = text[:last]
            code, _ = _scan(text)
    words = sorted({match.upper() for match in _WRITE_PATTERN.findall(code)})
    if words:
        problems.append("the query is read-only: it cannot use %s (a column named like one must be double-quoted, "
                        "e.g. \"update\")" % ", ".join(words))
    first = code.strip().lstrip("(").strip().split(None, 1)
    if not first or first[0].upper() not in _STARTS:
        problems.append("the query must be a SELECT (it starts with %s)" % (
            ", ".join(_STARTS) if not first else repr(first[0][:20])))
    if "?" in code:
        problems.append("the query must use named parameters ($name, declared in `params`), not ?")
    return text.strip(), problems


def _node_problem(node: dict[str, Any], policy: QueryPolicy, table_calls: set[int]) -> str | None:
    kind = node.get("type")
    if kind == "TABLE_FUNCTION":
        call = node.get("function") or {}
        table_calls.add(id(call))
        return policy.table_function_problem(str(call.get("function_name") or ""), call)
    if kind == "BASE_TABLE":
        return policy.table_problem(str(node.get("catalog_name") or ""), str(node.get("schema_name") or ""),
                                    str(node.get("table_name") or ""))
    if kind == "SHOW_REF":  # (SHOW, DESCRIBE, SUMMARIZE: of any table or view, system views too)
        return "the query cannot use SHOW, DESCRIBE or SUMMARIZE"
    name = node.get("function_name")
    if isinstance(name, str) and id(node) not in table_calls and policy.denied(name):
        return "the query cannot call %s" % name
    return None


def _tree_problems(tree: Any, policy: QueryPolicy) -> tuple[list[str], list[str]]:
    """(problems, parameter names) from DuckDB's parse of a query."""
    names, refused, table_calls = [], set(), set()
    for node in _walk(tree):
        problem = _node_problem(node, policy, table_calls)
        if problem:
            refused.add(problem)
        if node.get("class") == "PARAMETER":
            names.append(str(node.get("identifier")))
    problems = sorted(refused)
    if any(name.isdigit() for name in names):
        problems.append("the query must use named parameters ($name, declared in `params`), not $1 or ?")
    return problems, sorted(set(names))


def _parameter_problems(names: list[str], params: Any) -> list[str]:
    declared = sorted(params) if isinstance(params, Mapping) else []
    missing = [name for name in names if name not in declared]
    unused = [name for name in declared if name not in names]
    problems = []
    if missing:
        problems.append("the query uses $%s: declare %s in `params`" % (
            ", $".join(missing), "it" if len(missing) == 1 else "them"))
    if unused:
        problems.append("`params` %s not used in the query (write $%s in it)" % (
            ", ".join(unused) + (" is" if len(unused) == 1 else " are"), ", $".join(unused)))
    return problems


def check_query(text: Any, params: Mapping[str, Any] | None = None,
                policy: QueryPolicy = DEFAULT_QUERY_POLICY) -> tuple[Any, list[str]]:
    """
    (the query to run: without a trailing `;`, problems): the problems are why `text` is not a single read-only SELECT
    under `policy` whose named parameters ($name) are the names in `params` (a mapping, or None to not compare them).
    """
    if not isinstance(text, str) or not text.strip():
        return text, ["`query` must be a SELECT statement (a non-empty text)"]
    if "{{" in text:
        return text, ["the query cannot hold references ({{ ... }}): pass values in `params` and write $name in "
                      "the query, so they are bound, never pasted into it"]
    if "\x00" in text:
        return text, ["the query has a NUL character"]
    statement, problems = _statement(text)
    if problems:
        return statement, problems
    tree = _parse(statement)
    if tree.get("error"):
        detail = tree.get("error_message") or "it cannot be parsed"
        return statement, ["the query must be a single SELECT: %s" % (
            "it is not a SELECT statement" if "Only SELECT" in detail else detail)]
    if len(tree.get("statements") or []) != 1:
        return statement, ["the query must be a single statement"]
    problems, names = _tree_problems(tree["statements"], policy)
    if params is not None and not problems:
        problems = _parameter_problems(names, params)
    return statement, problems


def query_parameters(text: str, policy: QueryPolicy = DEFAULT_QUERY_POLICY) -> list[str]:
    """The named parameters ($name) of a query that passes check_query()."""
    return _tree_problems(_parse(text).get("statements") or [], policy)[1]


def identifier_problem(name: Any, limit: int | None = None) -> str | None:
    """Why `name` cannot be a schema, table or column name, or None (it is quoted, so any text but NUL works)."""
    if not isinstance(name, str) or not name.strip():
        return "is not a name (a non-empty text)"
    if "\x00" in name:
        return "has a NUL character"
    if limit and len(name.encode("utf-8")) > limit:
        return "is longer than %d bytes" % limit
    return None


def where_problems(where: Any) -> list[str]:
    """Problems with a `where` (conditions joined with AND; the shared operator vocabulary); a reference: later."""
    if where is None or (isinstance(where, str) and WHOLE_REFERENCE.match(where)):
        return []
    return check_conditions(where)


def select_query(relation: str, columns: Sequence[str] | None = None, where: Sequence[Mapping[str, Any]] | None = None,
                 limit: int | None = None) -> tuple[str, dict[str, Any]]:
    """
    (query, parameters) of a read of `relation` (SQL text: a quoted table, a table function call): SELECT the columns
    (default: all) WHERE every condition holds (sql.conditions: each value a bound parameter, cast to the condition's
    `type` when it has one), at most `limit` rows.
    """
    names = ", ".join(quote_ident(column) for column in columns) if columns else "*"
    condition, parameters = render_conditions(list(where or []), dialect="duckdb")
    text = "SELECT %s FROM %s%s" % (names, relation, condition)
    return (text + " LIMIT %d" % int(limit) if limit is not None else text), parameters


def table_query(table: str, schema: str | None = None, columns: Sequence[str] | None = None,
                where: Sequence[Mapping[str, Any]] | None = None) -> tuple[str, dict[str, Any]]:
    """(query, parameters) of a table read (see select_query)."""
    return select_query(quote_ident(table) if not schema else "%s.%s" % (quote_ident(schema), quote_ident(table)),
                        columns, where)


def timeout_ms(value: Any) -> int:
    """A statement timeout (seconds as a number, or text such as 500ms, 30s, 5min, 1h) in milliseconds; ValueError."""
    if isinstance(value, bool):
        raise ValueError("expected a number of seconds or a text such as 30s, 500ms, 5min")
    if isinstance(value, (int, float, decimal.Decimal)):
        milliseconds = float(value) * 1000
    else:
        match = _TIMEOUT.match(str(value))
        if not match:
            raise ValueError("%r is not a timeout (a number of seconds, or e.g. 30s, 500ms, 5min, 1h)" % (value,))
        milliseconds = float(match.group(1)) * _TIMEOUT_UNITS[match.group(2)]
    if milliseconds < 1:
        raise ValueError("a statement timeout must be at least 1ms")
    return round(milliseconds)
