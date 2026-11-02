"""DuckDB-parser-backed SQL reference analysis shared by source and job planners."""

import json

import duckdb


class SqlAnalysisError(ValueError):
    """A query could not be parsed for table reads."""


def query_references(tree):
    """References in a serialized DuckDB parse tree, preserving qualified-table markers."""
    found = {"tables": set(), "ctes": set(), "table_functions": set(), "functions": set(), "parameters": []}

    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "BASE_TABLE" and "table_name" in node:
                qualified = bool(node.get("schema_name") or node.get("catalog_name"))
                found["tables"].add(("." if qualified else "") + str(node["table_name"]).lower())
            elif node.get("type") == "TABLE_FUNCTION":
                found["table_functions"].add(str((node.get("function") or {}).get("function_name")).lower())
            if node.get("class") == "FUNCTION" and node.get("function_name"):
                found["functions"].add(str(node["function_name"]).lower())
            if node.get("class") == "PARAMETER":
                found["parameters"].append(str(node.get("identifier")))
            cte_map = node.get("cte_map")
            if isinstance(cte_map, dict):
                found["ctes"].update(str(entry.get("key")).lower() for entry in cte_map.get("map") or [])
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(tree)
    return found


def sql_table_reads(sql):
    """Lower-case unqualified base tables read by a query, excluding CTE names."""
    with duckdb.connect() as con:
        tree = json.loads(con.execute("SELECT json_serialize_sql($sql)", {"sql": sql}).fetchone()[0])
    if tree.get("error"):
        raise SqlAnalysisError("cannot read the query: %s" % tree.get("error_message", "not a SELECT"))
    refs = query_references(tree)
    return {name for name in refs["tables"] - refs["ctes"] if not name.startswith(".")}
