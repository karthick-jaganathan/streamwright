"""Shared validation and parameterized rendering for connector table filters."""

import re

from streamwright.core.sql.quote import quote_ident

VALUELESS = frozenset({"IS NULL", "IS NOT NULL"})
COMPARISONS = frozenset({"=", "!=", "<>", "<", "<=", ">", ">=", "LIKE", "NOT LIKE"})
MEMBERSHIP = frozenset({"IN", "NOT IN"})
OPERATORS = COMPARISONS | MEMBERSHIP | VALUELESS
DIALECTS = ("postgres", "mysql", "duckdb", "spark")
_EXPANDED_MEMBERSHIP = ("mysql", "spark")     # one placeholder per IN member; the others bind a list (= ANY)
_TYPE = re.compile(r"[A-Za-z_][A-Za-z_0-9]*(?:\(\d+(?:,\s*\d+)?\))?(?:\[\])?\Z")


def check_conditions(where, *, operators=OPERATORS):
    """Return all structural and operator errors for a list of {column, op, value, type}."""
    if where is None:
        return []
    if not isinstance(where, list):
        return ["`where` must be a list of conditions"]
    problems = []
    for index, condition in enumerate(where):
        at = "`where`[%d]" % index
        if not isinstance(condition, dict):
            problems.append("%s must be a mapping {column, op, value, type}" % at)
            continue
        unknown = set(condition) - {"column", "op", "value", "type"}
        if unknown:
            problems.append("%s has unknown keys: %s" % (at, ", ".join(sorted(map(str, unknown)))))
        try:
            if not isinstance(condition.get("column"), str):
                raise ValueError("column must be a string")
            quote_ident(condition["column"])
        except ValueError:
            problems.append("%s.column must be a nonempty identifier" % at)
        operator = str(condition.get("op", "=")).upper()
        if operator not in operators:
            problems.append("%s.op %r is unsupported" % (at, operator))
        else:
            if operator in VALUELESS and "value" in condition:
                problems.append("%s: %s takes no value" % (at, operator))
            if operator not in VALUELESS and "value" not in condition:
                problems.append("%s: %s needs a value" % (at, operator))
            if operator in MEMBERSHIP and "value" in condition and not isinstance(condition["value"], (list, tuple)):
                problems.append("%s: %s needs a list value" % (at, operator))
        if "type" in condition and (not isinstance(condition["type"], str) or
                                     _TYPE.fullmatch(condition["type"].strip()) is None):
            problems.append("%s.type must be a SQL column type" % at)
    return problems


def render_conditions(where, *, dialect="postgres"):
    """
    Render a WHERE fragment and parameter mapping; values are never interpolated into SQL. Placeholders are
    `$name`; identifiers are double-quoted (a caller with another quote character converts them).
    """
    if dialect not in DIALECTS:
        raise ValueError("unsupported condition dialect %r" % dialect)
    problems = check_conditions(where)
    if problems:
        raise ValueError("; ".join(problems))
    clauses, parameters = [], {}
    for index, condition in enumerate(where or ()):
        column = quote_ident(condition["column"])
        operator = str(condition.get("op", "=")).upper()
        if operator in VALUELESS:
            clauses.append("%s %s" % (column, operator))
            continue
        key = ("p_" if dialect in _EXPANDED_MEMBERSHIP else "w") + str(index)
        value = condition["value"]
        if operator in MEMBERSHIP and dialect in _EXPANDED_MEMBERSHIP:
            members = list(value)
            if not members:
                clauses.append("FALSE" if operator == "IN" else "TRUE")
                continue
            placeholders = []
            for position, member in enumerate(members):
                member_key = "%s_%d" % (key, position)
                parameters[member_key] = member
                placeholder = "$" + member_key
                placeholders.append("CAST(%s AS %s)" % (placeholder, condition["type"].strip())
                                    if condition.get("type") else placeholder)
            clauses.append("%s %s (%s)" % (column, operator, ", ".join(placeholders)))
            continue
        parameters[key] = list(value) if isinstance(value, tuple) else value
        placeholder = "$" + key
        kind = condition.get("type")
        if kind:
            placeholder = "CAST(%s AS %s%s)" % (placeholder, kind.strip(), "[]" if operator in MEMBERSHIP else "")
        if operator in MEMBERSHIP and dialect in ("postgres", "duckdb"):
            expression = "%s = ANY(%s)" % (column, placeholder)
            clauses.append(expression if operator == "IN" else "NOT (%s)" % expression)
        else:
            clauses.append("%s %s %s" % (column, operator, placeholder))
    return (" WHERE " + " AND ".join(clauses) if clauses else "", parameters)
