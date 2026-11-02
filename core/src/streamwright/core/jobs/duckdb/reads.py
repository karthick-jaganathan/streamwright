#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

"""
Compiling a step's read into DuckDB SQL.

File reads (`read: parquet | csv | json | auto`) use the shared ingest planner (`streamwright.core.jobs.ingest`)
whenever a planning feature is used (`columns`, `column_groups`, `path_pattern`, `vocabulary`, `schema_mode: strict`
or `read: auto`): files are listed, their headers read, the plan pinned in the batch manifest (`ingest:<step>`) so a
replay reads the same files the same way, and every planned Group becomes one `read_*` call whose projection is
`build_projection`. Rejected files and schema reports are kept for the batch. Without a planning feature, the files
are read with one `read_*` call (unioned by name).

Paths may be `{{ step.column }}`: the distinct values of that column (pinned). Every path goes through the adapter's
`locate`, which keeps it inside the connection's roots. Values are always quoted, never spliced.
"""

import re
from types import MappingProxyType

from streamwright.core.io.file_formats import duckdb_option_name
from streamwright.core.jobs import ingest
from streamwright.core.jobs.refs import step_columns_used
from streamwright.core.sql.quote import quote_ident, quote_literal

__all__ = [
    "build_projection",
    "compile_file_read",
    "duckdb_type",
    "empty_projection",
    "quote_relation",
    "read_call",
    "relation_parts",
    "sql_value",
]

_FUNCTIONS = MappingProxyType({"parquet": "read_parquet", "csv": "read_csv", "tsv": "read_csv", "json": "read_json",
                               "jsonl": "read_json"})
_TYPES = MappingProxyType({"string": "VARCHAR", "int": "INTEGER", "bigint": "BIGINT", "long": "BIGINT",
                           "double": "DOUBLE", "float": "FLOAT", "boolean": "BOOLEAN", "date": "DATE",
                           "timestamp": "TIMESTAMP"})
_OPTION_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FILENAME = "filename"
_ORDINALITY = "ordinality"


def duckdb_type(kind):
    """The DuckDB type of an ingest type (string, int, bigint/long, double, float, boolean, date, timestamp,
    decimal(p,s))."""
    kind = str(kind).strip().lower()
    if not ingest.TYPE_PATTERN.match(kind):
        raise ingest.IngestError("type %r is not supported" % kind)
    return kind.upper() if kind.startswith("decimal") else _TYPES[kind]


def sql_value(value):
    """A read option's value as SQL: booleans, numbers, text, lists and mappings (as structs)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, (list, tuple)):
        return "[%s]" % ", ".join(sql_value(item) for item in value)
    if isinstance(value, dict):
        return "{%s}" % ", ".join("%s: %s" % (quote_literal(k), sql_value(v)) for k, v in value.items())
    return quote_literal(value)


def relation_parts(catalog, table, default_schema="main"):
    """(catalog, schema, name) of `table`: `name`, `schema.name`, or `catalog.schema.name` (overriding catalog)."""
    parts = str(table).split(".")
    if len(parts) > 3 or not all(parts):
        raise ValueError("invalid table name %r" % (table,))
    if len(parts) < 3:
        parts = [catalog] + (parts if len(parts) == 2 else [default_schema, *parts])
    return tuple(parts)


def quote_relation(catalog, table, default_schema="main"):
    """The quoted catalog.schema.table of `table` (see relation_parts)."""
    return ".".join(quote_ident(part) for part in relation_parts(catalog, table, default_schema))


def read_call(fmt, files, options):
    """`read_<fmt>([files], options...)` with the scan's row order (`ordinality`)."""
    rendered, names = [], set()
    for key, value in sorted(options.items()):
        if not isinstance(key, str) or not _OPTION_KEY.match(key):
            raise ingest.IngestError("invalid read option name %r" % (key,))
        names.add(duckdb_option_name(fmt, key))
        rendered.append("%s=%s" % (duckdb_option_name(fmt, key), sql_value(value)))
    if fmt == "tsv" and "delim" not in names and "sep" not in names:
        rendered.append("delim='\\t'")
    return "%s([%s]%s) WITH ORDINALITY" % (_FUNCTIONS[fmt], ", ".join(quote_literal(f) for f in files),
                                           "".join(", " + item for item in rendered))


def _row_index_sql(row_index):
    name, start = row_index
    return "row_number() OVER (PARTITION BY %s ORDER BY %s) - 1 + %d AS %s" % (
        quote_ident(_FILENAME), quote_ident(_ORDINALITY), int(start), quote_ident(name))


def _column_group_sql(name, group):
    kind = duckdb_type(group["type"])
    if not group["entries"]:
        return "map([]::VARCHAR[], []::%s[]) AS %s" % (kind, quote_ident(name))
    entries = ", ".join("{'key': %s, 'value': TRY_CAST(%s AS %s)}" % (quote_literal(key), quote_ident(cell), kind)
                        for cell, key in group["entries"])
    if group.get("keep_nulls"):
        return "map_from_entries([%s]) AS %s" % (entries, quote_ident(name))
    return "map_from_entries(list_filter([%s], lambda e: e.value IS NOT NULL)) AS %s" % (entries, quote_ident(name))


def build_projection(group, row_index=None):
    """The select list of one planned Group: mapped/typed columns, column-group maps, path variables, the file name
    and the per-file row index. Pure: no database access."""
    items = []
    for source, name, kind in group.columns:
        if kind:
            items.append("TRY_CAST(%s AS %s) AS %s" % (quote_ident(source), duckdb_type(kind), quote_ident(name)))
        else:
            items.append("%s AS %s" % (quote_ident(source), quote_ident(name)))
    for name in sorted(group.column_groups):
        items.append(_column_group_sql(name, group.column_groups[name]))
    if group.path_vars_spec:
        pattern, variables = group.path_vars_spec
        for name, index in sorted(variables.items(), key=lambda item: item[1]):
            items.append("regexp_extract(%s, %s, %d) AS %s" % (quote_ident(_FILENAME), quote_literal(pattern),
                                                               int(index), quote_ident(name)))
    items.append(quote_ident(_FILENAME))
    if row_index is not None:
        items.append(_row_index_sql(row_index))
    return items


def empty_projection(step, row_index):
    """The columns a planned read has when no file is read (an empty batch keeps its shape)."""
    rules, _ = ingest.normalize_columns(step.get("columns"))
    items = []
    for rule in rules or ():
        if rule.get("canonical"):
            items.append("CAST(NULL AS %s) AS %s" % (duckdb_type(rule.get("type", "string")),
                                                     quote_ident(rule["canonical"])))
    for group in step.get("column_groups") or ():
        items.append("map([]::VARCHAR[], []::%s[]) AS %s" % (duckdb_type(group.get("type", "string")),
                                                              quote_ident(group["name"])))
    if step.get("path_pattern"):
        for name in sorted(re.compile(step["path_pattern"]).groupindex):
            items.append("CAST(NULL AS VARCHAR) AS %s" % quote_ident(name))
    items.append("CAST(NULL AS VARCHAR) AS %s" % quote_ident(_FILENAME))
    if row_index is not None:
        items.append("CAST(NULL AS BIGINT) AS %s" % quote_ident(row_index[0]))
    seen, unique = set(), []
    for item in items:
        alias = item.rsplit(" AS ", 1)[1]
        if alias not in seen:
            seen.add(alias)
            unique.append(item)
    return "SELECT %s WHERE false" % ", ".join(unique)


def _paths(context, step, locate):
    path = step["path"]
    used = step_columns_used(path)
    if used:
        if path.strip() != "{{ %s.%s }}" % used[0]:
            raise ingest.IngestError("a dynamic path is exactly one {{ step.column }} reference")
        values = [v for v in context.values(*used[0]) if v is not None]
    else:
        values = [path]
    return [locate(str(value)) for value in values]


def _needs_plan(step):
    return (step["read"] == "auto" or step.get("schema_mode") == "strict" or
            any(step.get(key) for key in ("columns", "column_groups", "path_pattern", "vocabulary")))


def compile_file_read(context, step, locate):
    """The SQL of a file read step (see the module documentation)."""
    row_index = ingest.normalize_row_index(step.get("row_index"))
    options = dict(step.get("options") or {})
    pin = "ingest:%s" % step["name"]
    if pin in context.pinned:
        groups, rejects, reports = ingest.deserialize_plan(context.pinned[pin])
    else:
        uris = _paths(context, step, locate)
        if not uris:
            return empty_projection(step, row_index)
        if not _needs_plan(step):
            projection = "* EXCLUDE (%s)" % quote_ident(_ORDINALITY)
            plain = dict(options, union_by_name=True)
            if row_index is not None:
                projection += ", " + _row_index_sql(row_index)
                plain["filename"] = True
            return "SELECT %s FROM %s" % (projection, read_call(step["read"], uris, plain))
        groups, rejects, reports = _plan(context, step, uris, options, locate)
        context.pinned[pin] = ingest.serialize_plan(groups, rejects, reports)
    context.file_errors.extend((step["name"], r.path, r.reason) for r in rejects)
    context.schema_reports.extend(reports)
    if step.get("columns"):
        for group in groups:
            context.schema_signatures.append((group.prefix, ingest.compute_header_hash(group.header),
                                              ",".join(group.header)))
    if not groups:
        return empty_projection(step, row_index)
    selects = ["SELECT %s FROM %s" % (", ".join(build_projection(group, row_index)),
                                      read_call(group.format, group.files, dict(group.options, filename=True)))
               for group in groups]
    return " UNION ALL BY NAME ".join(selects)


def _plan(context, step, uris, options, locate):
    if step.get("vocabulary"):
        ingest.check_vocabulary(step.get("columns"), step["vocabulary"])
    con = context.session.con
    files = []
    for uri in uris:
        listed = [row[0] for row in con.execute("SELECT file FROM glob(?) ORDER BY file", [uri]).fetchall()]
        files.extend(locate(found) for found in listed)
    formats, rejects = ingest.resolve_file_formats(files, step["read"])
    headers = {}
    for path, fmt in formats.items():
        described = con.execute("DESCRIBE SELECT * FROM %s" % read_call(fmt, [path], options)).fetchall()
        headers[path] = [row[0] for row in described if row[0] not in (_FILENAME, _ORDINALITY)]
    groups, more_rejects, reports = ingest.plan_ingest(
        list(formats), mappings=step.get("columns"), headers=headers, formats=formats,
        schema_mode=step.get("schema_mode", "by_name"), on_unmapped_column=step.get("on_unmapped_column", "warn"),
        previous_signatures=context.previous_signatures, options=options,
        max_groups=int(context.limits.get("read_groups", 200)), step_name=step["name"],
        column_groups=step.get("column_groups"), path_pattern=step.get("path_pattern"))
    return groups, rejects + more_rejects, reports
