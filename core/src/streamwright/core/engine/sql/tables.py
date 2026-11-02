#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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
"""The request tables of a stream: their columns, names, partition conditions and `$name` parameter types."""

import datetime
import json
import re
from types import MappingProxyType

from streamwright.core.engine.sql.types import _schema
from streamwright.core.runtime.templates import to_json

COLUMNS = (("record", "JSON"), ("partition", "JSON"), ("config", "JSON"), ("window_start", "DATE"),
           ("window_end", "DATE"), ("today", "DATE"))
LIMITS = MappingProxyType({"memory_limit": "1GB", "threads": 1})
TIMEOUT = 60.0             # seconds for the steps of one page (page mode), and for compiling a query
TRANSFORM_TIMEOUT = 600.0  # seconds for one step of a run-mode stream
MAX_ROWS = 1000000         # rows a page-mode step can make from one page
MODES = ("page", "run")
# table functions a query can use (no others: they read settings, files or change the connection)
TABLE_FUNCTIONS = ("range", "generate_series", "unnest", "json_each", "json_tree")
# functions that read or change the connection's settings or logs (built-in macros that run a subquery, which could
# read other table functions, are denied too: Sandbox.denied)
DENIED_FUNCTIONS = ("current_setting", "getvariable", "getenv", "enable_logging", "disable_logging",
                    "truncate_duckdb_logs", "write_log", "query", "query_table")
# the DuckDB type of a config input (by its spec.config type) as a `$name` parameter; a list is a LIST of its items'
PARAMETER_TYPES = MappingProxyType({"string": "VARCHAR", "integer": "BIGINT", "number": "DOUBLE", "boolean": "BOOLEAN",
                                    "date": "DATE"})
# the values plain casts are compiled with (a NULL folds into an untyped constant): their types are the values'
_SAMPLES = MappingProxyType({"VARCHAR": "x", "BIGINT": 1, "DOUBLE": 1.0, "BOOLEAN": True,
                             "DATE": datetime.date(2000, 1, 1)})
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SqlError(Exception):
    """A step's query that cannot run."""


def _message(exc):
    """DuckDB's message without its query excerpt: the error and its hints, on one line."""
    text = str(exc).strip().split("\n\n")[0]
    return " ".join(line.strip() for line in text.splitlines() if line.strip()) or type(exc).__name__


def _json_text(value):
    return to_json(value, default=lambda item: item.isoformat() if hasattr(item, "isoformat") else str(item))


def as_json(value):
    """A value as it reads back from a JSON column (dates become text, decimals numbers)."""
    return json.loads(_json_text(value))


def _checked(name):
    """A request, step or column name, which can be a table or column name: letters, digits and _."""
    if not (isinstance(name, str) and _NAME.match(name)):
        raise SqlError("%r is not a valid table or column name" % (name,))
    return name


def _quoted(name):
    """The SQL identifier of a request, step or column name."""
    return '"%s"' % _checked(name)


def _identifier(name):
    """Any column name as a SQL identifier (a step's columns can have any names)."""
    return '"%s"' % str(name).replace('"', '""')


def _column_problem(names):
    """Why result columns with these names cannot be output fields, or None."""
    for name in names:
        if not _NAME.match(name):
            return ("column %r is not a valid field name (letters, digits and _, not starting with a digit): name it "
                    "with AS" % name)
    # DuckDB compares names without case (a table keeps one of `id` and `Id`), and so do most warehouses
    lowered = [name.lower() for name in names]
    repeated = sorted({name for name in names if lowered.count(name.lower()) > 1})
    if repeated:
        return "columns named more than once: %s%s" % (", ".join(repeated), " (names ignore case)" if len(
            repeated) > len({name.lower() for name in repeated}) else "")
    return None


def _pointer(key):
    """The JSON pointer (RFC 6901) of a key of a JSON object: any key, with no quoting rules to get wrong."""
    return "/" + key.replace("~", "~0").replace("/", "~1")


def _partition_condition(partition):
    """
    (SQL, parameters) that select the raw rows read for a stream partition: their `partition` has its keys, with
    values that are equal as JSON ("1" is not 1). The values are parameters, never SQL text.
    """
    if not partition:
        return "true", {}
    conditions, params = [], {"scope": _json_text(partition)}
    for position, key in enumerate(partition):
        params["key%d" % position] = _pointer(str(key))
        conditions.append('json_extract("partition", $key%d) = json_extract($scope::JSON, $key%d)' % (
            position, position))
    return " AND ".join(conditions), params


def _path_text(path):
    """("transform", "steps", 0, "select") -> "transform.steps[0].select"."""
    text = ""
    for part in path:
        text += "[%d]" % part if isinstance(part, int) else ("." if text else "") + str(part)
    return text


def table_schema(columns):
    """The JSON Schema of records with these (name, DuckDB type) columns."""
    return {"type": "object", "properties": {name: _schema(kind) for name, kind in columns}}


def param_type(definition):
    """The DuckDB type of a config input (its spec.config entry) as a `$name` parameter, or None if it has none."""
    if not isinstance(definition, dict):
        return None
    if definition.get("type") == "list":
        item = PARAMETER_TYPES.get(definition.get("items") or "string")
        return item + "[]" if item else None
    return PARAMETER_TYPES.get(definition.get("type"))


def _sample(kind):
    """A value of a parameter type (PARAMETER_TYPES, or a list of one), to compile a plain cast with."""
    return [_sample(kind[:-2])] if kind.endswith("[]") else _SAMPLES[kind]
