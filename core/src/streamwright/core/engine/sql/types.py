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
"""DuckDB types as JSON Schema, and DuckDB values as plain (JSON) values."""

import base64
import datetime
import decimal
import json
import math
import re
import uuid

_ARRAYS = re.compile(r"(\[\d*\])+$")  # the list and array suffixes of a type: TIMESTAMP WITH TIME ZONE[]
_MULTIWORD_TYPES = ("TIMESTAMP WITH TIME ZONE", "TIME WITH TIME ZONE", "DOUBLE PRECISION")
_INTEGERS = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
             "UHUGEINT")


# * ------------------------------------------
# * DuckDB types: JSON Schema and plain values
# * ------------------------------------------

def _split(text):
    """Splits a type's arguments at the commas outside parentheses, brackets and quotes ("names", 'enum values')."""
    parts, depth, quote, current = [], 0, None, ""
    for char in text:
        if quote is not None:
            if char == quote:
                quote = None  # a doubled quote inside a name or value closes and reopens it
        elif char in "\"'":
            quote = char
        elif char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        if char == "," and depth == 0 and quote is None:
            parts.append(current.strip())
            current = ""
        else:
            current += char
    if current.strip():
        parts.append(current.strip())
    return parts


def _field(text):
    """(name, type) of a STRUCT field: `"b c" INTEGER[]` or `raw JSON`."""
    if text.startswith('"'):
        end = 1
        while end < len(text):
            if text[end] == '"' and text[end + 1:end + 2] != '"':
                break
            end += 2 if text[end] == '"' else 1
        return text[1:end].replace('""', '"'), text[end + 1:].strip()
    name, _, kind = text.partition(" ")
    return name, kind.strip()


def _unnamed(fields):
    """Whether STRUCT fields are bare types, as from row(1, 'x'): its values are tuples."""
    return all(" " not in field.split("(")[0] or _ARRAYS.sub("", field.upper()) in _MULTIWORD_TYPES
               for field in fields)


def _unnamed_struct(kind):
    """Whether a type has an unnamed STRUCT (row(1, 'x') or (1, 'x')) at any depth: DuckDB's tables cannot hold one."""
    shape = _shape(kind)
    if shape[0] == "list":
        return _unnamed_struct(shape[1])
    if shape[0] == "map":
        return _unnamed_struct(shape[1]) or _unnamed_struct(shape[2])
    if shape[0] in ("struct", "union"):
        if shape[0] == "struct" and _unnamed(shape[1]):
            return True
        return any(_unnamed_struct(_field(field)[1]) for field in shape[1])
    return False


def _shape(kind):
    """("list", item) | ("struct", fields) | ("map", key, value) | ("union", members) | ("json",) | ("scalar", KIND)."""
    kind = kind.strip()
    upper = kind.upper()
    if upper.endswith("]") and "[" in kind:  # LIST (INTEGER[]) and ARRAY (INTEGER[3])
        return ("list", kind[:kind.rfind("[")])
    for name in ("STRUCT", "MAP", "UNION", "TUPLE"):
        if upper.startswith(name + "(") and upper.endswith(")"):
            arguments = _split(kind[len(name) + 1:-1])
            if name == "MAP":
                return ("map", *tuple(([*arguments, "", ""])[:2]))
            return ("struct" if name == "TUPLE" else name.lower(), tuple(arguments))
    return ("json",) if upper == "JSON" else ("scalar", upper)


def _schema(kind):
    """The JSON Schema of a DuckDB column type (all columns are nullable)."""
    shape = _shape(kind)
    if shape[0] == "list" or (shape[0] == "struct" and _unnamed(shape[1])):
        return {"type": ["null", "array"]}
    if shape[0] in ("struct", "map"):
        return {"type": ["null", "object"]}
    if shape[0] in ("json", "union"):
        return {}
    return _scalar_schema(shape[1])


def _scalar_schema(kind):
    """The JSON Schema of a DuckDB scalar column type."""
    if kind == "BOOLEAN":
        return {"type": ["null", "boolean"]}
    if kind in _INTEGERS:
        return {"type": ["null", "integer"]}
    if kind in ("FLOAT", "DOUBLE", "REAL", "INTERVAL") or kind.startswith("DECIMAL"):  # intervals: seconds
        return {"type": ["null", "number"]}
    if kind == "DATE":
        return {"type": ["null", "string"], "format": "date"}
    if kind.startswith("TIMESTAMP"):
        return {"type": ["null", "string"], "format": "date-time"}
    if kind.startswith("TIME"):
        return {"type": ["null", "string"], "format": "time"}
    return {"type": ["null", "string"]}


def _plain(value):
    """
    A DuckDB value as plain data, ready for JSON (NaN and infinities are null: JSON has neither). DECIMAL values stay
    exact decimal.Decimal values, which outputs write as exact numbers (templates.to_json, to_text), and integers
    (HUGEINT too) are Python's exact ints.
    """
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return _plain_scalar(value)


def _plain_scalar(value):
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, decimal.Decimal):
        return value if value.is_finite() else None
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return value


def _decode(value, kind):
    """_plain(value), with the JSON values inside it (at any depth, per the column's type) decoded to data."""
    if value is None or "JSON" not in kind.upper():
        return _plain(value)
    shape = _shape(kind)
    if shape[0] == "json":
        return _plain(json.loads(value)) if isinstance(value, str) else _plain(value)
    if shape[0] == "list" and isinstance(value, (list, tuple)):
        return [_decode(item, shape[1]) for item in value]
    if shape[0] == "struct" and isinstance(value, dict):
        kinds = dict(_field(field) for field in shape[1])
        return {str(key): _decode(item, kinds.get(key, "")) for key, item in value.items()}
    if shape[0] == "struct" and isinstance(value, (list, tuple)):
        return [_decode(item, field) for item, field in zip(value, shape[1], strict=False)]
    if shape[0] == "map" and isinstance(value, dict):
        return {str(key): _decode(item, shape[2]) for key, item in value.items()}
    return _plain(value)
