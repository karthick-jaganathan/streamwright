"""dlt column hints of an export: its JSON Schema's types, and exact decimals for its DECIMAL(p,s) columns."""

import re
from types import MappingProxyType

from streamwright.core.outputs.staging import canonical

__all__ = ["column_hints", "decimal_types"]

_TYPES = MappingProxyType({"string": "text", "integer": "bigint", "number": "double", "boolean": "bool",
                           "object": "json", "array": "json"})
_FORMATS = MappingProxyType({"date": "date", "date-time": "timestamp"})
_DECIMAL = re.compile(r"^DECIMAL\((\d+),(\d+)\)$")


def decimal_types(columns):
    """{name: (precision, scale)} of the DECIMAL(p,s) columns among (name, DuckDB type) columns."""
    types = {}
    for column, kind in columns:
        match = _DECIMAL.match(canonical(kind).replace(" ", ""))
        if match:
            types[column] = (int(match.group(1)), int(match.group(2)))
    return types


def column_hints(schema, decimals=None):
    """
    dlt column hints from a stream's JSON Schema (columns without a type, such as JSON, get none); `decimals`:
    {name: (precision, scale)} of its DECIMAL columns, which are dlt decimals (exact) instead of doubles.
    """
    hints = {}
    decimals = decimals or {}
    for name, definition in (schema.get("properties") or {}).items():
        if name in decimals:
            precision, scale = decimals[name]
            hints[name] = {"data_type": "decimal", "precision": precision, "scale": scale, "nullable": True}
            continue
        types = definition.get("type") or []
        types = [kind for kind in (types if isinstance(types, list) else [types]) if kind != "null"]
        data_type = _FORMATS.get(definition.get("format")) or (_TYPES.get(types[0]) if types else None)
        if data_type:
            hints[name] = {"data_type": data_type, "nullable": True}
    return hints
