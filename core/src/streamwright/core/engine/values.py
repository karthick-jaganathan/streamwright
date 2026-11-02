"""Partition values: their key (the bookmarks' key in the state) and distinct values in first-seen order."""

import decimal
import json
import math

from streamwright.core.runtime.templates import to_json

__all__ = ["Distinct", "legacy_partition_key", "partition_key", "partition_value"]


def partition_key(partition):
    """A partition as text (its bookmarks' key in the state): JSON, its DECIMAL values exact numbers (to_json)."""
    return to_json(partition, sort_keys=True, default=str)


class Distinct:
    """Distinct values (also lists and mappings) in the order they were first seen."""

    def __init__(self):
        self.values = []
        self._seen = set()

    def add(self, value):
        key = partition_key(value)
        if key not in self._seen:
            self._seen.add(key)
            self.values.append(value)


def _as_floats(value):
    """A value with its decimal.Decimal values (in lists and mappings too) as floats."""
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _as_floats(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_as_floats(item) for item in value]
    return value


def legacy_partition_key(partition):
    """
    The key a partition's bookmarks had before DECIMAL values were exact, when they were floats: the partition's
    JSON with its decimals as floats (1.2345678901234567e+19 for 12345678901234567890.123456).
    """
    return json.dumps(_as_floats(partition), sort_keys=True, default=str)


def partition_value(value):
    """
    A partition value taken from a column (a `from_stream` parent's export, a step's rows): a DECIMAL value is the
    float it was before DECIMAL values were exact when that float is its exact value (2.60 -> 2.6, 100 -> 100.0), so
    its text in requests and steps (`partition->>'amount'`: '100.0'), the keys made of it and its bookmarks' key stay
    as they were; else it stays the exact decimal.Decimal (12345678901234567890.123456), which a float rounded.
    """
    if isinstance(value, decimal.Decimal):
        if value.is_finite():
            number = float(value)
            if math.isfinite(number) and decimal.Decimal(repr(number)) == value:
                return number
        return value
    if isinstance(value, dict):
        return {key: partition_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [partition_value(item) for item in value]
    return value
