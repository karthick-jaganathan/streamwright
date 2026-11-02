"""
The terms of connector contract v1 (streamwright.core.runtime.contract): engines, field types, name and reference
patterns, operation kinds, and its errors.
"""

from __future__ import annotations

import fnmatch
import re
from enum import StrEnum
from types import MappingProxyType
from typing import Final

from streamwright.core.validation.issues import Issue

ENGINES: Final = ("source", "duckdb", "spark")
ALL_ENGINES: Final = frozenset(ENGINES)
FIELD_TYPES: Final = ("string", "integer", "number", "boolean", "duration", "list", "mapping")
JSON_TYPES: Final = MappingProxyType({"string": "string", "integer": "integer", "number": "number",
                                      "boolean": "boolean", "duration": "string", "list": "array",
                                      "mapping": "object"})
NAME_PATTERN: Final = re.compile(r"^[a-z][a-z0-9_]*$")
# a vendor-native key or alias (kafka `fetch.wait.max.ms`, boto3 `MaxNumberOfMessages`): mixed case, dots allowed
VENDOR_KEY_PATTERN: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")
TARGET_PATTERN: Final = re.compile(r"^[A-Za-z_][\w.]*:[A-Za-z_]\w*$")
SECRET_REFERENCE: Final = re.compile(r"^\s*\{\{\s*secrets\.[A-Za-z_][A-Za-z0-9_]*\s*\}\}\s*$")
SECRET_MENTION: Final = re.compile(r"\{\{\s*secrets\b")
REFERENCE: Final = re.compile(r"\{\{.*?\}\}")
REFERENCE_SCHEMA: Final = MappingProxyType({"type": "string", "pattern": r"^\s*\{\{[^{}]*\}\}\s*$"})


class DefinitionError(ValueError):
    """A malformed connector definition (raised when the spec is built, i.e. at import time)."""


class AdapterLoadError(LookupError):
    """A connector has no adapter for an engine, or its adapter cannot be imported."""


class InvalidConnectionError(ValueError):
    """A connection that fails the generic or the vendor checks; `issues` never quote secret values."""

    def __init__(self, connector: str, issues: list[Issue]):
        super().__init__("%s: %s" % (connector, "; ".join(_issue_text(issue) for issue in issues)))
        self.connector = connector
        self.issues = tuple(issues)


class OperationKind(StrEnum):
    """What an operation does with data; an engine declares the kinds it runs (EngineCapabilities)."""
    READ = "read"
    WRITE = "write"
    CONSUME = "consume"
    PUBLISH = "publish"


def is_secret_reference(value: object) -> bool:
    """Whether `value` is exactly one `{{ secrets.NAME }}` reference."""
    return isinstance(value, str) and SECRET_REFERENCE.match(value) is not None


def matches_secret_pattern(key: str, patterns: tuple[str, ...]) -> bool:
    """Whether a vendor connection key may bear a secret (fnmatch against `secret_key_patterns`)."""
    return any(fnmatch.fnmatchcase(key, pattern) for pattern in patterns)


def _issue_text(issue: Issue) -> str:
    return "%s: %s" % (issue.path, issue.message) if issue.path else issue.message


def types_of(kind: str | tuple[str, ...]) -> tuple[str, ...]:
    return kind if isinstance(kind, tuple) else (kind,)
