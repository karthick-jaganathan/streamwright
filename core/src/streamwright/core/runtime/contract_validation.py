"""
Generic, engine-neutral validation of connector contract v1 declarations (streamwright.core.runtime.contract):
declared connections and operation arguments, and the resolution of a declared connection into a
ResolvedConnection. Connector packages import it as `streamwright.core.sdk.contract`.
"""

from __future__ import annotations

import datetime
import re
from collections.abc import Mapping
from typing import Any

from streamwright.core.config.inputs import InputError, parse_duration
from streamwright.core.engine.queries import BuiltQuery
from streamwright.core.runtime.contract import (
    ConnectorDefinition,
    EngineCapabilities,
    Field,
    InvalidConnectionError,
    Operation,
    ResolvedConnection,
    is_secret_reference,
    types_of,
)
from streamwright.core.runtime.contract_terms import REFERENCE, SECRET_MENTION, matches_secret_pattern
from streamwright.core.runtime.templates import render
from streamwright.core.validation.checker import suggest
from streamwright.core.validation.issues import Issue

__all__ = ["check_arguments", "check_connection", "resolve_arguments", "resolve_connection", "resolve_values",
           "vendor_arguments"]


def _type_problem(kind: str | tuple[str, ...], value: Any) -> str | None:
    if any(_has_type(single, value) for single in types_of(kind)):
        return None
    return "expected %s, got %s" % (" or ".join(types_of(kind)), type(value).__name__)


def _has_type(kind: str, value: Any) -> bool:
    if kind == "duration":
        return isinstance(value, str) and _is_duration(value)
    checks = {"string": str, "boolean": bool, "list": (list, tuple), "mapping": Mapping}
    if kind in checks:
        return isinstance(value, checks[kind])
    allowed = int if kind == "integer" else (int, float)
    return isinstance(value, allowed) and not isinstance(value, bool)


def _is_duration(text: str) -> bool:
    try:
        parse_duration(text)
    except InputError:
        return False
    return True


def _is_reference(value: Any) -> bool:
    return isinstance(value, str) and REFERENCE.search(value) is not None


def _scalar_problems(spec: Field, value: Any) -> list[str]:
    if spec.choices and value not in spec.choices:
        return ["expected one of: %s" % ", ".join(str(choice) for choice in spec.choices)]
    if spec.pattern is not None and isinstance(value, str) and re.search(spec.pattern, value) is None:
        return ["%r does not match %s" % (value, spec.pattern)]
    return []


def _value_problems(spec: Field, value: Any, *, rendered: bool) -> list[str]:
    """Why a declared (`rendered` False: references are known at run time) or resolved value is wrong."""
    if not rendered and _is_reference(value):
        return ["cannot contain `{{ }}` references (write values through a query builder)"] if spec.literal else []
    if isinstance(value, BuiltQuery) and "string" in types_of(spec.type):
        return []  # the text a query builder will write
    problem = _type_problem(spec.type, value)
    if problem:
        return [problem]
    if isinstance(value, (list, tuple)):
        found = []
        for index, item in enumerate(value):
            if rendered or not _is_reference(item):
                item_problem = _type_problem(spec.item_type, item) if spec.item_type else None
                found += ["[%d]: %s" % (index, text) for text in ([item_problem] if item_problem else
                                                                  _scalar_problems(spec, item))]
        return found
    return _scalar_problems(spec, value)


def _mentions_secret(value: Any) -> bool:
    if isinstance(value, str):
        return SECRET_MENTION.search(value) is not None
    if isinstance(value, Mapping):
        return any(_mentions_secret(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_mentions_secret(item) for item in value)
    return False


def _declared_value_issues(spec: Field, key: str, value: Any, fields: tuple[Field, ...], *,
                           rendered: bool) -> list[Issue]:
    """The checks of a value given under a declared field `key`: secret placement, then its type and choices."""
    if spec.secret:
        if not is_secret_reference(value):
            return [Issue("error", "literal-secret", "must be one {{ secrets.NAME }} reference", path=key)]
        return []
    if _mentions_secret(value):
        return [Issue("error", "secret-placement", "cannot use secrets (secret keys: %s)" % (
            ", ".join(item.name for item in fields if item.secret) or "none"), path=key)]
    return [Issue("error", "bad-value", text, path=key) for text in _value_problems(spec, value, rendered=rendered)]


def _vendor_value_issues(key: str, value: Any, secret_key_patterns: tuple[str, ...]) -> list[Issue]:
    """
    The check of a vendor argument (an undeclared key or a declared alias): it is forwarded verbatim, so only secret
    placement is judged. A connection key matching `secret_key_patterns` may bear a secret reference (redacted).
    """
    if _mentions_secret(value) and not matches_secret_pattern(key, secret_key_patterns):
        return [Issue("error", "secret-placement", "a vendor argument cannot hold a secret", path=key)]
    return []


def _declared_issues(fields: tuple[Field, ...], values: Any, what: str, *, rendered: bool = False,
                     secret_key_patterns: tuple[str, ...] = ()) -> list[Issue]:
    """
    The generic checks of a declared mapping: its type, missing keys, and per key either the declared-field checks or,
    for an undeclared key (a vendor argument) or a declared alias, the vendor-argument checks. Vendor arguments are
    accepted and forwarded verbatim; a near-miss of a declared name or alias is a warning (contract v1.2 §6a).
    """
    if not isinstance(values, Mapping):
        return [Issue("error", "bad-type", "%s must be a mapping" % what)]
    known = {item.name: item for item in fields}
    alias_of = {alias: item for item in fields for alias in item.aliases}
    candidates = list(known) + list(alias_of)
    issues = [Issue("error", "missing-key", "is required", path=item.name) for item in fields
              if item.required and item.name not in values and not any(alias in values for alias in item.aliases)]
    for item in fields:
        spellings = [name for name in (item.name, *item.aliases) if name in values]
        if len(spellings) > 1:
            issues.append(Issue("error", "alias-conflict", "set %s once, not %s" % (
                item.name, ", ".join("`%s`" % name for name in spellings)), path=spellings[0]))
    for key, value in values.items():
        spec = known.get(key)
        if spec is not None:
            issues += _declared_value_issues(spec, key, value, fields, rendered=rendered)
        elif key in alias_of:
            issues += _vendor_value_issues(key, value, secret_key_patterns)
        else:
            hint = "" if matches_secret_pattern(key, secret_key_patterns) else suggest(key, candidates)
            if hint:
                issues.append(Issue("warning", "unknown-key", "unknown vendor argument%s" % hint, path=key))
            issues += _vendor_value_issues(key, value, secret_key_patterns)
    return issues


def check_connection(definition: ConnectorDefinition, declared: Mapping[str, Any]) -> list[Issue]:
    """Problems with a connection as written (without `provider`): keys, types, choices, patterns, secret references."""
    return _declared_issues(definition.connection, declared, "%s connection keys" % definition.name,
                            secret_key_patterns=definition.secret_key_patterns)


def vendor_arguments(operation: Operation, arguments: Mapping[str, Any] | None) -> dict[str, Any]:
    """The arguments an operation does not declare (vendor arguments and aliases), for the connector's adapter."""
    declared = {field.name for field in operation.arguments}
    return {key: value for key, value in (arguments or {}).items() if key not in declared}


def _surface_issues(definition: ConnectorDefinition, operation: Operation) -> list[Issue]:
    """
    Whether the concrete `service.method` exists in the installed vendor library (contract v1.3 §6b): the connector's
    `surface` introspection, called only for a concrete operation and only when its library is importable. Problems
    (with the SDK's own "did you mean" suggestions) are `unknown-operation` errors, so typos are caught before a run.
    """
    if operation.is_pattern:
        return []
    check = definition.load_surface()
    if check is None:
        return []
    return [Issue("error", "unknown-operation", problem) for problem in check(operation.service, operation.method)]


def _capability_issues(operation: Operation, arguments: Mapping[str, Any],
                       capabilities: EngineCapabilities) -> list[Issue]:
    """Declared arguments the engine does not support, or set to a choice it does not support."""
    issues = []
    for key, value in arguments.items():
        declared = operation.argument(key)
        if declared is None:
            continue
        if not capabilities.supports(operation, key):
            issues.append(Issue("error", "unsupported-argument", "`%s` is not supported by the %s engine" % (
                key, capabilities.engine), path=key))
            continue
        choices = capabilities.choices(operation, key)
        if choices is not None and not _is_reference(value) and value in declared.choices and value not in choices:
            issues.append(Issue("error", "unsupported-choice", "`%s: %s` is not supported by the %s engine "
                                "(supported: %s)" % (key, value, capabilities.engine,
                                                     ", ".join(str(choice) for choice in choices)), path=key))
    return issues


def check_arguments(definition: ConnectorDefinition, operation: Operation, arguments: Any,
                    capabilities: EngineCapabilities | None = None, *, rendered: bool = False) -> list[Issue]:
    """
    Problems with an operation's arguments as written or rendered (references are checked once rendered): the
    declared checks, then - with an engine's `capabilities` - whether that engine runs the operation and supports
    each argument and choice, and the engine's semantic checks (None: as declared, for any engine). `rendered`: the
    arguments are final values (a text may contain braces), as when a request runs.
    """
    if capabilities is not None and not definition.runs(operation, capabilities):
        return [Issue("error", "unsupported-operation", "%s %s does not run on the %s engine" % (
            definition.name, operation.name, capabilities.engine))]
    values = {} if arguments is None else arguments
    issues = _declared_issues(operation.arguments, values, "%s %s arguments" % (definition.name, operation.name),
                              rendered=rendered)
    if capabilities is not None and isinstance(values, Mapping):
        capabilities = definition.capabilities_for(capabilities)
        issues += _capability_issues(operation, values, capabilities)
        check = capabilities.checks.get(operation.name)
        if check is not None and not issues:
            issues = [Issue("error", "bad-value", message) for message in check(values)]
    if capabilities is not None:
        issues += _surface_issues(definition, operation)
    return [Issue(issue.severity, issue.code, issue.message, path=("arguments.%s" % issue.path).rstrip("."))
            for issue in issues]


def resolve_arguments(operation: Operation, arguments: Any) -> Any:
    """
    Rendered arguments as their declared types: a date (`{{ today }}`, `{{ window.start }}`, a date input) in a text
    argument, or in a list of texts, is its ISO text. Other values are kept as rendered (check_arguments judges them).
    """
    if not isinstance(arguments, Mapping):
        return arguments
    fields = {spec.name: spec for spec in operation.arguments}
    return {name: _as_declared(fields.get(name), value) for name, value in arguments.items()}


def _as_declared(spec: Field | None, value: Any) -> Any:
    if spec is None:
        return value
    if isinstance(value, (list, tuple)) and spec.item_type == "string":
        return type(value)(_date_text(item) for item in value)
    return _date_text(value) if "string" in types_of(spec.type) else value


def _date_text(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime.date) else value


def _coerce(spec: Field, value: Any) -> Any:
    """A rendered text value as the boolean or number its field declares (references render to text)."""
    if not isinstance(value, str) or isinstance(spec.type, tuple):
        return value
    text = value.strip()
    if spec.type == "boolean" and text.lower() in ("true", "false"):
        return text.lower() == "true"
    if spec.type in ("integer", "number") and re.fullmatch(r"-?\d+", text):
        return int(text)
    if spec.type == "number" and re.fullmatch(r"-?\d+\.\d*(e-?\d+)?", text, re.IGNORECASE):
        return float(text)
    return value


def resolve_values(definition: ConnectorDefinition, values: Mapping[str, Any]) -> ResolvedConnection:
    """
    A ResolvedConnection from rendered values: defaults applied, declared types converted and checked, then the
    vendor's validator; InvalidConnectionError (messages never quote secret values) otherwise. Undeclared keys are
    vendor arguments, forwarded verbatim through `.vendor`; those matching `secret_key_patterns` are redacted (§6a).
    """
    declared_names = {spec.name for spec in definition.connection}
    resolved: dict[str, Any] = {}
    for spec in definition.connection:
        if values.get(spec.name) is not None:
            resolved[spec.name] = _coerce(spec, values[spec.name])
        elif spec.default is not None:
            resolved[spec.name] = spec.default
    vendor = {key: value for key, value in values.items() if key not in declared_names and value is not None}
    resolved.update(vendor)
    secret_values = tuple(resolved[spec.name] for spec in definition.secret_fields if spec.name in resolved)
    secret_values += tuple(str(value) for key, value in vendor.items()
                           if matches_secret_pattern(key, definition.secret_key_patterns))
    connection = ResolvedConnection(definition.name, resolved, secret_values, vendor=vendor)
    issues = [Issue("error", "missing-key", "is required", path=spec.name) for spec in definition.required_fields
              if spec.name not in resolved and not any(alias in vendor for alias in spec.aliases)]
    for spec in definition.connection:
        if spec.name in resolved and not spec.secret and spec.name not in vendor:
            issues += [Issue("error", "bad-value", text, path=spec.name)
                       for text in _value_problems(spec, resolved[spec.name], rendered=True)]
        elif spec.name in resolved and spec.secret and not isinstance(resolved[spec.name], str):
            issues.append(Issue("error", "bad-value", "expected a text", path=spec.name))
    if not issues:
        issues = [Issue("error", "bad-value", text) for text in definition.load_validator()(connection)]
    if issues:
        raise InvalidConnectionError(definition.name, [
            Issue(issue.severity, issue.code, connection.redact(issue.message), path=issue.path) for issue in issues])
    return connection


def resolve_connection(definition: ConnectorDefinition, declared: Mapping[str, Any], *,
                       config: Mapping[str, Any], secrets: Mapping[str, Any]) -> ResolvedConnection:
    """check_connection, render the `{{ config.* }}` and `{{ secrets.* }}` references, resolve_values (or raise)."""
    issues = check_connection(definition, declared)
    if issues:
        raise InvalidConnectionError(definition.name, issues)
    return resolve_values(definition, render(dict(declared), {"config": dict(config), "secrets": dict(secrets)}))

