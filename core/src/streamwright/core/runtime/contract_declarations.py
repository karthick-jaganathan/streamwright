"""
What a connector definition declares (streamwright.core.runtime.contract): its fields (Field), operations
(Operation), and the EngineCapabilities each engine declares once.
"""

from __future__ import annotations

import dataclasses
import fnmatch
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from streamwright.core.runtime.contract_terms import (
    ALL_ENGINES,
    ENGINES,
    FIELD_TYPES,
    JSON_TYPES,
    NAME_PATTERN,
    REFERENCE_SCHEMA,
    SECRET_REFERENCE,
    VENDOR_KEY_PATTERN,
    DefinitionError,
    OperationKind,
    types_of,
)


@dataclass(frozen=True)
class Field:
    """One connection key or operation argument. `type` is a FIELD_TYPES name, or a tuple of them (a union)."""
    name: str
    type: str | tuple[str, ...]
    required: bool = False
    secret: bool = False
    default: Any = None
    choices: tuple[Any, ...] = ()
    pattern: str | None = None
    item_type: str | None = None
    description: str = ""
    literal: bool = False  # the declared value is text (or a query builder call), never a `{{ }}` reference
    aliases: tuple[str, ...] = ()  # vendor-native names accepted interchangeably (unit conversion in the client)

    def __post_init__(self) -> None:
        object.__setattr__(self, "choices", tuple(self.choices))
        object.__setattr__(self, "aliases", tuple(self.aliases))
        unknown = [kind for kind in types_of(self.type) + ((self.item_type,) if self.item_type else ())
                   if kind not in FIELD_TYPES]
        if not NAME_PATTERN.match(self.name) or unknown:
            raise DefinitionError("field %r: bad name or type(s) %s" % (self.name, unknown))
        if any(not VENDOR_KEY_PATTERN.match(alias) for alias in self.aliases):
            raise DefinitionError("field %r: aliases are vendor-native names (%s)" % (self.name, self.aliases))
        if self.secret and self.type != "string":
            raise DefinitionError("field %r: a secret field is a string" % self.name)
        if self.pattern is not None:
            re.compile(self.pattern)

    def json_schema(self) -> dict[str, Any]:
        """The Draft 7 schema of the declared value (a non-secret value may also be a `{{ reference }}`)."""
        if self.secret:
            return {"type": "string", "pattern": SECRET_REFERENCE.pattern, "description": self.description}
        schema: dict[str, Any] = {"type": [JSON_TYPES[kind] for kind in types_of(self.type)]}
        if len(schema["type"]) == 1:
            schema["type"] = schema["type"][0]
        if self.item_type:
            schema["items"] = {"type": JSON_TYPES[self.item_type]}
            if self.pattern is not None:
                schema["items"]["pattern"] = self.pattern
        if self.choices:
            schema["enum"] = list(self.choices)
        if self.pattern is not None:
            schema["pattern"] = self.pattern
        if self.default is not None:
            schema["default"] = self.default
        schema["description"] = self.description
        if self.literal:
            return schema
        return {"anyOf": [schema, dict(REFERENCE_SCHEMA)], "description": self.description}

    def describe(self) -> dict[str, Any]:
        return {"type": self.type, "required": self.required, "secret": self.secret, "default": self.default,
                "choices": list(self.choices), "pattern": self.pattern, "item_type": self.item_type,
                "description": self.description, "aliases": list(self.aliases)}


@dataclass(frozen=True, kw_only=True)
class Operation:
    """
    A `service.method` a connector offers and its arguments. Shared operations come from the catalog
    (`streamwright.core.sdk.operations`); `with_arguments` adds a connector's own arguments, recorded in `extensions`
    (the connector's adapters take them on every engine they run on).
    """
    service: str
    method: str
    kind: OperationKind = OperationKind.READ
    arguments: tuple[Field, ...] = ()
    idempotent: bool = True
    description: str = ""
    extensions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "arguments", tuple(self.arguments))
        object.__setattr__(self, "extensions", tuple(self.extensions))
        try:
            object.__setattr__(self, "kind", OperationKind(self.kind))
        except ValueError:
            raise DefinitionError("operation %s: bad kind %r (kinds: %s)" % (
                self.name, self.kind, ", ".join(kind.value for kind in OperationKind))) from None
        names = [argument.name for argument in self.arguments]
        check_unique("operation %s arguments" % self.name, names)
        if any(argument.secret for argument in self.arguments):
            raise DefinitionError("operation %s: arguments are not credentials (no secret arguments)" % self.name)
        if not set(self.extensions) <= set(names):
            raise DefinitionError("operation %s: extensions name its arguments" % self.name)

    @property
    def name(self) -> str:
        return "%s.%s" % (self.service, self.method)

    @property
    def is_pattern(self) -> bool:
        """
        Whether this is an operation *family* (contract v1.3 §6b): its `service` or `method` is a glob pattern, so it
        stands for every concrete `service.method` of the vendor that fits it; the installed library decides which
        exist. An exact operation (no `*`, `?`, `[`) stands for itself.
        """
        return any(char in "*?[" for char in self.service + self.method)

    def matches(self, service: str, method: str) -> bool:
        """Whether a concrete `service.method` fits this operation (glob match; an exact operation matches equals)."""
        return fnmatch.fnmatchcase(service, self.service) and fnmatch.fnmatchcase(method, self.method)

    def argument(self, name: str) -> Field | None:
        return next((argument for argument in self.arguments if argument.name == name), None)

    def with_arguments(self, *fields: Field) -> Operation:
        """This operation with `fields` added, or replacing the arguments of the same name (a connector's own)."""
        given = {field.name: field for field in fields}
        known = {argument.name for argument in self.arguments}
        arguments = tuple(given.pop(argument.name, argument) for argument in self.arguments) + tuple(given.values())
        added = tuple(field.name for field in fields if field.name not in known)
        return dataclasses.replace(self, arguments=arguments, extensions=self.extensions + added)

    def arguments_for(self, capabilities: EngineCapabilities | None) -> tuple[Field, ...]:
        """The arguments an engine takes, their choices narrowed to what it supports (None: as declared)."""
        if capabilities is None or self.name not in capabilities.arguments:
            return self.arguments
        supported = capabilities.arguments[self.name]
        return tuple(_narrow(argument, supported[argument.name]) if argument.name in supported else argument
                     for argument in self.arguments
                     if argument.name in supported or argument.name in self.extensions)

    def json_schema(self, capabilities: EngineCapabilities | None = None) -> dict[str, Any]:
        return object_schema(self.arguments_for(capabilities), "%s arguments" % self.name, additional=True)

    def describe(self) -> dict[str, Any]:
        return {"name": self.name, "service": self.service, "method": self.method, "kind": self.kind.value,
                "idempotent": self.idempotent, "description": self.description, "extensions": list(self.extensions),
                "pattern": self.is_pattern,
                "arguments": [dict(argument.describe(), name=argument.name) for argument in self.arguments]}


def _narrow(argument: Field, choices: tuple[Any, ...] | None) -> Field:
    if choices is None:
        return argument
    return dataclasses.replace(argument, choices=tuple(choice for choice in argument.choices or choices
                                                       if choice in choices))


@dataclass(frozen=True)
class EngineCapabilities:
    """
    What one engine runs, declared once in its io module: the operation kinds, and per catalog operation name the
    arguments it supports - each None (every declared value) or the tuple of the choices it supports. An argument of
    a listed operation that is missing is not supported; an operation that is not listed passes all its arguments to
    the connector's adapter, as do a connector's own arguments (Operation.extensions). `checks` are the engine's
    semantic checks of an operation's arguments (what declarations cannot say), each returning messages.
    """
    engine: str
    operation_kinds: frozenset[OperationKind]
    arguments: Mapping[str, Mapping[str, tuple[Any, ...] | None]]
    checks: Mapping[str, Callable[[Mapping[str, Any]], list[str]]] = dataclasses.field(
        default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(self, "operation_kinds", frozenset(OperationKind(kind) for kind in self.operation_kinds))
        object.__setattr__(self, "arguments", MappingProxyType({
            name: MappingProxyType({key: None if choices is None else tuple(choices)
                                    for key, choices in supported.items()})
            for name, supported in self.arguments.items()}))
        object.__setattr__(self, "checks", MappingProxyType(dict(self.checks)))
        if self.engine not in ALL_ENGINES:
            raise DefinitionError("capabilities: engine %r is not one of %s" % (self.engine, ", ".join(ENGINES)))

    def runs(self, operation: Operation) -> bool:
        return operation.kind in self.operation_kinds

    def supports(self, operation: Operation, argument: str) -> bool:
        supported = self.arguments.get(operation.name)
        return supported is None or argument in supported or argument in operation.extensions

    def choices(self, operation: Operation, argument: str) -> tuple[Any, ...] | None:
        """The values of an argument the engine supports (None: every declared value)."""
        return self.arguments.get(operation.name, MappingProxyType({})).get(argument)

    def describe(self, operations: Iterable[Operation]) -> dict[str, Any]:
        """{operation name: {argument: [choices] or None}} of the operations this engine runs."""
        return {operation.name: {argument.name: list(argument.choices) if argument.choices else None
                                 for argument in operation.arguments_for(self)}
                for operation in operations if self.runs(operation)}


def combine_capabilities(*capabilities: EngineCapabilities) -> EngineCapabilities:
    """
    One engine's capabilities from those its io families declare (files, databases, messages, ...): the kinds
    unioned, the operations' arguments and checks merged. An operation two families declare must be declared alike.
    """
    engines = {item.engine for item in capabilities}
    if len(engines) != 1:
        raise DefinitionError("capabilities to combine must be of one engine, not %s" % ", ".join(sorted(engines)))
    arguments: dict[str, Mapping[str, tuple[Any, ...] | None]] = {}
    checks: dict[str, Callable[[Mapping[str, Any]], list[str]]] = {}
    for item in capabilities:
        for name, supported in item.arguments.items():
            if arguments.setdefault(name, supported) != supported:
                raise DefinitionError("capabilities: %s is declared differently by two families" % name)
        for name, check in item.checks.items():
            if checks.setdefault(name, check) is not check:
                raise DefinitionError("capabilities: %s has two different checks" % name)
    return EngineCapabilities(engine=engines.pop(),
                              operation_kinds=frozenset().union(*(item.operation_kinds for item in capabilities)),
                              arguments=arguments, checks=checks)


def check_unique(what: str, names: list[str]) -> None:
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise DefinitionError("%s: %s declared more than once" % (what, ", ".join(repeated)))


def object_schema(fields: tuple[Field, ...], title: str, extra: Mapping[str, Any] | None = None, *,
                  additional: bool = False) -> dict[str, Any]:
    """
    The Draft 7 object schema of declared `fields`. `additional` True opens the object to vendor arguments
    (`additionalProperties: true`): keys not declared here are forwarded verbatim to the vendor call.
    """
    properties = dict(extra or {})
    properties.update((item.name, item.json_schema()) for item in fields)
    required = list(extra or {}) + sorted(item.name for item in fields if item.required)
    return {"title": title, "type": "object", "properties": properties, "required": required,
            "additionalProperties": additional}
