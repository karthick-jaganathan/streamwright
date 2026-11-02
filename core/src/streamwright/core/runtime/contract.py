"""
Connector contract v1 (docs/design/connector-contract.md): the declarative, frozen connector definition - Field,
Operation, ConnectorDefinition - the EngineCapabilities each engine declares once, and the ResolvedConnection engine
adapters receive. Validation and resolution are in streamwright.core.runtime.contract_validation. Connector packages
import both as `streamwright.core.sdk.contract`. Its terms (engines, patterns, errors) are in contract_terms, Field,
Operation and EngineCapabilities in contract_declarations; this module re-exports them.
"""

from __future__ import annotations

import dataclasses
import fnmatch
import importlib
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from streamwright.core.runtime.constants import SDK_VERSION
from streamwright.core.runtime.contract_declarations import (
    EngineCapabilities,
    Field,
    Operation,
    check_unique,
    combine_capabilities,
    object_schema,
)
from streamwright.core.runtime.contract_terms import (
    ALL_ENGINES,
    ENGINES,
    FIELD_TYPES,
    NAME_PATTERN,
    TARGET_PATTERN,
    AdapterLoadError,
    DefinitionError,
    InvalidConnectionError,
    OperationKind,
    is_secret_reference,
    types_of,
)
from streamwright.core.runtime.redact import Redactor
from streamwright.core.validation.issues import Issue

__all__ = [
    "ALL_ENGINES", "ENGINES", "FIELD_TYPES", "SDK_VERSION", "AdapterLoadError", "ConnectorDefinition",
    "DefinitionError", "EngineCapabilities", "EngineLimit", "Field", "InvalidConnectionError", "Issue", "Operation",
    "OperationKind", "ResolvedConnection", "combine_capabilities", "is_secret_reference", "types_of",
]


def _load_target(target: str) -> Any:
    module, _, name = target.partition(":")
    return getattr(importlib.import_module(module), name)


@dataclass(frozen=True, kw_only=True)
class EngineLimit:
    """
    What a connector cannot do on one of its engines although the engine's capabilities allow it, declared once in its
    spec: without `choices`, the engine does not run `operation` for this connector (e.g. azure_blob writes on duckdb:
    DuckDB's azure extension cannot COPY to Azure storage); with `choices` ({argument: the values it supports there}),
    the engine runs it with only those values (e.g. no `mode: merge` with DuckDB's mysql extension).
    """
    engine: str
    operation: str
    choices: Mapping[str, tuple[Any, ...]] = dataclasses.field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(self, "choices", MappingProxyType({
            argument: tuple(values) for argument, values in self.choices.items()}))


def _limit_problem(limit: EngineLimit, adapters: Mapping[str, str], operations: tuple[Operation, ...]) -> str | None:
    operation = next((item for item in operations if item.name == limit.operation), None)
    if limit.engine not in adapters:
        return "engine_limits name engines it has an adapter for, not %r" % limit.engine
    if operation is None:
        return "engine_limits name its operations, not %r" % limit.operation
    for argument, values in limit.choices.items():
        declared = operation.argument(argument)
        if declared is None or not values or (declared.choices and not set(values) <= set(declared.choices)):
            return "engine_limits %s: %r takes a subset of the choices the operation declares" % (
                limit.operation, argument)
    return None


@dataclass(frozen=True)
class ConnectorDefinition:
    """
    A connector, as data: identity, connection fields, operations, the `"module:Class"` of each engine adapter and of
    each query builder, and the `"module:function"` of the vendor's resolved-connection check (`validator`). The
    engines it supports are the keys of `adapters`; the operations an engine runs are those whose kind its
    EngineCapabilities list, less what the connector's `engine_limits` exclude there.
    """
    name: str
    title: str
    family: str
    connection: tuple[Field, ...]
    operations: tuple[Operation, ...]
    adapters: Mapping[str, str]
    sdk_version: str = SDK_VERSION
    guide: str = "guide.md"
    summary: str = ""
    validator: str | None = None
    engine_limits: tuple[EngineLimit, ...] = ()
    query_builders: Mapping[str, str] = dataclasses.field(default_factory=lambda: MappingProxyType({}))
    secret_key_patterns: tuple[str, ...] = ()  # vendor connection keys that may bear a secret reference (redacted)
    surface: str | None = None  # "module:function": a pure introspection of the installed library (contract v1.3 §6b)
    # the provider error codes worth retrying by default (data, not code); a stream's `retry.codes` replace them
    retry_codes: tuple[str | int, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "engine_limits", tuple(self.engine_limits))
        object.__setattr__(self, "connection", tuple(self.connection))
        object.__setattr__(self, "operations", tuple(self.operations))
        object.__setattr__(self, "adapters", MappingProxyType(dict(self.adapters)))
        object.__setattr__(self, "query_builders", MappingProxyType(dict(self.query_builders)))
        object.__setattr__(self, "secret_key_patterns", tuple(self.secret_key_patterns))
        object.__setattr__(self, "retry_codes", tuple(self.retry_codes))
        if not all(isinstance(code, (str, int)) and not isinstance(code, bool) for code in self.retry_codes):
            raise DefinitionError("connector %s: retry_codes are provider error codes (text or integers)" % self.name)
        if not NAME_PATTERN.match(self.name) or not self.title or not self.family:
            raise DefinitionError("connector %r: a name ([a-z][a-z0-9_]*), a title and a family" % self.name)
        check_unique("connector %s connection" % self.name, [item.name for item in self.connection])
        check_unique("connector %s operations" % self.name, [item.name for item in self.operations])
        targets = list(self.adapters.values()) + ([self.validator] if self.validator else [])
        if not set(self.adapters) <= ALL_ENGINES or not all(TARGET_PATTERN.match(target) for target in targets):
            raise DefinitionError("connector %s: adapters are {engine: 'module:Class'} for %s" % (
                self.name, ", ".join(ENGINES)))
        if self.surface is not None and not TARGET_PATTERN.match(self.surface):
            raise DefinitionError("connector %s: surface is a 'module:function'" % self.name)
        if not all(NAME_PATTERN.match(name) and TARGET_PATTERN.match(target)
                   for name, target in self.query_builders.items()):
            raise DefinitionError("connector %s: query_builders are {name: 'module:Class'}" % self.name)
        for limit in self.engine_limits:
            problem = _limit_problem(limit, self.adapters, self.operations)
            if problem:
                raise DefinitionError("connector %s: %s" % (self.name, problem))

    @property
    def engines(self) -> frozenset[str]:
        return frozenset(self.adapters)

    @property
    def secret_fields(self) -> tuple[Field, ...]:
        return tuple(item for item in self.connection if item.secret)

    @property
    def required_fields(self) -> tuple[Field, ...]:
        return tuple(item for item in self.connection if item.required)

    def field(self, name: str) -> Field | None:
        return next((item for item in self.connection if item.name == name), None)

    def operation(self, service: str | None, method: str | None) -> Operation | None:
        """
        The operation for a concrete `service.method`: the exact one if the connector declares it, else the first
        operation *family* (§6b) whose glob patterns it fits, bound to the concrete names (so engines, validation and
        logs never see a pattern). None when nothing matches.
        """
        exact = next((item for item in self.operations if (item.service, item.method) == (service, method)), None)
        if exact is not None:
            return exact
        if service is None or method is None:
            return None
        family = next((item for item in self.operations if item.is_pattern and item.matches(service, method)), None)
        return dataclasses.replace(family, service=service, method=method) if family is not None else None

    def runs(self, operation: Operation, capabilities: EngineCapabilities) -> bool:
        """
        Whether an engine runs this (possibly family-bound) operation for this connector: the engine has an adapter,
        its capabilities run the operation's kind, and no `engine_limit` excludes it (families and limits are matched
        by name, so a limit may name an operation family).
        """
        if capabilities.engine not in self.engines or not capabilities.runs(operation):
            return False
        excluded = (limit.operation for limit in self.engine_limits
                    if limit.engine == capabilities.engine and not limit.choices)
        return not any(fnmatch.fnmatchcase(operation.name, name) for name in excluded)

    def load_surface(self) -> Callable[[str, str], list[str]] | None:
        """
        The vendor's surface introspection (`surface(service, method) -> list[str]` of problems, §6b), or None when
        the connector declares none or its library is not installed (ImportError is skipped silently, so validation
        stays offline-friendly; a typo is then caught by the clear run-time error instead).
        """
        if self.surface is None:
            return None
        try:
            return _load_target(self.surface)
        except ImportError:
            return None

    def operations_for(self, capabilities: EngineCapabilities) -> tuple[Operation, ...]:
        """
        The operations an engine runs for this connector: none when the connector has no adapter for it, and none
        that one of its `engine_limits` excludes on that engine.
        """
        if capabilities.engine not in self.engines:
            return ()
        excluded = {limit.operation for limit in self.engine_limits
                    if limit.engine == capabilities.engine and not limit.choices}
        return tuple(item for item in self.operations if capabilities.runs(item) and item.name not in excluded)

    def capabilities_for(self, capabilities: EngineCapabilities) -> EngineCapabilities:
        """The engine's capabilities with this connector's `engine_limits` on its choices applied."""
        limits = [limit for limit in self.engine_limits if limit.engine == capabilities.engine and limit.choices]
        if not limits:
            return capabilities
        arguments = {name: dict(supported) for name, supported in capabilities.arguments.items()}
        for limit in limits:
            operation = next(item for item in self.operations if item.name == limit.operation)
            supported = arguments.setdefault(limit.operation, {item.name: None for item in operation.arguments})
            for argument, values in limit.choices.items():
                if argument not in supported and argument not in operation.extensions:
                    continue  # the engine does not take it at all
                engine_choices = supported.get(argument)
                supported[argument] = tuple(value for value in values
                                            if engine_choices is None or value in engine_choices)
        return dataclasses.replace(capabilities, arguments=arguments)

    def json_schema(self, engine: str | None = None) -> dict[str, Any]:
        """
        The Draft 7 schema of a declared connection (with its `provider`), for `engine` or any engine. The object is
        open to vendor arguments (`additionalProperties: true`): any key the connector does not declare is forwarded
        verbatim to the vendor client configuration (contract v1.2, docs/design/connector-contract.md §6a).
        """
        if engine is not None and engine not in self.engines:
            raise AdapterLoadError("connector %s does not support the %s engine" % (self.name, engine))
        schema = object_schema(self.connection, "%s connection" % self.name, {"provider": {"const": self.name}},
                               additional=True)
        return dict(schema, description=self.summary or self.title)

    def describe(self, *capabilities: EngineCapabilities) -> dict[str, Any]:
        """
        The declared contract (nothing is imported or connected), as plain data; `capabilities` add, per engine of
        this connector, the operations it runs and the arguments (and choices) it supports. `vendor_arguments` is
        always true: undeclared keys and declared `aliases` are forwarded to the vendor call (§6a).
        """
        return {"name": self.name, "title": self.title, "family": self.family, "summary": self.summary,
                "sdk_version": self.sdk_version, "engines": sorted(self.engines),
                "connection": {item.name: item.describe() for item in self.connection},
                "required_keys": [item.name for item in self.required_fields],
                "secret_keys": [item.name for item in self.secret_fields],
                "secret_key_patterns": list(self.secret_key_patterns),
                "vendor_arguments": True, "dynamic_surface": self.surface is not None,
                "retry_codes": list(self.retry_codes),
                "operations": [item.describe() for item in self.operations],
                "capabilities": {item.engine: self.capabilities_for(item).describe(self.operations_for(item))
                                 for item in capabilities if item.engine in self.engines},
                "query_builders": sorted(self.query_builders)}

    def load_adapter(self, engine: str) -> Any:
        """A new instance of the engine's adapter, imported now (the engine's libraries load with it)."""
        target = self.adapters.get(engine)
        if target is None:
            raise AdapterLoadError("connector %s has no %s adapter (engines: %s)" % (
                self.name, engine, ", ".join(sorted(self.engines)) or "none"))
        try:
            return _load_target(target)()
        except (ImportError, AttributeError) as exc:
            raise AdapterLoadError("connector %s: cannot load its %s adapter %s (%s: %s)" % (
                self.name, engine, target, type(exc).__name__, exc)) from exc

    def load_query_builder(self, name: str) -> Any:
        """A new instance of the connector's query builder `name`, imported now."""
        target = self.query_builders.get(name)
        if target is None:
            raise AdapterLoadError("connector %s has no query builder %r" % (self.name, name))
        try:
            return _load_target(target)()
        except (ImportError, AttributeError) as exc:
            raise AdapterLoadError("connector %s: cannot load its query builder %s (%s: %s)" % (
                self.name, target, type(exc).__name__, exc)) from exc

    def load_validator(self) -> Callable[[ResolvedConnection], list[str]]:
        """The vendor's check of a resolved connection (no validator: one that finds nothing)."""
        return _load_target(self.validator) if self.validator else _no_problems


def _no_problems(connection: ResolvedConnection) -> list[str]:
    return []


# * -----------------------
# * the resolved connection
# * -----------------------


class ResolvedConnection(Mapping[str, Any]):
    """
    A connection's resolved values (config and secrets substituted, defaults applied, text booleans and numbers
    converted), read-only. `secret_values` are the values of its secret fields, which `redact` masks.
    """

    __slots__ = ("_redactor", "_secret_values", "_values", "_vendor", "connector")

    def __init__(self, connector: str, values: Mapping[str, Any], secret_values: tuple[str, ...] = (),
                 vendor: Mapping[str, Any] = MappingProxyType({})):
        self.connector = connector
        self._values = MappingProxyType(dict(values))
        self._secret_values = tuple(str(value) for value in secret_values if value not in (None, ""))
        self._redactor = Redactor(self._secret_values)
        self._vendor = MappingProxyType(dict(vendor))

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return "ResolvedConnection(%s, keys=%s)" % (self.connector, sorted(self._values))

    @property
    def secret_values(self) -> tuple[str, ...]:
        return self._secret_values

    @property
    def vendor(self) -> Mapping[str, Any]:
        """The connection keys the connector does not declare, forwarded verbatim to the vendor client (§6a)."""
        return self._vendor

    @property
    def redactor(self) -> Redactor:
        return self._redactor

    def redact(self, text: object) -> str:
        return self._redactor(text)
