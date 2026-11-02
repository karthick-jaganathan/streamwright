"""
The DuckDB engine's adapter for a connector contract v1 definition (docs/design/connector-contract.md): what the
engine asks of an adapter (fields, reads, write target, configure, compile_read, write) derived from the definition,
with the work done by the connector's `duckdb` adapter, imported when the job first configures a connection.

The engine's EngineCapabilities (streamwright.core.io.capabilities.DUCKDB_CAPABILITIES) say which operations it runs
(their kind) and which arguments and choices it takes. A step `read: <kind>` selects the connector's read operation
whose `format` argument takes <kind> (the format), or else - for a read operation without format choices - the one
whose method is <kind>; the step's other keys are that operation's arguments. Outputs use the write operation: the
`mode` choices the engine supports are the modes; an operation that is not idempotent is replay-safe in `overwrite`,
`merge` and `delete` only (as every DuckDB writer). A connector whose duckdb adapter has `open_state(session, name,
connection, schema)` can hold the job's bookkeeping state.
"""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Final

from streamwright.core.io.capabilities import DUCKDB_CAPABILITIES
from streamwright.core.jobs.duckdb.adapter import AdapterError, ProviderAdapter, ReadContext, WriteContext
from streamwright.core.jobs.fields import Field as JobField
from streamwright.core.jobs.state import StateStore
from streamwright.core.jobs.types import WriteReceipt
from streamwright.core.jobs.write_rules import OutputSpec, WriteRuleProblem, WriteTarget, warn_merge_scope
from streamwright.core.runtime.contract import (
    AdapterLoadError,
    ConnectorDefinition,
    Field,
    InvalidConnectionError,
    Operation,
    OperationKind,
    ResolvedConnection,
    types_of,
)
from streamwright.core.runtime.contract_validation import check_arguments, check_connection, resolve_values

__all__ = ["DefinitionDuckDBAdapter"]

CAPABILITIES: Final = DUCKDB_CAPABILITIES
REPLAY_SAFE_MODES: Final = ("overwrite", "merge", "delete")
SECRET_CODES: Final = frozenset({"literal-secret", "secret-placement"})  # reported by the jobs secret check (K4)
JOB_TYPES: Final = MappingProxyType({"string": "string", "integer": "integer", "number": "number",
                                     "boolean": "boolean", "duration": "string", "list": "array", "mapping": "object"})


def _job_field(field: Field) -> JobField:
    """The manifest rule of a connection field: its JSON types, or a `{{ reference }}` text."""
    kinds = tuple(dict.fromkeys([JOB_TYPES[kind] for kind in types_of(field.type)] + ["string"]))
    items = JobField(JOB_TYPES[field.item_type]) if field.item_type else None
    return JobField(kinds, required=field.required, items=items, description=field.description)


def _read_kinds(operations: tuple[Operation, ...]) -> dict[str, tuple[Operation, str | None]]:
    """{read kind: (operation, the format it sets or None)}."""
    kinds: dict[str, tuple[Operation, str | None]] = {}
    for operation in (operation for operation in operations if operation.kind == OperationKind.READ):
        file_format = next((argument for argument in operation.arguments_for(CAPABILITIES)
                            if argument.name == "format"), None)
        choices = file_format.choices if file_format is not None else ()
        for kind, fixed_format in [(choice, choice) for choice in choices] or [(operation.method, None)]:
            kinds.setdefault(kind, (operation, fixed_format))
    return kinds


class DefinitionDuckDBAdapter(ProviderAdapter):
    """Binds a ConnectorDefinition that lists the `duckdb` engine to the DuckDB engine."""

    def __init__(self, definition: ConnectorDefinition):
        self.definition = definition
        self.provider = definition.name
        self.secret_keys = tuple(field.name for field in definition.secret_fields)
        self.connection_fields = {field.name: _job_field(field) for field in definition.connection}
        self.read_kinds = _read_kinds(definition.operations_for(CAPABILITIES))
        self.reads = {kind: tuple(argument.name for argument in operation.arguments_for(CAPABILITIES)
                                  if argument.name != "format")
                      for kind, (operation, _) in self.read_kinds.items()}
        self.write_operation = next((operation for operation in definition.operations_for(CAPABILITIES)
                                     if operation.kind == OperationKind.WRITE), None)
        self._adapter: Any = None

    def describe(self) -> dict[str, Any]:
        return dict(super().describe(), definition=self.definition.describe(CAPABILITIES))

    def adapter(self) -> Any:
        """The connector's DuckDB adapter, imported on first use."""
        if self._adapter is None:
            try:
                self._adapter = self.definition.load_adapter(CAPABILITIES.engine)
            except AdapterLoadError as exc:
                raise AdapterError(str(exc)) from None
        return self._adapter

    @property
    def holds_state(self) -> bool:  # type: ignore[override]
        """Whether the connector's duckdb adapter can hold the job's state (imports the adapter)."""
        return callable(getattr(self.adapter(), "open_state", None))

    def open_state(self, session: Any, name: str, connection: Mapping[str, Any], schema: str) -> StateStore:
        if not self.holds_state:
            return super().open_state(session, name, connection, schema)
        return self.adapter().open_state(session, name, self.resolve(connection), schema)

    def resolve(self, connection: Mapping[str, Any]) -> ResolvedConnection:
        values = {key: value for key, value in connection.items() if key != "provider"}
        try:
            return resolve_values(self.definition, values)
        except InvalidConnectionError as exc:
            raise AdapterError(str(exc)) from None

    def validate(self, name: str, connection: Mapping[str, Any]) -> list[tuple[str, str, bool]]:
        declared = {key: value for key, value in connection.items() if key != "provider"}
        return [(issue.code, "%s: %s" % (issue.path, issue.message) if issue.path else issue.message, False)
                for issue in check_connection(self.definition, declared) if issue.code not in SECRET_CODES]

    def check_read(self, step: Mapping[str, Any]) -> list[tuple[str, str, bool]]:
        operation, arguments = self._read_call(step)
        return [(issue.code, "%s: %s" % (issue.path, issue.message), False)
                for issue in check_arguments(self.definition, operation, arguments, CAPABILITIES)]

    def _read_call(self, step: Mapping[str, Any]) -> tuple[Operation, dict[str, Any]]:
        operation, file_format = self.read_kinds[step["read"]]
        arguments = {key: step[key] for key in self.reads[step["read"]] if key in step}
        if file_format is not None:
            arguments["format"] = file_format
        return operation, arguments

    def configure(self, session: Any, name: str, connection: Mapping[str, Any]) -> None:
        self.adapter().configure(session, name, self.resolve(connection))

    def compile_read(self, context: ReadContext, step: Mapping[str, Any]) -> str:
        operation, arguments = self._read_call(step)
        context.connection = self.resolve(context.connection)
        return self.adapter().read_sql(context, operation, arguments)

    def write_target(self, connection: Mapping[str, Any]) -> WriteTarget | None:
        operation = self.write_operation
        if operation is None:
            return None
        arguments = operation.arguments_for(CAPABILITIES)
        mode = next((argument for argument in arguments if argument.name == "mode"), None)
        modes = tuple(mode.choices) if mode is not None else ("append",)
        keys = tuple(argument.name for argument in arguments if argument.name != "mode")
        return WriteTarget(
            name="%s %s" % (self.provider, operation.name), modes=modes, keys=keys,
            required={"*": tuple(argument.name for argument in arguments if argument.required and
                                 argument.name != "mode")},
            replay_safe=modes if operation.idempotent else tuple(m for m in modes if m in REPLAY_SAFE_MODES),
            checks=(self._check_output, warn_merge_scope) if operation.argument("target_predicate") else (
                self._check_output,))

    def _output_arguments(self, spec: OutputSpec) -> dict[str, Any]:
        names = {argument.name for argument in self.write_operation.arguments_for(CAPABILITIES)}
        return {key: getattr(spec, key) for key in spec.given() if key in names}

    def _check_output(self, spec: OutputSpec) -> list[WriteRuleProblem]:
        issues = check_arguments(self.definition, self.write_operation, self._output_arguments(spec), CAPABILITIES)
        return [WriteRuleProblem(issue.code, "%s: %s" % (issue.path, issue.message)) for issue in issues
                if issue.code not in ("unknown-key", "missing-key")]   # check_output reports keys

    def write(self, context: WriteContext) -> WriteReceipt:
        context.connection = self.resolve(context.connection)
        return self.adapter().write(context, self.write_operation, self._output_arguments(context.spec))
