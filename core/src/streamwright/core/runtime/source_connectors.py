"""
How the source engine (`streamwright run`, and DuckDBJob call steps) uses a connector definition: the declared and
resolved checks of a source's `auth` block, the checks of its sdk calls with the engine's EngineCapabilities (the
operations it runs, the arguments and choices it supports, their semantic checks), and reading through the
definition's `source` adapter - `SourceAdapter.open` when the first request connects, then `SourceSession.read` with
each request's context.
"""

from collections.abc import Iterator, Mapping
from typing import Any, Final

from streamwright.core.io.capabilities import SOURCE_CAPABILITIES
from streamwright.core.runtime.connector_types import ConnectorContext, ConnectorError
from streamwright.core.runtime.contract import (
    AdapterLoadError,
    ConnectorDefinition,
    InvalidConnectionError,
    Issue,
    Operation,
)
from streamwright.core.runtime.contract_validation import (
    check_arguments,
    check_connection,
    resolve_arguments,
    resolve_values,
)
from streamwright.core.sdk.operations import http

__all__ = ["CAPABILITIES", "check_call", "check_declared_auth", "check_resolved_auth", "describe_connector",
           "open_session", "read_call", "select_transport"]

CAPABILITIES: Final = SOURCE_CAPABILITIES


def _messages(prefix: str, issues: list[Issue] | tuple[Issue, ...]) -> list[str]:
    return ["%s%s: %s" % (prefix, "." + issue.path if issue.path else "", issue.message) for issue in issues]


def select_transport(definition: ConnectorDefinition) -> str:
    """`http` for a declarative HTTP connector (it offers the catalog's `http.request`), else `sdk`."""
    return "http" if definition.operation(http.REQUEST.service, http.REQUEST.method) else "sdk"


def check_declared_auth(definition: ConnectorDefinition, auth: Mapping[str, Any]) -> list[str]:
    """Problems with a source's `auth` block as written (without `provider`): keys, types, secret references."""
    return _messages("auth", check_connection(definition, auth))


def check_resolved_auth(definition: ConnectorDefinition, auth: Mapping[str, Any]) -> list[str]:
    """Problems with the rendered `auth` block: the resolved values and the vendor's checks (secrets never quoted)."""
    try:
        resolve_values(definition, auth)
    except InvalidConnectionError as exc:
        return _messages("auth", exc.issues)
    return []


def _operation(definition: ConnectorDefinition, call: Mapping[str, Any]) -> tuple[Operation | None, list[str]]:
    operation = definition.operation(call.get("service"), call.get("method"))
    if operation is None or not definition.runs(operation, CAPABILITIES):
        supported = definition.operations_for(CAPABILITIES)
        return None, ["%s: %s.%s is not supported (supported: %s)" % (
            definition.name, call.get("service"), call.get("method"),
            ", ".join(item.name for item in supported) or "none")]
    return operation, []


def check_call(definition: ConnectorDefinition, call: Mapping[str, Any]) -> list[str]:
    """Problems with an unrendered {service, method, arguments} call (query builder calls are BuiltQuery values)."""
    operation, problems = _operation(definition, call)
    if operation is None:
        return problems
    issues = check_arguments(definition, operation, call.get("arguments"), CAPABILITIES)
    return _messages("%s: %s" % (definition.name, operation.name), issues)


def open_session(definition: ConnectorDefinition, auth: Mapping[str, Any], context: ConnectorContext) -> Any:
    """The source adapter's session on the resolved `auth` block; its secret values are masked from now on."""
    try:
        connection = resolve_values(definition, auth)
        adapter = definition.load_adapter(CAPABILITIES.engine)
    except (InvalidConnectionError, AdapterLoadError) as exc:
        raise ConnectorError(context.redact(str(exc))) from None
    for value in connection.secret_values:
        context.secret(value)
    return adapter.open(connection, context)


def read_call(definition: ConnectorDefinition, session: Any, call: Mapping[str, Any],
              context: ConnectorContext) -> Iterator[Any]:
    """The pages of a rendered call, read by the session with the context of the request that makes it."""
    operation, problems = _operation(definition, call)
    arguments = call.get("arguments")
    if operation is not None:
        arguments = resolve_arguments(operation, arguments)
        issues = check_arguments(definition, operation, arguments, CAPABILITIES, rendered=True)
        problems = _messages("%s: %s" % (definition.name, operation.name), issues)
    if problems:
        raise ConnectorError("; ".join(problems))
    return session.read(operation, arguments or {}, context)


def describe_connector(definition: ConnectorDefinition) -> dict[str, Any]:
    """The declared contract for the source engine (nothing imported but the spec, nothing connected)."""
    return dict(definition.describe(CAPABILITIES), kind="connector", transport=select_transport(definition))
