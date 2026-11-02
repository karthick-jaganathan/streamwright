"""
Connector contract v1 (docs/design/connector-contract.md): what a connector's `spec.py` declares and the generic
checks engines run on it. A spec imports this module and the shared operation catalog
(streamwright.core.sdk.operations).
"""

from streamwright.core.runtime.contract import (
    ALL_ENGINES,
    ENGINES,
    FIELD_TYPES,
    SDK_VERSION,
    AdapterLoadError,
    ConnectorDefinition,
    DefinitionError,
    EngineCapabilities,
    EngineLimit,
    Field,
    InvalidConnectionError,
    Issue,
    Operation,
    OperationKind,
    ResolvedConnection,
    combine_capabilities,
    is_secret_reference,
)
from streamwright.core.runtime.contract_guide import connection_table, guide_text, operation_table
from streamwright.core.runtime.contract_validation import (
    check_arguments,
    check_connection,
    resolve_connection,
    resolve_values,
    vendor_arguments,
)

__all__ = [
    "ALL_ENGINES", "ENGINES", "FIELD_TYPES", "SDK_VERSION", "AdapterLoadError", "ConnectorDefinition",
    "DefinitionError", "EngineCapabilities", "EngineLimit", "Field", "InvalidConnectionError", "Issue", "Operation",
    "OperationKind", "ResolvedConnection", "check_arguments", "check_connection", "combine_capabilities",
    "connection_table", "guide_text", "is_secret_reference", "operation_table", "resolve_connection", "resolve_values",
    "vendor_arguments",
]
