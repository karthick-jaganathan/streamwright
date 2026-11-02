"""
The `source` adapter of declarative HTTP connectors (`streamwright.core.sdk.operations.http`). They have no vendor
code: the source's `auth:` and `http:` blocks declare the API, and the source engine's HTTP reader runs its `http:`
requests with the authentication the connection declares. A connector's `sdk:` request would reach this adapter, which
says so.
"""

from streamwright.core.runtime.connector_types import ConnectorContext, ConnectorError
from streamwright.core.runtime.contract import ResolvedConnection

__all__ = ["HttpSourceAdapter"]


class HttpSourceAdapter:
    """Declarative HTTP connectors run `http:` requests in the engine; there is no session to open."""

    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> None:
        raise ConnectorError("%s is a REST API connector: use an http request (http: {path, method, ...}), "
                             "not sdk" % connection.connector)
