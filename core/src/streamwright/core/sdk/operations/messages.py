"""Shared message operations for topic and queue connectors."""

from typing import Final

from streamwright.core.sdk.contract import Field, Operation, OperationKind

__all__ = ["CONSUME", "PUBLISH", "READ"]

_DESTINATION: Final = "the topic or queue to read from or publish to"
_POLL: Final = (
    Field("destination", "string", required=True, description=_DESTINATION),
    Field("max_messages", "integer", default=100, description="maximum messages in one poll"),
    Field("max_wait", "duration", default="10s", description="maximum time to wait for messages"),
)

READ: Final = Operation(
    service="message", method="read", kind=OperationKind.READ,
    description="Inspect messages without acknowledging them.", arguments=_POLL,
)
CONSUME: Final = Operation(
    service="message", method="consume", kind=OperationKind.CONSUME,
    description="Consume messages; acknowledge only after outputs and the outbox commit.", arguments=_POLL,
)
PUBLISH: Final = Operation(
    service="message", method="publish", kind=OperationKind.PUBLISH,
    description="Publish events to a destination with a stable deduplication key.",
    arguments=(Field("destination", "string", required=True, description=_DESTINATION),),
)
