#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

"""Shared message operations for topic and queue connectors."""

from typing import Final

from streamwright.core.runtime.contract import Field, Operation, OperationKind

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
