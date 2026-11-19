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

"""
What the engines support of the message operations (`streamwright.core.runtime.operations.messages`): the shared
polling policy of source reads and job brokers (`max_messages`, `max_wait`), MESSAGE_SOURCE_CAPABILITIES and a job
engine's capabilities (job_capabilities; MESSAGE_DUCKDB_CAPABILITIES). Connectors reach it as
`streamwright.core.sdk.messaging`.
"""

from types import MappingProxyType
from typing import Final

from streamwright.core.config.inputs import InputError, parse_duration
from streamwright.core.runtime.contract import EngineCapabilities, OperationKind

__all__ = ["MESSAGE_DUCKDB_CAPABILITIES", "MESSAGE_SOURCE_CAPABILITIES", "check_poll", "job_capabilities", "poll_limit",
           "poll_wait_seconds"]


def check_poll(arguments):
    problems = []
    if "max_messages" in arguments and (
        isinstance(arguments["max_messages"], bool) or not isinstance(arguments["max_messages"], int)
        or arguments["max_messages"] < 1
    ):
        problems.append("max_messages must be a positive integer")
    if "max_wait" in arguments and "{{" not in str(arguments["max_wait"]):
        try:
            if parse_duration(arguments["max_wait"]).total_seconds() <= 0:
                problems.append("max_wait must be positive")
        except InputError:
            problems.append("max_wait must be a duration")
    return problems


def poll_limit(arguments, default):
    return int(arguments.get("max_messages", default))


def poll_wait_seconds(arguments, default):
    return parse_duration(arguments["max_wait"]).total_seconds() if "max_wait" in arguments else default


MESSAGE_SOURCE_CAPABILITIES: Final = EngineCapabilities(
    engine="source",
    operation_kinds=frozenset({OperationKind.READ}),
    arguments=MappingProxyType({"message.read": dict.fromkeys(("destination", "max_messages", "max_wait"))}),
    checks=MappingProxyType({"message.read": check_poll}),
)


def job_capabilities(engine):
    """A job engine's messaging: a step consumes a broker's messages (the batch's input), an output publishes events."""
    return EngineCapabilities(
        engine=engine,
        operation_kinds=frozenset({OperationKind.CONSUME, OperationKind.PUBLISH}),
        arguments=MappingProxyType({
            "message.consume": dict.fromkeys(("destination", "max_messages", "max_wait")),
            "message.publish": dict.fromkeys(("destination",)),
        }),
        checks=MappingProxyType({"message.consume": check_poll}),
    )


MESSAGE_DUCKDB_CAPABILITIES: Final = job_capabilities("duckdb")
