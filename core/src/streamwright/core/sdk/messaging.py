"""Shared messaging policy for source reads and job brokers."""

from types import MappingProxyType
from typing import Final

from streamwright.core.config.inputs import InputError, parse_duration
from streamwright.core.jobs.types import Broker as BrokerClient
from streamwright.core.jobs.types import Message
from streamwright.core.sdk.contract import EngineCapabilities, OperationKind

__all__ = ["SOURCE_CAPABILITIES", "BrokerClient", "Message", "check_poll", "poll_limit", "poll_wait_seconds"]


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


SOURCE_CAPABILITIES: Final = EngineCapabilities(
    engine="source",
    operation_kinds=frozenset({OperationKind.READ}),
    arguments=MappingProxyType({"message.read": dict.fromkeys(("destination", "max_messages", "max_wait"))}),
    checks=MappingProxyType({"message.read": check_poll}),
)
