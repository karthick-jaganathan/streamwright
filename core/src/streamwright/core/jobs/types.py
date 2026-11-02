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
Engine-neutral contracts of the jobs framework: the data types shared by the runner, the state store and the
engines, the `Broker` base class and the `Executor` protocol.

Nothing here imports an engine: a DataFrame or a DuckDB relation is `Any`.
"""

import abc
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "CONTRACT_VERSION",
    "BatchContext",
    "Broker",
    "ExecutionHooks",
    "ExecutionResult",
    "Executor",
    "Lease",
    "Message",
    "Observation",
    "WriteReceipt",
    "written_files",
]

CONTRACT_VERSION = "1"


@dataclass(frozen=True)
class Lease:
    """A job's exclusive lease; every state write carries it and is rejected when `epoch` is no longer current."""
    job_name: str
    owner_id: str
    epoch: int
    expires_at: str                                  # ISO-8601 UTC, computed by the database clock


@dataclass(frozen=True)
class Observation:
    """Metrics gathered by a write (the commit's own metrics, for example)."""
    name: str
    values: Mapping[str, Any]


def written_files(receipt):
    """The files a write named beyond its destination (a file_name template's file, or split_by's files)."""
    if isinstance(receipt, dict):
        files, destination = receipt.get("files") or (), receipt.get("destination")
    else:
        files, destination = receipt.files, receipt.destination
    return [path for path in files if path != destination]


@dataclass(frozen=True)
class WriteReceipt:
    """What a write returns; stored with the batch so a replay skips the output."""
    output_name: str
    destination: str                                 # table, table path or object prefix
    rows: int | None
    version: int | None = None                    # table version the write committed (table formats)
    files: Sequence[str] = ()                        # final object names (split exports)
    predicate: str | None = None                  # rendered replace_where / merge target predicate
    observations: Sequence[Observation] = ()
    already_committed: bool = False                  # the write was skipped: its commit was found from an earlier try

    def to_dict(self):
        return {"output_name": self.output_name, "destination": self.destination, "rows": self.rows,
                "version": self.version, "files": list(self.files), "predicate": self.predicate,
                "already_committed": self.already_committed,
                "observations": [{"name": o.name, "values": dict(o.values)} for o in self.observations]}

    @classmethod
    def from_dict(cls, data):
        return cls(data["output_name"], data["destination"], data.get("rows"), data.get("version"),
                   tuple(data.get("files") or ()), data.get("predicate"),
                   tuple(Observation(o["name"], o["values"]) for o in data.get("observations") or ()),
                   bool(data.get("already_committed")))


@dataclass(frozen=True)
class Message:
    """One broker message, pinned in the batch manifest until it is acknowledged."""
    message_id: str
    key: str | None
    body: str
    headers: Mapping[str, str]
    timestamp: str | None
    receipt: Any = None                              # opaque to the runner; the broker acks with it

    def to_dict(self):
        return {"message_id": self.message_id, "key": self.key, "body": self.body,
                "headers": dict(self.headers or {}), "timestamp": self.timestamp, "receipt": self.receipt}


@dataclass(frozen=True)
class BatchContext:
    """Run constants fixed when a batch is first received; a replay reuses them."""
    batch_id: str
    batch_sequence: int                              # increases per batch of a job and is never reused
    batch_ts: str                                    # ISO-8601 UTC, pinned in the manifest
    job_name: str
    lease_epoch: int
    store_identity: str                              # stable id of the state store that issued the sequence
    engine_metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionHooks:
    """
    What an executor calls back into while a batch runs:
      fence()                   before every output write; raises StaleLeaseError once the lease is lost
      save_manifest(manifest)   once the steps are built (pinned values are durable before the first write)
      record_receipt(receipt, manifest)   after each output write, with the manifest, in one transaction
    """
    fence: Callable[[], None]
    save_manifest: Callable[[Mapping[str, Any]], None]
    record_receipt: Callable[[WriteReceipt, Mapping[str, Any]], None]


@dataclass(frozen=True)
class ExecutionResult:
    """What an executor returns: events per publishing output, accepted file signatures and schema reports."""
    events: Mapping[str, list[Mapping[str, Any]]] = field(default_factory=dict)
    schema_signatures: Sequence[tuple[str, str, str]] = ()       # (path_prefix, header_hash, signature)
    schema_reports: Sequence[Any] = ()


@runtime_checkable
class Executor(Protocol):
    def execute(self, context: BatchContext, manifest: dict[str, Any], done: Mapping[str, Mapping[str, Any]],
                hooks: ExecutionHooks) -> ExecutionResult:
        """
        Builds the steps and writes every output that has no receipt in `done` (output name -> receipt dict),
        calling hooks.fence() right before each write. `manifest["pinned"]` holds the values to replay.
        """


class Broker(abc.ABC):
    """
    A message source and event sink (a message broker). Optional capabilities are explicit: a broker that can report
    its end boundary sets `supports_watermarks` and overrides `watermarks`.
    """
    contract_version = CONTRACT_VERSION
    supports_watermarks = False

    @abc.abstractmethod
    def poll(self, arguments: Mapping[str, Any], limit: int, max_wait_seconds: float,
             watermarks: Any = None) -> list[Message]:
        """Never commits on its own; with watermarks, returns nothing past them."""

    @abc.abstractmethod
    def ack(self, messages: Sequence[Message], arguments: Mapping[str, Any]) -> None:
        """Called only after the manifest and the outbox are durable."""

    @abc.abstractmethod
    def publish(self, arguments: Mapping[str, Any], events: Sequence[Mapping[str, Any]], dedupe_key: str) -> None:
        """At least once; `dedupe_key` is stable across replays so consumers can drop a replayed event."""

    @abc.abstractmethod
    def destination(self, arguments: Mapping[str, Any]) -> str | None:
        """The topic or queue a publish goes to (stored with each outbox event)."""

    def watermarks(self, arguments: Mapping[str, Any]) -> Any:
        raise NotImplementedError("%s does not support watermarks" % type(self).__name__)

    def poll_limit(self, arguments: Mapping[str, Any], default: int) -> int:
        """How many messages one batch takes (a broker reads its own argument for it)."""
        return default

    def poll_wait_seconds(self, arguments: Mapping[str, Any], default: float) -> float:
        """How long one poll waits for messages."""
        return default
