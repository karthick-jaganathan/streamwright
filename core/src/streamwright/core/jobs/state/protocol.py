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

"""The bookkeeping store contract (runtime.state) shared by every engine and its errors."""

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from streamwright.core.jobs.types import Lease, WriteReceipt

__all__ = ["STATUSES", "TERMINAL_STATUSES", "LeaseHeldError", "StaleLeaseError", "StateError", "StateStore"]

# received -> processing -> committed -> acked -> completed; any other -> failed | abandoned;
# failed -> the status it failed from (retry) | abandoned
STATUSES = ("received", "processing", "committed", "acked", "completed", "failed", "abandoned")
TERMINAL_STATUSES = ("completed", "abandoned")


class StateError(Exception):
    """A bookkeeping operation that is not allowed (unknown status, impossible transition, bad sequence)."""


class StaleLeaseError(StateError):
    """The lease was taken over (or expired and was renewed by another worker): stop writing."""


class LeaseHeldError(StateError):
    """Another worker holds the job's lease."""


@runtime_checkable
class StateStore(Protocol):
    def initialize(self) -> None:
        """Creates or migrates the bookkeeping tables (ordered, idempotent migrations)."""

    def store_identity(self) -> str:
        """A stable identity of this state store, used to make per-run idempotency keys unique across stores."""

    def acquire_lease(self, job_name: str, owner_id: str, ttl_seconds: int) -> Lease: ...
    def renew_lease(self, lease: Lease, ttl_seconds: int) -> Lease: ...
    def check_lease(self, lease: Lease) -> None:
        """Raises StaleLeaseError unless `lease` is still the current, unexpired lease."""
    def release_lease(self, lease: Lease) -> None: ...
    def get_lease(self, job_name: str) -> dict[str, Any] | None: ...

    def next_batch_sequence(self, job_name: str) -> int:
        """One more than the highest sequence the job ever used; purging batches never lowers it."""

    def record_batch(self, lease: Lease, batch_id: str, status: str, manifest: Mapping[str, Any], *,
                     batch_sequence: int | None = None, run_key: str | None = None,
                     error: str | None = None, engine_metadata: Mapping[str, Any] | None = None) -> None:
        """Creates a batch (needs batch_sequence) or moves it along the status machine; fenced by the lease."""

    def record_receipt(self, lease: Lease, batch_id: str, receipt: WriteReceipt,
                       manifest: Mapping[str, Any] | None = None) -> None:
        """Stores an output's receipt (and the batch manifest, in the same transaction)."""

    def commit_batch(self, lease: Lease, batch_id: str, manifest: Mapping[str, Any],
                     events: Mapping[str, tuple[str, Sequence[Mapping[str, Any]]]],
                     schema_signatures: Sequence[tuple[str, str, str]] = ()) -> None:
        """
        Marks the batch committed and stores its outbox events ({output: (destination, rows)}) and the accepted
        schema signatures ([(path prefix, header hash, signature)]) atomically.
        """

    def retry_batch(self, lease: Lease, batch_id: str) -> str:
        """Moves a failed batch back to the status it failed from; returns that status."""

    def get_receipts(self, job_name: str, batch_id: str) -> dict[str, dict[str, Any]]: ...
    def get_batch(self, job_name: str, batch_id: str) -> dict[str, Any] | None: ...
    def find_batch_by_run_key(self, job_name: str, run_key: str) -> dict[str, Any] | None: ...
    def find_latest_completed_batch(self, job_name: str) -> dict[str, Any] | None: ...
    def list_incomplete_batches(self, job_name: str) -> list[dict[str, Any]]: ...
    def list_batches(self, job_name: str, limit: int = 20) -> list[dict[str, Any]]: ...
    def list_pending_events(self, job_name: str) -> list[dict[str, Any]]: ...
    def mark_events_sent(self, lease: Lease, batch_id: str, output_name: str,
                         event_sequences: Sequence[int]) -> None: ...
    def purge_batches(self, job_name: str, older_than: Any) -> int:
        """Deletes completed/abandoned batches last updated before `older_than` (an aware datetime)."""

    def get_schema_signatures(self, job_name: str) -> dict[str, str]: ...
