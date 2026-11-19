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
Apply operations (connector contract, `OperationKind.APPLY`): a job output whose rows are operations on a vendor's
objects - create or update campaigns, upload conversions - each row with its own outcome.

A connector offers them on its driver-side (`source`) adapter: the session it opens on a resolved connection also
implements `ApplySession.apply`, called with one chunk of rows at a time (at most the operation's `max_chunk`). The
engine has already checked the rows: every row's `op` (ACTION_COLUMN) is one of the operation's `actions` (missing:
the first), destructive actions only when the output allows them, and NULL values per the output's `nulls` policy
(dropped for `skip_field`, kept as None - "clear this field" - for `clear`). The session returns one RowResult per
row, in order; a vendor's partial failure is per-row results, a failed call is a ConnectorError (retried per the
output's `retry` through `context.call`). A session that sends a chunk as several requests (a request per row)
raises PartialApplyError when one is cut short: the results of the rows before it are recorded and the rest stay in
flight, like a cut-short call. A session may implement `reconcile` for `on_unknown: reconcile`: for rows whose earlier
call has an unknown outcome, the result of the object already in the vendor (None: not there).
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final, Protocol, runtime_checkable

from streamwright.core.runtime.connector_types import ConnectorContext, ConnectorError
from streamwright.core.runtime.contract import EngineCapabilities, Operation, OperationKind

__all__ = ["ACTION_COLUMN", "APPLIED", "APPLY_CAPABILITIES", "FAILED", "ApplySession", "PartialApplyError",
           "ReconcilingSession", "RowResult"]

ACTION_COLUMN: Final = "op"               # a row's action (create, update, remove, ...) when an operation has several
APPLIED: Final = "applied"
FAILED: Final = "failed"

# apply operations run on the driver through the connector's `source` adapter, never as a source stream's request
APPLY_CAPABILITIES: Final = EngineCapabilities(engine="source", operation_kinds=frozenset({OperationKind.APPLY}),
                                               arguments={})


@dataclass(frozen=True)
class RowResult:
    """The outcome of one row: `applied` with what the vendor returned (e.g. the new object's id), or `failed`."""
    status: str
    result: Mapping[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        if self.status not in (APPLIED, FAILED):
            raise ValueError("a row result is %s or %s, not %r" % (APPLIED, FAILED, self.status))

    @classmethod
    def applied(cls, result: Mapping[str, Any] | None = None) -> "RowResult":
        return cls(APPLIED, result)

    @classmethod
    def failed(cls, code: str | None, message: str) -> "RowResult":
        return cls(FAILED, None, code, message)


class PartialApplyError(ConnectorError):
    """
    A chunk cut short after its first rows were sent: `results` are those rows' outcomes, in order (fewer than the
    chunk's rows). The row being sent and the rows after it stay in flight (their outcome is unknown).
    """

    def __init__(self, message: str, results: Sequence[RowResult], *, code: Any = None):
        super().__init__(message, code=code, retryable=True)
        self.results = list(results)


@runtime_checkable
class ApplySession(Protocol):
    def apply(self, operation: Operation, arguments: Mapping[str, Any], rows: Sequence[Mapping[str, Any]],
              context: ConnectorContext) -> list[RowResult]:
        """One chunk: a RowResult per row, in order. Raises ConnectorError when the whole call fails."""
        ...


@runtime_checkable
class ReconcilingSession(Protocol):
    def reconcile(self, operation: Operation, arguments: Mapping[str, Any], rows: Sequence[Mapping[str, Any]],
                  context: ConnectorContext) -> list[RowResult | None]:
        """Per row whose earlier call has an unknown outcome: the existing object's result, or None (absent)."""
        ...
