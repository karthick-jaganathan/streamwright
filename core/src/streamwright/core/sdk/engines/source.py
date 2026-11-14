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
The `source` engine adapter protocol (streamwright run): a connector's `engines/source.py` opens a session on a
resolved connection; the session reads an operation's pages with the context of the request that reads them (its
stream's rate limit, retries, metrics and log fields). Errors a caller can act on are raised as ConnectorError (with a
provider `code`); messages never quote secret values.
"""

from collections.abc import Iterator, Mapping
from typing import Any, Protocol, runtime_checkable

from streamwright.core.runtime.connector_types import ConnectorContext, ConnectorError
from streamwright.core.runtime.contract import Operation, ResolvedConnection

__all__ = ["ConnectorContext", "ConnectorError", "SourceAdapter", "SourceSession"]


@runtime_checkable
class SourceSession(Protocol):
    def read(self, operation: Operation, arguments: Mapping[str, Any],
             context: ConnectorContext) -> Iterator[list[dict[str, Any]]]:
        """The pages (lists of records) of one call with rendered, checked arguments; `context.call` applies the
        requesting stream's rate limit and retries."""
        ...

    def close(self) -> None: ...


@runtime_checkable
class SourceAdapter(Protocol):
    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> SourceSession:
        """A session on the connection (`context`: of the request that opens it, for calls made while connecting)."""
        ...
