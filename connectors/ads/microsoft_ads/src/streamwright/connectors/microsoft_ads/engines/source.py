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
The microsoft_ads connector on the `source` engine (streamwright run, and job call steps and apply outputs): SOAP
calls through the bingads SDK, reads and apply operations (Add*, Update*, Delete*, conversion uploads).
"""

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from streamwright.connectors.microsoft_ads import client, writes
from streamwright.core.sdk.apply import RowResult
from streamwright.core.sdk.contract import Operation, ResolvedConnection, vendor_arguments
from streamwright.core.sdk.engines.source import ConnectorContext, make_caller, translate_errors, translate_pages

__all__ = ["MicrosoftAdsSession", "MicrosoftAdsSourceAdapter"]


class MicrosoftAdsSession:
    """
    Makes SOAP calls and applies rows (streamwright.core.sdk.apply); rate limits and retries come from the stream's
    or output's context.
    """

    def __init__(self, connection: client.MicrosoftAdsConnection):
        self.connection = connection

    def read(self, operation: Operation, arguments: Mapping[str, Any],
             context: ConnectorContext) -> Iterator[list[Any]]:
        call = make_caller(context, client.translate_error)
        pages = client.read_pages(self.connection, operation.service, operation.method, arguments, call,
                                  vendor=vendor_arguments(operation, arguments))
        return translate_pages(pages, client.translate_error)

    def apply(self, operation: Operation, arguments: Mapping[str, Any], rows: Sequence[Mapping[str, Any]],
              context: ConnectorContext) -> list[RowResult]:
        call = make_caller(context, client.translate_error)
        with translate_errors(client.translate_error):
            return writes.apply_rows(self.connection, operation.service, operation.method, arguments=arguments,
                                     rows=rows, call=call, vendor=vendor_arguments(operation, arguments))

    def close(self) -> None:
        self.connection.services.clear()


class MicrosoftAdsSourceAdapter:
    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> MicrosoftAdsSession:
        with translate_errors(client.translate_error):  # the OAuth token is requested when the session is made
            return MicrosoftAdsSession(client.connect(connection, context.secret))
