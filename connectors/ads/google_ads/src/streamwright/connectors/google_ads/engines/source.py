# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The google_ads connector on the `source` engine (streamwright run): GAQL reads through the Google Ads client."""

from collections.abc import Iterator, Mapping
from typing import Any

from streamwright.connectors.google_ads import client
from streamwright.core.io.api_calls import make_caller, translate_errors, translate_pages
from streamwright.core.sdk.contract import Operation, ResolvedConnection, vendor_arguments
from streamwright.core.sdk.engines.source import ConnectorContext

__all__ = ["GoogleAdsSession", "GoogleAdsSourceAdapter"]


class GoogleAdsSession:
    """Reads GAQL queries and the accessible customers; rate limits and retries come from the stream's context."""

    def __init__(self, connection: client.GoogleAdsConnection):
        self.connection = connection

    def read(self, operation: Operation, arguments: Mapping[str, Any],
             context: ConnectorContext) -> Iterator[list[dict[str, Any]]]:
        call = make_caller(context, client.translate_error)
        pages = client.read_pages(self.connection, operation.service, operation.method, arguments, call,
                                  vendor=vendor_arguments(operation, arguments))
        return translate_pages(pages, client.translate_error)

    def close(self) -> None:
        self.connection.services.clear()


class GoogleAdsSourceAdapter:
    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> GoogleAdsSession:
        with translate_errors(client.translate_error):  # the OAuth refresh happens when the client is made
            return GoogleAdsSession(client.connect(connection, context.secret))
