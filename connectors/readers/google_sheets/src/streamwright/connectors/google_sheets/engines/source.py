# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The google_sheets connector on the `source` engine (streamwright run): reads through the Sheets API v4."""

from collections.abc import Iterator, Mapping
from typing import Any

from streamwright.connectors.google_sheets import client
from streamwright.core.io.api_calls import make_caller, translate_pages
from streamwright.core.sdk.contract import Operation, ResolvedConnection, vendor_arguments
from streamwright.core.sdk.engines.source import ConnectorContext

__all__ = ["GoogleSheetsSession", "GoogleSheetsSourceAdapter"]


class GoogleSheetsSession:
    """Reads sheets; rate limits and retries come from the stream's context."""

    def __init__(self, sheets: client.SheetsClient):
        self.sheets = sheets

    def read(self, operation: Operation, arguments: Mapping[str, Any],
             context: ConnectorContext) -> Iterator[list[dict[str, Any]]]:
        call = make_caller(context, client.translate_error)
        pages = client.read_pages(self.sheets, operation.method, arguments, call,
                                  vendor_arguments(operation, arguments))
        return translate_pages(pages, client.translate_error)

    def close(self) -> None:
        self.sheets.http.session.close()


class GoogleSheetsSourceAdapter:
    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> GoogleSheetsSession:
        return GoogleSheetsSession(client.connect(connection, context.secret, context.redact))
