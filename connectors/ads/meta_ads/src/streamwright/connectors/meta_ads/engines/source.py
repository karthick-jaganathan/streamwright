# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""The meta_ads connector on the `source` engine (streamwright run): reads through the Marketing API SDK."""

from collections.abc import Iterator, Mapping
from typing import Any

from facebook_business.api import FacebookAdsApi

from streamwright.connectors.meta_ads import client
from streamwright.core.io.api_calls import make_caller, translate_pages
from streamwright.core.sdk.contract import Operation, ResolvedConnection, vendor_arguments
from streamwright.core.sdk.engines.source import ConnectorContext

__all__ = ["MetaAdsSession", "MetaAdsSourceAdapter"]


class MetaAdsSession:
    """Reads objects and edges; rate limits and retries come from the stream's context."""

    def __init__(self, api: FacebookAdsApi):
        self.api = api

    def read(self, operation: Operation, arguments: Mapping[str, Any],
             context: ConnectorContext) -> Iterator[list[dict[str, Any]]]:
        call = make_caller(context, client.translate_error)
        pages = client.read_pages(self.api, operation.service, operation.method, arguments, call,
                                  vendor=vendor_arguments(operation, arguments))
        return translate_pages(pages, client.translate_error)

    def close(self) -> None:
        self.api = None


class MetaAdsSourceAdapter:
    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> MetaAdsSession:
        return MetaAdsSession(client.connect(connection, context))
