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
Microsoft Advertising types from the WSDLs bundled with the installed bingads, offline: an enum's values (e.g.
ReportAggregation, PredefinedTime) or an object's fields and their types (e.g. CampaignPerformanceReportRequest).
"""

import functools
import glob
import logging
import os
from contextlib import contextmanager
from typing import Any

import bingads
from suds.client import Client

__all__ = ["describe_type"]

@functools.cache
def _clients() -> tuple[Client, ...]:
    """A client of each service WSDL the installed bingads bundles (production), read once."""
    folder = os.path.join(os.path.dirname(bingads.__file__), "v13", "proxies", "production")
    return tuple(Client("file://" + path, cache=None) for path in sorted(glob.glob(os.path.join(folder, "*.xml"))))


@contextmanager
def _quiet() -> Any:
    """suds logs every type it does not find as an error: a lookup that tries each service hides them."""
    logger = logging.getLogger("suds.resolver")
    level = logger.level
    logger.setLevel(logging.CRITICAL)
    try:
        yield
    finally:
        logger.setLevel(level)


def describe_type(name: str) -> dict[str, Any] | None:
    """The type `name` of the first service that declares it, or None."""
    with _quiet():
        for client in _clients():
            found = client.factory.resolver.find(name)
            if found is None:
                continue
            service = str(client.wsdl.services[0].name)
            children = [child for child, _ in found.children()]
            if found.enum():
                return {"kind": "enum", "service": service, "values": [str(child.name) for child in children]}
            return {"kind": "object", "service": service,
                    "fields": {str(child.name): str(child.type[0]) if child.type else None for child in children}}
    return None
