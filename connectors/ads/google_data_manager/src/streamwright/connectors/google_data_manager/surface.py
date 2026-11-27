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
The installed google-ads-datamanager library is the authority on which services, methods and request fields exist
(contract v1.3 §6b). This module reads them from the library by introspection - no hardcoded vendor catalog, no
network - so a release that adds a service or a method needs no change here. `surface(service, method)` is the
connector's declared surface check (called by `validate` when the library is importable); the client reuses these
helpers to resolve calls and to name a clear error with "did you mean" suggestions.
"""

import functools
import json
from importlib import resources
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from google.ads import datamanager_v1 as library

from streamwright.core.sdk import suggest

__all__ = ["client_class", "method_names", "sdk_version", "service_names", "surface"]


def sdk_version() -> str:
    try:
        return "google-ads-datamanager %s" % version("google-ads-datamanager")
    except PackageNotFoundError:
        return "the installed google-ads-datamanager"


@functools.cache
def _methods() -> dict[str, tuple[str, ...]]:
    """{service: its methods}, from the library's own gapic_metadata.json (its synchronous gRPC client)."""
    metadata = json.loads(resources.files(library).joinpath("gapic_metadata.json").read_text(encoding="utf-8"))
    found = {}
    for service, spec in metadata.get("services", {}).items():
        rpcs = spec.get("clients", {}).get("grpc", {}).get("rpcs", {})
        found[service] = tuple(sorted(method for rpc in rpcs.values() for method in rpc.get("methods", ())))
    return found


def service_names() -> list[str]:
    return sorted(_methods())


def client_class(service: str) -> Any:
    """The generated `<service>Client` class, or None when the service does not exist."""
    return getattr(library, service + "Client", None) if service in _methods() else None


def method_names(service: str) -> list[str]:
    """The API methods of a service (none when it does not exist)."""
    return list(_methods().get(service, ()))


def surface(service: str, method: str) -> list[str]:
    """
    Problems if `service.method` is not a service and method of the installed google-ads-datamanager (§6b): a pure
    introspection (no network, no credentials) with "did you mean" suggestions, so a typo is caught before a run.
    """
    if client_class(service) is None:
        return ["%s is not a service in %s%s" % (service, sdk_version(),
                                                  suggest(service, service_names(), style="list"))]
    if method not in method_names(service):
        return ["%s has no %s method in %s%s" % (service, method, sdk_version(),
                                                  suggest(method, method_names(service), style="list"))]
    return []
