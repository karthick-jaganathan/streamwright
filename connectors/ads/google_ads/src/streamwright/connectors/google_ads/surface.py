# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
The installed google-ads library is the authority on which services, read methods and request fields exist (contract
v1.3 §6b). This module reads those from the library by introspection - no hardcoded vendor catalog, no network - so a
vendor release that adds or deprecates a service needs no change here. `surface(service, method)` is the connector's
declared surface check (called by `validate` when the library is importable); the client reuses these helpers to
resolve calls and to name a clear error with "did you mean" suggestions.
"""

import difflib
import importlib
import pkgutil
import re
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from google.ads.googleads import client as library

__all__ = ["default_version", "did_you_mean", "read_method_names", "sdk_version", "service_names",
           "supported_versions", "surface"]


def supported_versions() -> list[str]:
    return list(getattr(library, "_VALID_API_VERSIONS", []))


def sdk_version() -> str:
    try:
        return "google-ads %s" % version("google-ads")
    except PackageNotFoundError:
        return "the installed google-ads"


def default_version() -> str:
    return getattr(library, "_DEFAULT_VERSION", None) or next(iter(supported_versions()), "")


def _snake(service: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", service).lower()


def did_you_mean(value: str, candidates: list[str]) -> str:
    matches = difflib.get_close_matches(value, candidates, n=3)
    return "; did you mean %s?" % ", ".join(matches) if matches else ""


def read_method_names(obj: Any) -> list[str]:
    """The read methods an object offers: `search`, `search_stream` and any `get_*`/`list_*`."""
    return sorted(name for name in dir(obj)
                  if (name in ("search", "search_stream") or name.startswith(("get_", "list_")))
                  and callable(getattr(obj, name, None)))


def _service_client_class(service: str, version_name: str) -> Any:
    """The generated `<service>Client` class of a version, or None when the service does not exist."""
    if not version_name:
        return None
    module_name = "google.ads.googleads.%s.services.services.%s" % (version_name, _snake(service))
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        return None
    return getattr(module, service + "Client", None)


def service_names(version_name: str) -> list[str]:
    """The service names a version offers, recovered from its `services.services` sub-packages."""
    if not version_name:
        return []
    package = importlib.import_module("google.ads.googleads.%s.services.services" % version_name)
    return ["".join(part.capitalize() for part in name.split("_"))
            for _, name, _ in pkgutil.iter_modules(package.__path__)]


def surface(service: str, method: str) -> list[str]:
    """
    Problems if `service.method` is not a readable service and method of the installed google-ads (§6b): a pure
    introspection (no network, no credentials) of the default API version's generated clients, with "did you mean"
    suggestions, so a typo is caught before a run.
    """
    version_name = default_version()
    client_class = _service_client_class(service, version_name)
    if client_class is None:
        return ["%s is not a service in %s%s" % (
            service, sdk_version(), did_you_mean(service, service_names(version_name)))]
    if not callable(getattr(client_class, method, None)):
        return ["%s has no %s method in %s%s" % (
            service, method, sdk_version(), did_you_mean(method, read_method_names(client_class)))]
    return []
