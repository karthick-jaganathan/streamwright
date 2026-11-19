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
What the meta_ads connector knows about the Meta Marketing API, shared by its engines: the connection rules, the
FacebookAdsApi built for one access token, reads of objects and edges (paged by the connector), the bound methods
writes call, and the SDK's errors as ConnectorError (with the code, retryability and the wait the throttling headers
ask for).
"""

import datetime
import difflib
import importlib
import json
import pkgutil
from collections.abc import Callable, Iterator, Mapping
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Final

import requests
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError
from facebook_business.session import FacebookSession

from streamwright.core.sdk import ConnectorError, suggest
from streamwright.core.sdk.contract import ResolvedConnection
from streamwright.core.sdk.network import http_details

__all__ = ["api_message", "bound_method", "connect", "export", "read_pages", "surface", "translate_error",
           "validate_connection"]

PAGE_SIZE: Final = 500
TIMEOUT_SECONDS: Final = 300  # insights can take minutes
_NODE_METHODS: Final = ("api_get", "api_update", "api_delete")
Call = Callable[..., Any]


def validate_connection(connection: Mapping[str, Any]) -> list[str]:
    """Why a resolved connection cannot be used: an empty access token."""
    return ["access_token is empty"] if connection["access_token"] == "" else []


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def export(item: Any) -> Any:
    """An SDK object as its plain data (other values as they are)."""
    return item.export_all_data() if hasattr(item, "export_all_data") else item


def _network_details(redact: Callable[[Any], str]) -> Callable[..., None]:
    """A requests response hook: the exchange's headers and bodies on `streamwright.network` at DEBUG."""
    def hook(response: Any, *args: Any, **kwargs: Any) -> None:
        http_details(response, redact)
    return hook


def connect(connection: ResolvedConnection, context: Any) -> FacebookAdsApi:
    """An API object of our own (no global default API, no crash reporter) for the connection's access token."""
    session = FacebookSession(app_id=connection.get("app_id") or None, app_secret=connection.get("app_secret") or None,
                              access_token=str(connection["access_token"]), timeout=TIMEOUT_SECONDS)
    if connection.get("app_secret"):
        context.secret(session.appsecret_proof)
    session.requests.hooks["response"].append(_network_details(context.redact))
    return FacebookAdsApi(session, api_version=connection.get("api_version") or None)


def _sdk_version() -> str:
    try:
        return "facebook_business %s" % version("facebook_business")
    except PackageNotFoundError:
        return "the installed facebook_business"


def _object_module_names() -> list[str]:
    """The adobjects submodule names (lower-case) of the installed SDK, one per Marketing API object."""
    package = importlib.import_module("facebook_business.adobjects")
    return [name for _, name, _ in pkgutil.iter_modules(package.__path__)]


def _object_class(service: str) -> Any:
    """The adobjects class of `service` (e.g. AdAccount -> adobjects.adaccount.AdAccount), or None if it is not one."""
    try:
        module = importlib.import_module("facebook_business.adobjects.%s" % service.lower())
    except ImportError:
        return None
    cls = getattr(module, service, None)
    return cls if isinstance(cls, type) else None


def _methods(cls: Any) -> list[str]:
    """The methods an object offers: `api_get`, `api_update`, `api_delete`, its `get_<edge>` and `create_<edge>`."""
    return sorted(name for name in dir(cls) if name in _NODE_METHODS or name.startswith(("get_", "create_")))


def _object_suggestions(service: str) -> list[str]:
    """Object class names close to `service`, recovered from the close adobjects submodules."""
    found = []
    for name in difflib.get_close_matches(service.lower(), _object_module_names(), n=3):
        module = importlib.import_module("facebook_business.adobjects.%s" % name)
        found += [attr for attr in dir(module) if attr.lower() == name and isinstance(getattr(module, attr), type)]
    return found


def surface(service: str, method: str) -> list[str]:
    """
    Problems if `service.method` is not an object and method of the installed facebook_business (§6b): a pure
    introspection (no network), with the SDK's own "did you mean" suggestions, so a typo is caught before a run.
    """
    cls = _object_class(service)
    if cls is None:
        return ["%s is not a Marketing API object in %s%s" % (
            service, _sdk_version(), suggest(service, _object_suggestions(service), style="list"))]
    if not callable(getattr(cls, method, None)):
        return ["%s has no %s method in %s%s" % (
            service, method, _sdk_version(), suggest(method, _methods(cls), style="list"))]
    return []


def _node(api: FacebookAdsApi, service: str, identifier: Any) -> Any:
    """The object `service` with `identifier`; an unknown object fails without a retry, naming the SDK version."""
    cls = _object_class(service)
    if cls is None:
        raise ConnectorError("meta_ads: %s is not a Marketing API object in %s%s" % (
            service, _sdk_version(), suggest(service, _object_suggestions(service), style="list")), retryable=False)
    return cls(str(identifier), api=api)


def bound_method(api: FacebookAdsApi, service: str, identifier: Any, method: str) -> Call:
    """
    `method` of the object `service` with `identifier`; an unknown object or method fails without a retry, naming the
    SDK version and suggestions.
    """
    node = _node(api, service, identifier)
    bound = getattr(node, method, None)
    if not callable(bound):
        raise ConnectorError("meta_ads: %s has no %s method in %s%s" % (
            service, method, _sdk_version(), suggest(method, _methods(type(node)), style="list")), retryable=False)
    return bound


def read_pages(api: FacebookAdsApi, service: str, method: str, arguments: Mapping[str, Any],
               call: Call, *, vendor: Mapping[str, Any] | None = None) -> Iterator[list[dict[str, Any]]]:
    """The pages of one read: the object itself (`api_get`), or an edge's records in pages of PAGE_SIZE.

    `vendor` are the operation's undeclared arguments: Graph API parameters (time_increment, breakdowns,
    action_breakdowns, filtering, action_attribution_windows, ...), merged into the request's `params` (an explicit
    `params` entry wins where they collide).
    """
    read = bound_method(api, service, arguments["id"], method)
    fields = arguments.get("fields") or []
    if isinstance(fields, str):
        fields = [field.strip() for field in fields.split(",") if field.strip()]
    params = _jsonable({**(vendor or {}), **(arguments.get("params") or {})})
    result = call(lambda: read(fields=fields, params=params))  # edges load their first page here
    if method == "api_get":
        yield [export(result)]
        return
    load_next_page = result.load_next_page

    def next_page() -> Any:  # every later page: rate limit and retries; none once the edge is read (no request)
        if getattr(result, "_finished_iteration", False):
            return False
        return call(load_next_page)
    result.load_next_page = next_page
    page: list[dict[str, Any]] = []
    for item in result:
        page.append(export(item))
        if len(page) >= PAGE_SIZE:
            yield page
            page = []
    if page:
        yield page


def _regain_seconds(headers: Mapping[str, Any] | None) -> float | None:
    """Seconds until access is restored, from the x-business-use-case-usage header (in minutes)."""
    value = None
    for key in headers or {}:
        if key.lower() == "x-business-use-case-usage":
            value = headers[key]
    try:
        usage = json.loads(value) if value else {}
    except ValueError:
        return None
    minutes = [entry.get("estimated_time_to_regain_access") or 0 for entries in usage.values()
               if isinstance(entries, list) for entry in entries if isinstance(entry, dict)]
    longest = max(minutes or [0])
    return longest * 60 if longest else None


def _error_body(exc: FacebookRequestError) -> dict[str, Any]:
    body = exc.body() if isinstance(exc.body(), dict) else {}
    return body["error"] if isinstance(body.get("error"), dict) else {}


def api_message(exc: FacebookRequestError) -> str:
    """The Graph API error's message, with Meta's explanation for users when it has one (e.g. which value is wrong)."""
    message = exc.api_error_message() or exc.get_message()
    user_message = _error_body(exc).get("error_user_msg")
    return "%s: %s" % (message, user_message) if user_message else message


def translate_error(exc: Exception) -> ConnectorError | None:
    """The ConnectorError of a Graph API or connection exception; None for exceptions that are not API errors."""
    if isinstance(exc, FacebookRequestError):
        code, subcode, status = exc.api_error_code(), exc.api_error_subcode(), exc.http_status()
        trace = _error_body(exc).get("fbtrace_id")
        details = ["code %s" % code] + (["subcode %s" % subcode] if subcode else []) + ["HTTP %s" % status] + \
            (["fbtrace_id %s" % trace] if trace else [])
        # the codes worth retrying are configuration (the spec's `retry_codes`, or a stream's `retry.codes`);
        # Meta's own transient flag and a 5xx are the vendor's judgement
        transient = bool(exc.api_transient_error()) or (isinstance(status, int) and status >= 500)
        return ConnectorError("meta_ads: %s (%s)" % (api_message(exc), ", ".join(details)),
                              codes=(code, subcode), retryable=transient,
                              retry_after=_regain_seconds(exc.http_headers()))
    if isinstance(exc, (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError,
                        requests.exceptions.ContentDecodingError)):  # including connections dropped mid-page
        return ConnectorError("meta_ads: %s: %s" % (type(exc).__name__, exc), retryable=True)
    return None
