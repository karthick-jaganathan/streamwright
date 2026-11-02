# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the google_ads connector knows about the Google Ads API, shared by its engines: the connection rules, the
GoogleAdsClient and its services, rows as plain dicts keyed like GAQL fields, and the library's errors as
ConnectorError (with the code, retryability and wait the API reports).
"""

import contextlib
import importlib
import re
from collections.abc import Callable, Iterator, Mapping
from typing import Any, Final

import grpc
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.api_core import exceptions as api_errors
from google.auth import exceptions as auth_errors
from google.protobuf.json_format import MessageToDict

from streamwright.connectors.google_ads.surface import (
    default_version,
    did_you_mean,
    read_method_names,
    sdk_version,
    service_names,
    supported_versions,
)
from streamwright.core.sdk import ConnectorError
from streamwright.core.sdk.contract import ResolvedConnection

__all__ = ["GoogleAdsConnection", "connect", "customer_id", "read_pages", "remember_tokens", "rows",
           "supported_versions", "translate_error", "validate_connection"]

CREDENTIALS: Final = ("developer_token", "client_id", "client_secret", "refresh_token")
_CUSTOMER_ID: Final = re.compile(r"^[0-9]+$")


def _digits(value: Any) -> str | None:
    """A customer ID's digits (dashes are removed), or None when it is not one."""
    text = str(value if value is not None else "").replace("-", "").strip()
    return text if _CUSTOMER_ID.match(text) else None


def customer_id(value: Any, key: str) -> str:
    digits = _digits(value)
    if digits is None:
        raise ConnectorError("google_ads: %s %r is not a Google Ads customer ID (digits; dashes are removed)" % (
            key, value))
    return digits


def validate_connection(connection: Mapping[str, Any]) -> list[str]:
    """Why a resolved connection cannot be used: an empty credential, a login customer or an API version it lacks."""
    problems = ["%s is empty" % key for key in CREDENTIALS if connection[key] == ""]
    login = connection.get("login_customer_id")
    if login not in (None, "") and _digits(login) is None:
        problems.append("login_customer_id %r is not a Google Ads customer ID (digits; dashes are removed)" % (login,))
    version, supported = connection.get("api_version"), supported_versions()
    if version and supported and version not in supported:
        problems.append("api_version %r is not supported by the installed google-ads library (supported: %s); upgrade "
                        "google-ads or change api_version" % (version, ", ".join(supported)))
    return problems


# * ----
# * rows
# * ----

def _gaql_names(data: dict[str, Any], descriptor: Any) -> dict[str, Any]:
    """
    Renames, in place, the fields the Python library renamed (`type_`, since `type` is a Python name) back to their
    GAQL names. Only field names change: map keys are data and stay as they are.
    """
    for key in list(data):  # the fields the row has, not the hundreds its message declares
        field = descriptor.fields_by_name.get(key)
        if field is None:
            continue
        message = field.message_type
        if message is not None and not message.GetOptions().map_entry:
            value = data[key]
            for item in value if isinstance(value, list) else [value]:
                if isinstance(item, dict):  # well-known types (e.g. timestamps) are rendered as text
                    _gaql_names(item, message)
        if key.endswith("_") and field.json_name == key[:-1]:
            data[field.json_name] = data.pop(key)
    return data


def rows(response: Any) -> list[dict[str, Any]]:
    """The rows of one SearchGoogleAdsResponse / SearchGoogleAdsStreamResponse as plain dicts."""
    found = []
    for row in response.results:
        message = getattr(row, "_pb", row)
        found.append(_gaql_names(MessageToDict(message, preserving_proto_field_name=True), message.DESCRIPTOR))
    return found


# * ----------
# * connection
# * ----------

class GoogleAdsConnection:
    """A GoogleAdsClient and the API version its services use."""

    def __init__(self, client: GoogleAdsClient, version: str | None = None):
        self.client = client
        self.version = version
        self.services: dict[str, Any] = {}

    def service(self, name: str) -> Any:
        if name not in self.services:
            options = {"version": self.version} if self.version else {}
            try:
                self.services[name] = self.client.get_service(name, **options)
            except ValueError as exc:  # get_service rejects an unknown service name
                version_name = self.version or default_version()
                raise ConnectorError("google_ads: %s is not a service in %s%s" % (
                    name, sdk_version(), did_you_mean(name, service_names(version_name))),
                    retryable=False) from exc
        return self.services[name]


def remember_tokens(credentials: Any, remember: Callable[[Any], None]) -> None:
    """The access tokens the client's credentials hold and get when they refresh are masked like secrets."""
    if credentials is None:
        return
    remember(getattr(credentials, "token", None))
    refresh = getattr(credentials, "refresh", None)
    if not callable(refresh):
        return

    def refresh_and_remember(request: Any) -> None:
        refresh(request)
        remember(getattr(credentials, "token", None))
    with contextlib.suppress(AttributeError, TypeError):  # credentials that take no attributes: tokens are not logged
        credentials.refresh = refresh_and_remember


def connect(connection: ResolvedConnection, remember: Callable[[Any], None]) -> GoogleAdsConnection:
    """A client for the resolved connection; `remember` masks the access tokens it obtains."""
    version = connection.get("api_version") or None
    config = {"use_proto_plus": True}
    config.update((key, str(connection[key])) for key in CREDENTIALS)
    if connection.get("login_customer_id") not in (None, ""):
        config["login_customer_id"] = customer_id(connection["login_customer_id"], "login_customer_id")
    try:
        client = GoogleAdsClient.load_from_dict(config, version=version)
    except ValueError as exc:
        raise ConnectorError("google_ads: invalid auth configuration: %s" % exc) from None
    remember_tokens(getattr(client, "credentials", None), remember)
    return GoogleAdsConnection(client, version)


# * -----
# * reads
# * -----

Call = Callable[..., Any]
# the request fields the adapter sets from the operation's declared arguments: a vendor argument may not override them
_ADAPTER_FIELDS: Final = ("customer_id", "query", "page_token")


def _request_fields(vendor: Mapping[str, Any] | None) -> dict[str, Any]:
    """The vendor arguments to add to a search request; the adapter's own fields are rejected, not overridden."""
    extras = dict(vendor or {})
    owned = sorted(key for key in extras if key in _ADAPTER_FIELDS)
    if owned:
        raise ConnectorError("google_ads: %s set by the adapter (from `customer_id`, `query` and its paging); do not "
                             "pass %s as a vendor argument" % (
                                 ", ".join(owned), "it" if len(owned) == 1 else "them"))
    return extras


def _search_stream(service: Any, customer: str, query: str, call: Call,
                   extras: Mapping[str, Any]) -> Iterator[list[dict[str, Any]]]:
    def start() -> tuple[Any, Iterator[Any]]:
        batches = iter(service.search_stream(customer_id=customer, query=query, **extras))
        return next(batches, None), batches
    first, batches = call(start)
    if first is None:
        return
    yield rows(first)
    for batch in batches:  # a failure after the first batch is not retried: its rows were already read
        yield rows(batch)


def _search(service: Any, customer: str, query: str, call: Call,
            extras: Mapping[str, Any]) -> Iterator[list[dict[str, Any]]]:
    request = {"customer_id": customer, "query": query, **extras}
    while True:
        page = call(service.search, request=dict(request))
        yield rows(page)  # the pager's `results` and `next_page_token` are those of the current page
        if not page.next_page_token:
            return
        request["page_token"] = page.next_page_token


def read_pages(connection: GoogleAdsConnection, service: str, method: str, arguments: Mapping[str, Any],
               call: Call, *, vendor: Mapping[str, Any] | None = None) -> Iterator[list[dict[str, Any]]]:
    """The pages of one read; `call` is the caller of the calls that reach the API (rate limit, retries).

    `vendor` are the operation's undeclared arguments. For `search`/`search_stream` they are GoogleAdsService request
    fields (e.g. summary_row_setting, search_settings), merged into the request (the adapter owns `customer_id`,
    `query` and the paging token). For a `get_*`/`list_*` family call they are the method's whole request.
    """
    api = connection.service(service)
    if method in ("search", "search_stream"):
        query = arguments["query"]
        if not query.strip():
            raise ConnectorError("google_ads: `query` is empty")
        extras = _request_fields(vendor)
        pages = _search_stream if method == "search_stream" else _search
        yield from pages(api, customer_id(arguments["customer_id"], "customer_id"), query, call, extras)
        return
    yield from _read_method(api, service, method, dict(vendor or {}), call)


def _resolve_method(api: Any, service: str, method: str) -> Call:
    """The bound read method of a service; an unknown method fails without a retry, naming the SDK and suggestions."""
    bound = getattr(api, method, None)
    if not callable(bound):
        raise ConnectorError("google_ads: %s has no %s method in %s%s" % (
            service, method, sdk_version(), did_you_mean(method, read_method_names(api))), retryable=False)
    return bound


def _read_method(api: Any, service: str, method: str, request: dict[str, Any],
                 call: Call) -> Iterator[list[dict[str, Any]]]:
    """A `get_*`/`list_*` read: its request fields are `request` (vendor arguments); one page of rows."""
    bound = _resolve_method(api, service, method)
    response = call((lambda: bound(request=request)) if request else bound)
    yield _response_rows(response)


def _response_rows(response: Any) -> list[dict[str, Any]]:
    """The rows of a `get_*`/`list_*` response: resource-name lists, a pager of messages, or one message."""
    names = getattr(response, "resource_names", None)
    if names is not None and not isinstance(names, str) and hasattr(names, "__iter__"):
        return [{"customer_id": str(name).split("/")[-1], "resource_name": str(name)} for name in names]
    if hasattr(response, "__iter__") and not hasattr(response, "_pb") and not hasattr(response, "DESCRIPTOR"):
        return [_message_row(item) for item in response]
    return [_message_row(response)]


def _message_row(message: Any) -> dict[str, Any]:
    pb = getattr(message, "_pb", message)
    return _gaql_names(MessageToDict(pb, preserving_proto_field_name=True), pb.DESCRIPTOR)


# * ------
# * errors
# * ------

def _error_code(error: Any) -> str:
    """The name of a GoogleAdsError's code, e.g. RESOURCE_EXHAUSTED (from `quota_error`)."""
    code = error.error_code
    kind = type(code).pb(code).WhichOneof("error_code")
    if not kind:
        return "UNKNOWN"
    value = getattr(code, kind)
    return getattr(value, "name", str(value))


def _call_details(call: Any) -> tuple[Any, str | None]:
    """(GoogleAdsFailure or None, request ID or None) from a failed gRPC call's trailing metadata."""
    failure = request_id = None
    metadata = call.trailing_metadata() if callable(getattr(call, "trailing_metadata", None)) else None
    for key, value in metadata or ():
        if key.endswith("googleadsfailure-bin"):  # google.ads.googleads.<version>.errors.googleadsfailure-bin
            parts = key.split(".")
            try:
                module = importlib.import_module("google.ads.googleads.%s.errors.types.errors" % parts[3])
                failure = module.GoogleAdsFailure.deserialize(value)
            except Exception:  # an unknown version or a malformed value: the status is still reported
                failure = None
        elif key == "request-id":
            request_id = value
    return failure, request_id


def _connector_error(status: str | None, failure: Any, request_id: str | None, message: str | None) -> ConnectorError:
    """A ConnectorError from a gRPC status and the Google Ads failure that came with it."""
    codes, details, retry_after = [], [], None
    for error in (failure.errors if failure is not None else ()):
        code = _error_code(error)
        codes.append(code)
        details.append("%s: %s" % (code, error.message))
        delay = error.details.quota_error_details.retry_delay
        seconds = delay.total_seconds() if hasattr(delay, "total_seconds") else getattr(delay, "seconds", 0)
        if seconds:
            retry_after = max(retry_after or 0, seconds)
    # whether it is retried is configuration: the spec's `retry_codes`, or a stream's `retry.codes`, matched against
    # every Google Ads error name and the gRPC status
    text = "; ".join(details) or message or status or "unknown error"
    return ConnectorError("google_ads: %s%s" % (text, " (request id %s)" % request_id if request_id else ""),
                          codes=(*codes, status), retry_after=retry_after)


def translate_error(exc: Exception) -> ConnectorError | None:
    """The ConnectorError of a Google Ads, gRPC or auth library exception; None when it is not an API error."""
    if isinstance(exc, GoogleAdsException):
        status = exc.error.code().name if callable(getattr(exc.error, "code", None)) else None
        return _connector_error(status, exc.failure, exc.request_id, None)
    if isinstance(exc, api_errors.GoogleAPICallError):
        # the client raises these for RESOURCE_EXHAUSTED, INTERNAL, UNAVAILABLE, ...: Google Ads leaves them
        # unconverted, so the failure details are read from the call
        status = exc.grpc_status_code.name if exc.grpc_status_code is not None else None
        failure, request_id = _call_details(exc.response if exc.response is not None else exc.__cause__)
        return _connector_error(status, failure, request_id, "%s: %s" % (status or exc.code, exc.message))
    if isinstance(exc, api_errors.RetryError):
        return ConnectorError("google_ads: %s" % exc, code="RETRY_DEADLINE", retryable=True)
    if isinstance(exc, auth_errors.RefreshError):
        return ConnectorError("google_ads: the OAuth token refresh failed (check client_id, client_secret and "
                              "refresh_token): %s" % exc, code="REFRESH_FAILED",
                              retryable=bool(getattr(exc, "retryable", False)))
    if isinstance(exc, auth_errors.TransportError):
        return ConnectorError("google_ads: cannot reach the OAuth server: %s" % exc, code="TRANSPORT_ERROR",
                              retryable=True)
    if isinstance(exc, grpc.RpcError) and callable(getattr(exc, "code", None)):
        status = exc.code().name
        failure, request_id = _call_details(exc)
        details = exc.details() if callable(getattr(exc, "details", None)) else str(exc)
        return _connector_error(status, failure, request_id, "%s: %s" % (status, details))
    return None
