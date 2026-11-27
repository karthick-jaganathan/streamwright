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
What the google_data_manager connector knows about the Data Manager API, shared by its engines: the connection rules
and credentials (an OAuth refresh token or a service account), the library's service clients, responses as plain
dicts, and the library's errors as ConnectorError (with the codes, the fields Google rejected and the wait it asks).
"""

import contextlib
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, Final

import grpc
from google.api_core import exceptions as api_errors
from google.auth import exceptions as auth_errors
from google.oauth2.credentials import Credentials as UserCredentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google.protobuf.json_format import MessageToDict

from streamwright.connectors.google_data_manager.surface import client_class, method_names, sdk_version, service_names
from streamwright.core.sdk import ConnectorError, suggest
from streamwright.core.sdk.contract import ResolvedConnection

__all__ = ["DAILY_QUOTA", "DataManagerConnection", "RequestError", "connect", "message_dict", "read_pages",
           "remember_tokens", "repeated", "response_rows", "translate_error", "validate_connection"]

SCOPE: Final = "https://www.googleapis.com/auth/datamanager"
TOKEN_URI: Final = "https://oauth2.googleapis.com/token"
# the code of a daily quota error: not among the retried codes (the quota resets at midnight Pacific time)
DAILY_QUOTA: Final = "DAILY_QUOTA_EXHAUSTED"
_NAME: Final = "google_data_manager"
# a gRPC response carries a rejected request's field violations in its headers: Google's guide raises the client's
# limit (8-10 KB by default) to 16 MB, so a request with many invalid records still reports every one
MAX_METADATA_BYTES: Final = 16 * 1024 * 1024
Call = Callable[..., Any]


def validate_connection(connection: Mapping[str, Any]) -> list[str]:
    """Why a resolved connection cannot be used: no credentials, both kinds, or a refresh token without its client."""
    refresh, account = connection.get("refresh_token"), connection.get("service_account_json")
    if not refresh and not account:
        return ["one of refresh_token (with client_id and client_secret) or service_account_json is required"]
    if refresh and account:
        return ["refresh_token and service_account_json are two kinds of credentials: give one"]
    if refresh and not (connection.get("client_id") and connection.get("client_secret")):
        return ["refresh_token needs client_id and client_secret"]
    return []


# * -----------
# * credentials
# * -----------

def remember_tokens(credentials: Any, remember: Callable[[Any], None]) -> None:
    """The access tokens the credentials hold and get when they refresh are masked like secrets."""
    remember(getattr(credentials, "token", None))
    refresh = getattr(credentials, "refresh", None)
    if not callable(refresh):
        return

    def refresh_and_remember(request: Any) -> None:
        refresh(request)
        remember(getattr(credentials, "token", None))
    with contextlib.suppress(AttributeError, TypeError):  # credentials that take no attributes: tokens are not logged
        credentials.refresh = refresh_and_remember


def _service_account_info(raw: Any) -> Mapping[str, Any]:
    """The service account key as a mapping: JSON text, a mapping or the path of a JSON file."""
    if isinstance(raw, Mapping):
        return raw
    text = str(raw).strip()
    try:
        if text.startswith("{"):
            return json.loads(text)
        with open(text, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError) as exc:
        raise ConnectorError("%s: service_account_json is neither a service account key nor the path of one (%s)" % (
            _NAME, type(exc).__name__), retryable=False) from None


def credentials(connection: ResolvedConnection, remember: Callable[[Any], None]) -> Any:
    """The connection's credentials, for the datamanager scope; `remember` masks what they hold and obtain."""
    if connection.get("refresh_token"):
        found = UserCredentials(None, refresh_token=str(connection["refresh_token"]), token_uri=TOKEN_URI,
                                client_id=str(connection["client_id"]), client_secret=str(connection["client_secret"]),
                                scopes=[SCOPE])
    else:
        info = _service_account_info(connection["service_account_json"])
        remember(str(info.get("private_key") or "") or None)
        try:
            found = ServiceAccountCredentials.from_service_account_info(info, scopes=[SCOPE])
        except ValueError as exc:
            raise ConnectorError("%s: invalid service account key: %s" % (_NAME, exc), retryable=False) from None
    remember_tokens(found, remember)
    return found


# * ----------
# * connection
# * ----------

class DataManagerConnection:
    """The credentials of a connection and the service clients made with them (one per service, made when used)."""

    def __init__(self, credentials: Any):
        self.credentials = credentials
        self.clients: dict[str, Any] = {}

    def client(self, service: str) -> Any:
        if service not in self.clients:
            found = client_class(service)
            if found is None:
                raise ConnectorError("%s: %s is not a service in %s%s" % (
                    _NAME, service, sdk_version(), suggest(service, service_names(), style="list")), retryable=False)
            transport = found.get_transport_class("grpc")
            channel = transport.create_channel(credentials=self.credentials,
                                               options=[("grpc.max_metadata_size", MAX_METADATA_BYTES)])
            self.clients[service] = found(transport=transport(channel=channel))
        return self.clients[service]

    def method(self, service: str, method: str) -> Call:
        """The bound method of a service; an unknown one fails without a retry, naming the SDK and suggestions."""
        bound = getattr(self.client(service), method, None) if method in method_names(service) else None
        if not callable(bound):
            raise ConnectorError("%s: %s has no %s method in %s%s" % (
                _NAME, service, method, sdk_version(), suggest(method, method_names(service), style="list")),
                retryable=False)
        return bound

    def close(self) -> None:
        for client in self.clients.values():
            with contextlib.suppress(Exception):  # closing is best effort: the channel may already be gone
                client.transport.close()
        self.clients.clear()


def connect(connection: ResolvedConnection, remember: Callable[[Any], None]) -> DataManagerConnection:
    """A connection for the resolved values; `remember` masks the access tokens it obtains."""
    return DataManagerConnection(credentials(connection, remember))


# * -----
# * reads
# * -----

# the request fields the adapter sets: a vendor argument may not override them
_ADAPTER_FIELDS: Final = ("request_id", "page_token")


def repeated(field: Any) -> bool:
    """Whether a protobuf field descriptor is a list (repeated)."""
    return field.is_repeated if hasattr(field, "is_repeated") else field.label == field.LABEL_REPEATED


def message_dict(message: Any) -> dict[str, Any]:
    """A message (proto-plus or protobuf) as a plain dict keyed by the proto field names."""
    return MessageToDict(getattr(message, "_pb", message), preserving_proto_field_name=True)


def response_rows(response: Any) -> list[dict[str, Any]]:
    """
    The rows of a response (a message or a pager's current page): the items of its one repeated message field (user
    lists, request statuses per destination, ...), else the response itself as one row.
    """
    message = getattr(response, "_pb", response)
    lists = [field for field in message.DESCRIPTOR.fields if field.message_type is not None and repeated(field)
             and not field.message_type.GetOptions().map_entry]
    if len(lists) == 1:
        return [message_dict(item) for item in getattr(message, lists[0].name)]
    return [message_dict(message)]


def read_pages(connection: DataManagerConnection, service: str, method: str, arguments: Mapping[str, Any],
               call: Call, *, vendor: Mapping[str, Any] | None = None) -> Iterator[list[dict[str, Any]]]:
    """
    The pages of one read; `call` is the caller of the calls that reach the API (rate limit, retries). `vendor` are
    the operation's undeclared arguments: the method's request fields. A paged method is read page by page, each page
    one call; the rows of a request status carry its request_id.
    """
    owned = sorted(key for key in (vendor or {}) if key in _ADAPTER_FIELDS)
    if owned:
        raise ConnectorError("%s: %s set by the adapter, not as vendor arguments" % (_NAME, ", ".join(owned)),
                             retryable=False)
    bound = connection.method(service, method)
    request = dict(vendor or {})
    request_id = arguments.get("request_id")
    if request_id is not None:
        request["request_id"] = str(request_id)
    while True:
        page = call(lambda fields=dict(request): bound(request=fields))
        rows = response_rows(page)
        yield [dict(row, request_id=request["request_id"]) for row in rows] if request_id is not None else rows
        token = getattr(page, "next_page_token", "")
        if not token:
            return
        request["page_token"] = token


# * ------
# * errors
# * ------

class RequestError(ConnectorError):
    """
    A failed call; `violations` are the BadRequest field violations Google named, (field path, reason, description):
    e.g. ("events.events[3].user_data.user_identifiers", "INVALID_HEX_ENCODING", "Email is not hex encoded.").
    """

    def __init__(self, message: str, *, codes: Sequence[Any] = (), retryable: bool = False,
                 retry_after: float | None = None, violations: Sequence[tuple[str, str, str]] = ()):
        super().__init__(message, codes=tuple(codes), retryable=retryable, retry_after=retry_after)
        self.violations = tuple(violations)


def _violations(details: Sequence[Any]) -> list[tuple[str, str, str]]:
    """(field, reason, description) of a BadRequest detail's violations (a protobuf message over gRPC, a dict over
    REST)."""
    found = []
    for detail in details or ():
        if isinstance(detail, Mapping):
            found.extend((item.get("field", ""), item.get("reason", ""), item.get("description", ""))
                         for item in detail.get("fieldViolations", ()))
        else:
            found.extend((item.field, getattr(item, "reason", ""), item.description)
                         for item in getattr(detail, "field_violations", ()))
    return found


def _support_id(details: Sequence[Any]) -> str | None:
    """The request ID of a RequestInfo detail: what Google's support asks for."""
    for detail in details or ():
        found = detail.get("requestId") if isinstance(detail, Mapping) else getattr(detail, "request_id", None)
        if found:
            return str(found)
    return None


def _retry_after(details: Sequence[Any]) -> float | None:
    """The wait a RetryInfo detail asks, in seconds."""
    for detail in details or ():
        delay = getattr(detail, "retry_delay", None)
        if delay is not None and (delay.seconds or delay.nanos):
            return delay.seconds + delay.nanos / 1e9
    return None


def _call_error(status: str | None, reason: str | None, quota: str, text: str, details: Sequence[Any]) -> RequestError:
    violations = _violations(details)
    if violations:
        shown = "; ".join("%s: %s" % (field, description or reason) for field, reason, description in violations[:5])
        more = len(violations) - 5
        text = "%s (%s%s)" % (text, shown, ", and %d more" % more if more > 0 else "")
    support = _support_id(details)
    if support:
        text += " (request id %s)" % support
    if quota.endswith("PerDayPerProject"):  # waiting minutes does not help: the run fails, a later one resumes
        return RequestError("%s: the daily quota %s is exhausted: %s" % (_NAME, quota, text),
                            codes=(DAILY_QUOTA, quota), violations=violations)
    # whether it is retried is configuration: the spec's `retry_codes`, or a stream's `retry.codes`
    return RequestError("%s: %s" % (_NAME, text), codes=tuple(dict.fromkeys((reason, status))),
                        retry_after=_retry_after(details),
                        violations=violations)


def translate_error(exc: Exception) -> ConnectorError | None:
    """The ConnectorError of a Data Manager, gRPC or auth library exception; None when it is not an API error."""
    if isinstance(exc, api_errors.GoogleAPICallError):
        status = exc.grpc_status_code.name if exc.grpc_status_code is not None else (
            str(exc.code) if exc.code is not None else None)
        quota = str((exc.metadata or {}).get("quota_limit") or "")
        return _call_error(status, exc.reason, quota, "%s: %s" % (status or "error", exc.message), exc.details)
    if isinstance(exc, api_errors.RetryError):
        return ConnectorError("%s: %s" % (_NAME, exc), code="RETRY_DEADLINE", retryable=True)
    if isinstance(exc, auth_errors.RefreshError):
        return ConnectorError("%s: the OAuth token refresh failed (check the credentials, and that they were granted "
                              "the %s scope): %s" % (_NAME, SCOPE, exc), code="REFRESH_FAILED",
                              retryable=bool(getattr(exc, "retryable", False)))
    if isinstance(exc, auth_errors.TransportError):
        return ConnectorError("%s: cannot reach the OAuth server: %s" % (_NAME, exc), code="TRANSPORT_ERROR",
                              retryable=True)
    if isinstance(exc, grpc.RpcError) and callable(getattr(exc, "code", None)):
        status = exc.code().name
        details = exc.details() if callable(getattr(exc, "details", None)) else str(exc)
        return _call_error(status, None, "", "%s: %s" % (status, details), ())
    return None
