# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the microsoft_ads connector knows about Microsoft Advertising (SOAP API v13), shared by its engines: the OAuth
session and one ServiceClient per service, SOAP request objects built from plain mappings with the WSDL's own
templates, responses as plain data, and the SDK's errors as ConnectorError (code, retryability and the pause
Microsoft asks for).
"""

import contextlib
import datetime
import difflib
import http.client
import socket
import urllib.error
from collections.abc import Callable, Iterator, Mapping
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Final

import requests
from bingads import AuthorizationData, ServiceClient
from bingads.authorization import OAuthDesktopMobileAuthCodeGrant, OAuthWebAuthCodeGrant
from bingads.exceptions import OAuthTokenRequestException
from suds import WebFault

from streamwright.core.sdk import ConnectorError, InputError, as_date
from streamwright.core.sdk.contract import ResolvedConnection

__all__ = ["MicrosoftAdsConnection", "connect", "read_pages", "translate_error", "validate_connection"]

API_VERSION: Final = 13
# how long to pause, not whether to retry (that is the spec's `retry_codes` or a stream's `retry.codes`): Microsoft
# asks callers to wait before retrying CallRateExceeded and ConcurrentRequestOverLimit
WAIT_CODES: Final = (117, 207)
NATIVE_REDIRECT_URI: Final = "https://login.microsoftonline.com/common/oauth2/nativeclient"
CREDENTIALS: Final = ("developer_token", "client_id", "refresh_token")
Call = Callable[..., Any]


def validate_connection(connection: Mapping[str, Any]) -> list[str]:
    """Why a resolved connection cannot be used: an empty credential."""
    return ["%s is empty" % key for key in CREDENTIALS if connection[key] == ""]


# * ----------------------
# * SOAP request building
# * ----------------------

def _is_object(value: Any) -> bool:
    return hasattr(value, "__keylist__")


def _keys(value: Any) -> list[str]:
    return list(value.__keylist__)


def _text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


class _Builder:
    """Builds suds request objects from plain arguments, using the WSDL's templates."""

    def __init__(self, factory: Any, method: str):
        self.factory = factory
        self.method = method

    def create(self, type_name: str, path: str) -> Any:
        try:
            return self.factory.create(type_name)
        except Exception as exc:  # suds.TypeNotFound and friends
            raise ConnectorError("microsoft_ads: %s: unknown type %r (%s)" % (path, type_name, exc)) from None

    def fill(self, target: Any, values: Any, path: str) -> Any:
        if not isinstance(values, dict):
            raise ConnectorError("microsoft_ads: %s expects a mapping of fields, got %r" % (path, values))
        keys = _keys(target)
        templates = {key: getattr(target, key) for key in keys}
        for key in keys:  # only the fields given are sent
            setattr(target, key, None)
        for key, value in values.items():
            if key == "@type":
                continue
            if key not in templates:
                raise ConnectorError("microsoft_ads: %s has no field %r (fields: %s)" % (path, key, ", ".join(keys)))
            setattr(target, key, self.convert(templates[key], value, "%s.%s" % (path, key)))
        return target

    def convert(self, template: Any, value: Any, path: str) -> Any:
        if value is None:
            return None
        if isinstance(value, dict) and "@type" in value:
            return self.fill(self.create(value["@type"], path), value, path)
        if not _is_object(template):  # a simple value; lists are XML list values ("Search Shopping")
            if isinstance(value, (list, tuple)):
                return " ".join(_text(item) for item in value)
            if isinstance(value, dict):
                raise ConnectorError("microsoft_ads: %s expects a value, got a mapping" % path)
            return _text(value) if isinstance(value, (datetime.date, datetime.datetime)) else value
        keys = _keys(template)
        if keys == ["value"]:  # an enumeration
            return " ".join(_text(item) for item in value) if isinstance(value, (list, tuple)) else _text(value)
        if sorted(keys) == ["Day", "Month", "Year"]:
            try:
                day = as_date(value)
            except InputError as exc:
                raise ConnectorError("microsoft_ads: %s: %s" % (path, exc)) from None
            template.Day, template.Month, template.Year = day.day, day.month, day.year
            return template
        if len(keys) == 1 and isinstance(getattr(template, keys[0]), list):  # ArrayOf<Item>
            item_type = keys[0]
            items = value if isinstance(value, (list, tuple)) else [value]
            setattr(template, item_type, [self.item(item_type, item, "%s[%d]" % (path, i))
                                          for i, item in enumerate(items)])
            return template
        return self.fill(template, value, path)

    def item(self, item_type: str, value: Any, path: str) -> Any:
        if isinstance(value, dict):
            return self.fill(self.create(value.get("@type", item_type), path), value, path)
        return _text(value) if isinstance(value, (bool, datetime.date)) else value

    def arguments(self, values: Mapping[str, Any]) -> dict[str, Any]:
        wrapper = self.fill(self.create("%sRequest" % self.method, self.method), dict(values), self.method)
        return {key: getattr(wrapper, key) for key in _keys(wrapper) if getattr(wrapper, key) is not None}


def _plain(value: Any) -> Any:
    """A suds response as plain data."""
    if _is_object(value):
        return {key: _plain(getattr(value, key)) for key in _keys(value)}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    if isinstance(value, str):
        return str(value)  # suds Text
    return value


def _account_id(request: Mapping[str, Any]) -> Any:
    """The account a request is about: its AccountId, or the only account of a report scope."""
    if isinstance(request.get("AccountId"), (str, int)) and not isinstance(request.get("AccountId"), bool):
        return request["AccountId"]
    for value in request.values():
        scope = value.get("Scope") if isinstance(value, dict) else None
        ids = scope.get("AccountIds") if isinstance(scope, dict) else None
        if isinstance(ids, list) and len(ids) == 1:
            return ids[0]
    return None


# * ----------
# * connection
# * ----------

class MicrosoftAdsConnection:
    """The OAuth session plus one bingads ServiceClient per service."""

    def __init__(self, authentication: Any, developer_token: str, customer_id: Any = None, account_id: Any = None,
                 environment: str = "production"):
        self.authentication = authentication
        self.developer_token = developer_token
        self.customer_id = customer_id
        self.account_id = account_id
        self.environment = environment
        self.services: dict[str, Any] = {}

    def service(self, name: str, account_id: Any = None, customer_id: Any = None) -> Any:
        if name not in self.services:
            data = AuthorizationData(account_id=None, customer_id=self.customer_id,
                                     developer_token=self.developer_token, authentication=self.authentication)
            self.services[name] = ServiceClient(name, API_VERSION, authorization_data=data,
                                                environment=self.environment)
        client = self.services[name]
        # the SDK builds the SOAP headers from this when an operation is looked up
        client.authorization_data.account_id = account_id if account_id is not None else self.account_id
        client.authorization_data.customer_id = customer_id if customer_id is not None else self.customer_id
        return client


def connect(connection: ResolvedConnection, remember: Callable[[Any], None]) -> MicrosoftAdsConnection:
    """The OAuth session of the connection (refreshing its token now); `remember` masks the tokens it obtains."""
    options = {"env": connection["environment"], "tenant": str(connection.get("tenant") or "common")}
    if connection.get("client_secret"):
        oauth = OAuthWebAuthCodeGrant(str(connection["client_id"]), str(connection["client_secret"]),
                                      NATIVE_REDIRECT_URI, **options)
    else:
        oauth = OAuthDesktopMobileAuthCodeGrant(client_id=str(connection["client_id"]), **options)

    def remember_tokens(tokens: Any) -> None:  # refreshed tokens are secrets too
        remember(tokens.access_token)
        remember(tokens.refresh_token)
    oauth.token_refreshed_callback = remember_tokens
    oauth.request_oauth_tokens_by_refresh_token(str(connection["refresh_token"]))
    return MicrosoftAdsConnection(oauth, str(connection["developer_token"]), customer_id=connection.get("customer_id"),
                                  account_id=connection.get("account_id"), environment=connection["environment"])


def _sdk_version() -> str:
    try:
        return "bingads %s" % version("bingads")
    except PackageNotFoundError:
        return "the installed bingads"


def _operations(client: Any) -> list[str]:
    """The SOAP operations the service's WSDL declares (bingads bundles the WSDL, so this needs no network)."""
    names: set[str] = set()
    for service in client.soap_client.wsdl.services:
        for port in service.ports:
            names.update(port.methods)
    return sorted(names)


def _resolve_method(client: Any, service: str, method: str) -> Any:
    """
    The bound SOAP operation `method` of `service`; an operation the WSDL does not declare fails without a retry,
    naming the bingads version and the closest operations (contract v1.3 §6b: the WSDL is the authority).
    """
    operations = _operations(client)
    if method not in operations:
        raise ConnectorError("microsoft_ads: %s has no %s operation in %s%s" % (
            service, method, _sdk_version(), _did_you_mean(method, operations)), retryable=False)
    return getattr(client, method)


def _did_you_mean(value: str, candidates: list[str]) -> str:
    matches = difflib.get_close_matches(value, candidates, n=3)
    return "; did you mean %s?" % ", ".join(matches) if matches else ""


def read_pages(connection: MicrosoftAdsConnection, service: str, method: str, arguments: Mapping[str, Any],
               call: Call, *, vendor: Mapping[str, Any] | None = None) -> Iterator[list[Any]]:
    """The response of one call as a page: `request` holds the SOAP request's fields.

    `vendor` are the operation's undeclared arguments: more SOAP request fields, merged into `request` (an explicit
    `request` entry wins). A field the request type does not have fails with a clear error naming it.
    """
    request = {**(vendor or {}), **(arguments.get("request") or {})}
    account_id = arguments.get("account_id")
    client = connection.service(service, account_id if account_id not in (None, "") else _account_id(request),
                                arguments.get("customer_id") or None)
    operation = _resolve_method(client, service, method)
    values = _Builder(client.factory, method).arguments(request)
    yield _plain(call(lambda: operation(**values)))


# * ------
# * errors
# * ------

def _fault_errors(node: Any, found: list[tuple[Any, Any, Any]]) -> list[tuple[Any, Any, Any]]:
    """(code, error_code, message) of every error object in a SOAP fault detail."""
    if _is_object(node):
        keys = _keys(node)
        if "Code" in keys and "Message" in keys:
            found.append((getattr(node, "Code", None), getattr(node, "ErrorCode", None),
                          getattr(node, "Message", None)))
        for key in keys:
            _fault_errors(getattr(node, key), found)
    elif isinstance(node, (list, tuple)):
        for item in node:
            _fault_errors(item, found)
    return found


def _tracking_id(node: Any) -> str | None:
    if _is_object(node):
        if "TrackingId" in _keys(node) and getattr(node, "TrackingId", None):
            return str(node.TrackingId)
        for key in _keys(node):
            found = _tracking_id(getattr(node, key))
            if found:
                return found
    return None


def _http_status(exc: Exception) -> tuple[int, str] | None:
    """suds reports an HTTP error that is not a SOAP fault as Exception((status, reason))."""
    if type(exc) is Exception and len(exc.args) == 1 and isinstance(exc.args[0], tuple) and \
            len(exc.args[0]) == 2 and isinstance(exc.args[0][0], int):
        return exc.args[0]
    return None


def _fault_error(exc: WebFault) -> ConnectorError:
    fault = getattr(exc, "fault", None)
    detail = getattr(fault, "detail", None)
    errors = _fault_errors(detail, [])
    codes = []
    for code, _, _ in errors:
        with contextlib.suppress(TypeError, ValueError):
            codes.append(int(code))
    text = "; ".join("%s %s: %s" % (code, name or "", message) for code, name, message in errors) or \
        str(getattr(fault, "faultstring", exc))
    tracking = _tracking_id(detail)
    return ConnectorError("microsoft_ads: %s%s" % (text, " (tracking id %s)" % tracking if tracking else ""),
                          codes=codes, retry_after=60 if any(code in WAIT_CODES for code in codes) else None)


def translate_error(exc: Exception) -> ConnectorError | None:
    """The ConnectorError of a SOAP fault, OAuth, HTTP or connection exception; None when it is not an API error."""
    if isinstance(exc, WebFault):
        return _fault_error(exc)
    if isinstance(exc, OAuthTokenRequestException):
        return ConnectorError("microsoft_ads: the OAuth token refresh failed (check client_id, client_secret, "
                              "tenant and refresh_token): %s: %s" % (exc.error_code, exc.error_description),
                              code=exc.error_code)
    reply = _http_status(exc)
    if reply is not None:
        status, reason = reply
        return ConnectorError("microsoft_ads: HTTP %d: %s" % (status, reason), code=status,
                              retryable=status == 429 or status >= 500)
    # connection failures: urllib (the SOAP calls) and requests (OAuth)
    if isinstance(exc, (urllib.error.URLError, http.client.HTTPException, ConnectionError, socket.timeout,
                        TimeoutError, requests.ConnectionError, requests.Timeout)):
        return ConnectorError("microsoft_ads: %s: %s" % (type(exc).__name__, exc), retryable=True)
    return None
