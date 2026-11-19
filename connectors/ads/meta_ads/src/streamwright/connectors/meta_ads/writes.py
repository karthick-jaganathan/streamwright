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
Apply operations of the meta_ads connector (streamwright.core.sdk.apply): `*.create_*` (create objects on a parent's
edge), `*.api_update` and `*.api_delete` (change or delete objects by id), and the Conversions API
(`AdsPixel.create_event`). Nothing is catalogued: the installed facebook_business decides which objects and methods
exist.

A row's columns (but `op`) are the call's Graph API parameters, over the operation's static `params` (an empty one
is left out, so an optional config input can fill it). Nested values (structs, lists) pass as they are; a text value
holding a JSON object (`'{"em": [...]}'`) is sent as the object; NULLs are not sent. Creates, updates and deletes are
a request per row: a row Meta rejects fails alone, with its Graph API error code; a transient failure cuts the chunk
short (PartialApplyError). Events are a request per chunk.
"""

import datetime
import decimal
import json
import warnings
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Final

from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError

from streamwright.connectors.meta_ads import client
from streamwright.core.sdk import ConnectorError
from streamwright.core.sdk.apply import ACTION_COLUMN, PartialApplyError, RowResult

__all__ = ["EVENTS", "apply_rows"]

EVENTS: Final = ("AdsPixel", "create_event")          # the Conversions API: a chunk of server events per call
_IDENTIFIER: Final = "id"
_ROW_METHODS: Final = ("api_update", "api_delete")      # called on the row's own object
_EVENT_RESULT: Final = ("events_received", "fbtrace_id")
# the SDK's type checker flags `data` (declared list<string>, sent as a list of objects, as its own EventRequest does)
_TYPE_WARNING: Final = r"value of data might not be compatible"


class _InvalidRowError(ValueError):
    pass


def _value(value: Any) -> Any:
    """A column value as the Graph API takes it: JSON-object text parsed, NULLs inside objects dropped."""
    if isinstance(value, str) and value.lstrip().startswith("{"):
        try:
            parsed = json.loads(value)
        except ValueError:
            return value
        return _value(parsed) if isinstance(parsed, dict) else value
    if isinstance(value, Mapping):
        return {key: _value(item) for key, item in value.items() if item is not None}
    if isinstance(value, (list, tuple)):
        return [_value(item) for item in value]
    if isinstance(value, decimal.Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    return value


def _params(row: Mapping[str, Any], static: Mapping[str, Any], skip: tuple[str, ...] = ()) -> dict[str, Any]:
    """The row's columns (but `op`, `skip` and NULLs: the Graph API has no "clear" value) over `static`."""
    params = dict(static)
    for column, value in row.items():
        if column != ACTION_COLUMN and column not in skip and value is not None:
            params[column] = _value(value)
    return params


def _rejection(error: ConnectorError) -> RowResult:
    """A row Meta rejected: its Graph API error code and message."""
    cause = error.__cause__
    if not isinstance(cause, FacebookRequestError):
        return RowResult.failed(None if error.code is None else str(error.code), str(error))
    code = cause.api_error_code()
    return RowResult.failed(None if code is None else str(code), client.api_message(cause))


def _each(rows: Sequence[Mapping[str, Any]], send: Callable[[Mapping[str, Any]], Any],
          retryable: Callable[[ConnectorError], bool]) -> list[RowResult]:
    """A request per row: a rejected or invalid row fails alone; a transient failure cuts the chunk short."""
    results: list[RowResult] = []
    for row in rows:
        try:
            results.append(RowResult.applied(client.export(send(row))))
        except _InvalidRowError as exc:
            results.append(RowResult.failed("INVALID_ROW", str(exc)))
        except ConnectorError as exc:
            if exc.retryable or retryable(exc):
                raise PartialApplyError("%s (after %d of %d row(s))" % (exc, len(results), len(rows)), results,
                                        code=exc.code) from exc
            results.append(_rejection(exc))
        except Exception as exc:  # the row's request may have been sent: it and the rows after it stay in flight
            raise PartialApplyError("meta_ads: %s: %s (after %d of %d row(s))" % (
                type(exc).__name__, exc, len(results), len(rows)), results) from exc
    return results


def _send_events(bound: Callable[..., Any], static: Mapping[str, Any], rows: Sequence[Mapping[str, Any]],
                 call: Callable[..., Any]) -> list[RowResult]:
    """One request for the chunk's events; every row has the call's outcome (a failed call is a ConnectorError)."""
    params = dict(static, data=[_params(row, {}) for row in rows])

    def send() -> Any:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=_TYPE_WARNING)
            return bound(params=params)
    response = client.export(call(send))
    result = {key: response.get(key) for key in _EVENT_RESULT} if isinstance(response, Mapping) else None
    return [RowResult.applied(result) for _ in rows]


def apply_rows(api: FacebookAdsApi, service: str, method: str, *, arguments: Mapping[str, Any],
               rows: Sequence[Mapping[str, Any]], call: Callable[..., Any],
               retryable: Callable[[ConnectorError], bool], vendor: Mapping[str, Any] | None = None) -> list[RowResult]:
    """
    One chunk of rows applied through `service.method`; a RowResult per row, in order. `vendor` (the operation's
    undeclared arguments) are Graph API parameters too, under an explicit `params` entry; `retryable` is the context's
    judgement of an error (its retry codes).
    """
    static = {key: value for key, value in _value({**(vendor or {}), **(arguments.get("params") or {})}).items()
              if value != ""}                                 # an empty (or NULL) parameter is left out
    if method in _ROW_METHODS:
        client.bound_method(api, service, "0", method)  # an unknown object or method fails before a row is sent

        def send_to_object(row: Mapping[str, Any]) -> Any:
            identifier = row.get(_IDENTIFIER)
            if identifier is None or str(identifier).strip() == "":
                raise _InvalidRowError("a row of %s.%s needs an id column (the object's id)" % (service, method))
            bound = client.bound_method(api, service, identifier, method)
            params = _params(row, static, skip=(_IDENTIFIER,))
            return call(lambda: bound(params=params))
        return _each(rows, send_to_object, retryable)
    bound = client.bound_method(api, service, arguments["id"], method)
    if (service, method) == EVENTS:
        return _send_events(bound, static, rows, call)

    def send_to_edge(row: Mapping[str, Any]) -> Any:
        params = _params(row, static)
        return call(lambda: bound(params=params))
    return _each(rows, send_to_edge, retryable)
