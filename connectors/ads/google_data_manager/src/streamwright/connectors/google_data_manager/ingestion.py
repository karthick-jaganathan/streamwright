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
Apply operations of the google_data_manager connector (streamwright.core.sdk.apply): the IngestionService writes.
Nothing is catalogued: the installed library's request type of a method names the list the rows go in (`events`,
`audience_members`, ...) and its element type (Event, AudienceMember, ...).

A chunk of rows is one request. The operation's arguments are the request's fields (destinations, consent, encoding,
validate_only, terms_of_service; for the families, vendor arguments), and each row is one element of its list, whose
columns are the element's field paths: `transaction_id`, `ad_identifiers.gclid`, or a mapping or list (a SQL STRUCT,
LIST or JSON text) for a nested message such as `user_data`. Timestamps may be SQL timestamps or RFC 3339 text.

Google's fast-fail model: a request with an invalid record fails whole and nothing in it is processed; its BadRequest
names each invalid record (`events.events[3].user_data.user_identifiers`, reason INVALID_HEX_ENCODING). Those rows
fail - the reason their error code, the description and field their message - and the request is sent again without
them, as Google asks (fix and resend). An accepted request is processed later: its rows are applied with the request's
`request_id` and their `record_index` in it (IngestionService.retrieve_request_status reports the records that fail
in processing by that index), and the optional fields Google dropped as `warnings` ({field, reason, description}).

Destinations follow Google's request model: a request lists its destinations (at most 10, each with a unique
`reference`) and an event names the ones it goes to in `destination_references` (none: all of them). Known up front,
the destinations are the output's `destinations` argument and a row may set `destination_references` itself. Looked
up (a conversion action's id found by its name), a row carries its own: its `destination` column (a Destination, or a
list of them) - the request's destinations are then the chunk's rows' (each given a reference: its own, or a new one)
and each event's destination_references name its own. A chunk needs more than one request only past 10 destinations;
then a request that fails fails its own rows only.
"""

import datetime as dt
import json
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Final

from google.ads import datamanager_v1 as library
from google.protobuf import json_format

from streamwright.connectors.google_data_manager import client
from streamwright.connectors.google_data_manager.surface import sdk_version
from streamwright.core.sdk import ConnectorError
from streamwright.core.sdk.apply import ACTION_COLUMN, RowResult
from streamwright.core.sdk.contract import Operation

__all__ = ["DESTINATION_COLUMN", "apply_rows"]

DESTINATION_COLUMN: Final = "destination"          # a row's own Destination(s) (not a field of the element)
MAX_DESTINATIONS: Final = 10                       # the destinations of one request (the API's request limit)

_TIMESTAMP: Final = "google.protobuf.Timestamp"
_OFFSET_HOURS: Final = re.compile(r"[+-][0-9]{2}$")          # DuckDB writes a whole-hour offset as +00


def _camel(snake: str) -> str:
    return "".join(part.capitalize() for part in snake.split("_"))


def _lower_camel(snake: str) -> str:
    text = _camel(snake)
    return text[:1].lower() + text[1:]


def _request_type(service: str, method: str) -> tuple[Any, str]:
    """(the request message class of `service.method`, the list its rows go in: its repeated non-Destination field)."""
    request = getattr(library, _camel(method) + "Request", None)
    if request is None or not hasattr(request, "pb"):
        raise ConnectorError("google_data_manager: %s.%s has no request type in %s" % (service, method, sdk_version()),
                             retryable=False)
    destination = library.Destination.pb().DESCRIPTOR
    lists = [field.name for field in request.pb().DESCRIPTOR.fields
             if field.message_type is not None and client.repeated(field) and field.message_type != destination]
    if len(lists) != 1:
        raise ConnectorError("google_data_manager: %s.%s does not take a list of rows (apply operations take an "
                             "ingest or remove method with one list of records)" % (service, method), retryable=False)
    return request, lists[0]


# * ----
# * rows
# * ----

def _timestamp(value: Any) -> Any:
    """RFC 3339 text from a SQL timestamp: a datetime, or text such as `2026-11-20 04:30:00+00` (UTC when naive)."""
    if isinstance(value, dt.datetime):
        return (value if value.tzinfo else value.replace(tzinfo=dt.UTC)).isoformat()
    if not isinstance(value, str):
        return value
    text = value.strip()
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    if _OFFSET_HOURS.search(text):
        text += ":00"
    elif "T" in text and not (text.endswith("Z") or re.search(r"[+-][0-9]{2}:[0-9]{2}$", text)):
        text += "Z"
    return text


def _decoded(value: Any, path: str) -> Any:
    """JSON text (a SQL JSON value) given for a message or a list, decoded."""
    if not (isinstance(value, str) and value.lstrip()[:1] in ("{", "[")):
        return value
    try:
        return json.loads(value)
    except ValueError:
        raise ValueError("%s is not valid JSON" % path) from None


def _subfield(message: Any, key: str) -> Any:
    return message.fields_by_name.get(key) or message.fields_by_camelcase_name.get(key)


def _fit(value: Any, field: Any, path: str) -> Any:
    """A column's value made fit for json_format: nested JSON text decoded, SQL timestamps written as RFC 3339."""
    if value is None or field is None or field.message_type is None:
        return value
    value = _decoded(value, path)
    if client.repeated(field) and isinstance(value, list):
        return [_fit_message(item, field.message_type, "%s[%d]" % (path, index)) for index, item in enumerate(value)]
    return _fit_message(value, field.message_type, path)


def _fit_message(value: Any, message: Any, path: str) -> Any:
    if message.full_name == _TIMESTAMP:
        return _timestamp(value)
    value = _decoded(value, path)
    if not isinstance(value, Mapping):
        return value
    return {key: _fit(item, _subfield(message, key), "%s.%s" % (path, key)) for key, item in value.items()
            if item is not None}


def _nested(row: Mapping[str, Any]) -> dict[str, Any]:
    """The row's field columns as a nested message dict (`ad_identifiers.gclid` -> {ad_identifiers: {gclid}})."""
    message: dict[str, Any] = {}
    for column, value in row.items():
        if column == ACTION_COLUMN or value is None:
            continue
        *parents, last = column.split(".")
        target = message
        for part in parents:
            target = target.setdefault(part, {})
            if not isinstance(target, dict):
                raise ValueError("column %s and column %s both set %s" % (column, part, part))
        target[last] = value
    return message


def _element(container: Any, field: str, values: Mapping[str, Any]) -> Any:
    """A row's nested values as an element of the request's list (a protobuf message); ValueError when they do not
    fit."""
    element = getattr(container, field).add()
    message = element.DESCRIPTOR
    fitted = {key: _fit(value, _subfield(message, key), key) for key, value in values.items()}
    json_format.ParseDict(fitted, element)
    return element


def _row_destinations(values: dict[str, Any]) -> list[Any]:
    """The row's own Destinations (its `destination` column - one or a list - taken out of `values`); ValueError when
    they do not fit."""
    value = _decoded(values.pop(DESTINATION_COLUMN, None), DESTINATION_COLUMN)
    if value is None:
        return []
    found = []
    for item in value if isinstance(value, list) else [value]:
        destination = library.Destination.pb()()
        fitted = _fit_message(item, destination.DESCRIPTOR, DESTINATION_COLUMN)
        if not isinstance(fitted, Mapping):
            raise ValueError("%s is not a Destination object or a list of them" % DESTINATION_COLUMN)
        try:
            json_format.ParseDict(fitted, destination)
        except json_format.ParseError as exc:
            raise ValueError("%s: %s" % (DESTINATION_COLUMN, exc)) from None
        found.append(destination)
    return found


# * --------
# * requests
# * --------

def _enum_name(value: Any) -> str:
    return getattr(value, "name", "") if value else ""


def _base_request(request_class: Any, operation: Operation, arguments: Mapping[str, Any],
                  vendor: Mapping[str, Any], field: str) -> Any:
    """The request without its rows: the operation's declared arguments (or their defaults) and vendor arguments."""
    if field in vendor or _lower_camel(field) in vendor:
        raise ConnectorError("google_data_manager: %s are the output's rows, not an argument" % field, retryable=False)
    fields = {declared.name: arguments.get(declared.name, declared.default) for declared in operation.arguments}
    fields = {key: value for key, value in dict(fields, **vendor).items() if value is not None}
    request = request_class.pb()()
    try:
        json_format.ParseDict(_fit_message(fields, request.DESCRIPTOR, "arguments"), request)
    except (ValueError, json_format.ParseError) as exc:
        raise ConnectorError("google_data_manager: the %s arguments do not fit %s: %s" % (
            operation.name, request.DESCRIPTOR.name, exc), retryable=False) from None
    return request


def _by_record(items: Sequence[tuple[str, str, str]], field: str, count: int, *,
               strict: bool = True) -> dict[int, list[dict[str, str]]] | None:
    """
    {record index: [{field (its path in the record), reason, description}]} of (path, reason, description) items whose
    path names a record of the list: `events.events[3].user_data` (Google's form) or `events[3].user_data`. One that
    does not (a request-level problem, e.g. a destination) makes it None, or with `strict` False is left out.
    """
    names = "|".join(re.escape(name) for name in (field, _lower_camel(field)))
    pattern = re.compile(r"^(?:(?:%s)\.)?(?:%s)\[([0-9]+)\]\.?(.*)$" % (names, names))
    found: dict[int, list[dict[str, str]]] = {}
    for path, reason, description in items:
        match = pattern.match(path or "")
        if match is None or int(match.group(1)) >= count:
            if strict:
                return None
            continue
        found.setdefault(int(match.group(1)), []).append(
            {key: value for key, value in (("field", match.group(2)), ("reason", reason),
                                           ("description", description)) if value})
    return found


def _rejection(problems: Sequence[Mapping[str, str]]) -> RowResult:
    """A rejected record's result: its first reason as the code (e.g. INVALID_HEX_ENCODING), every problem said."""
    texts = ["%s (field: %s)" % (item.get("description") or item.get("reason", ""), item["field"]) if "field" in item
             else item.get("description") or item.get("reason", "") for item in problems]
    return RowResult.failed(next((item["reason"] for item in problems if item.get("reason")), "INVALID_ARGUMENT"),
                            "; ".join(texts))


def _send(bound: Callable[..., Any], request_class: Any, template: Any, *, field: str,
          pending: list[tuple[int, Any]], results: list[RowResult | None], call: Callable[..., Any]) -> None:
    """
    The rows `pending` ((position, element)) sent in one request made from `template`, their results set by position;
    rows Google rejects by index fail and the rest are sent again. Raises the ConnectorError of a failed request.
    """
    while pending:
        request = type(template)()
        request.CopyFrom(template)
        getattr(request, field).extend(element for _, element in pending)
        try:
            response = call(lambda sent=request: bound(request=request_class.wrap(sent)))
        except client.RequestError as exc:
            rejected = _by_record(exc.violations, field, len(pending)) if exc.violations else None
            if not rejected:
                raise
            for index, problems in rejected.items():
                results[pending[index][0]] = _rejection(problems)
            pending = [item for index, item in enumerate(pending) if index not in rejected]
            continue
        # responses differ: remove_* and the user responses have no field_warnings, ingest_ad_events not even an id
        warnings = _by_record([(item.field, _enum_name(item.reason), item.description)
                               for item in getattr(response, "field_warnings", ())], field, len(pending),
                              strict=False) or {}
        request_id = getattr(response, "request_id", "")
        for index, (position, _) in enumerate(pending):
            result: dict[str, Any] = {"request_id": request_id, "record_index": index} if request_id else {}
            if getattr(template, "validate_only", False):
                result["validate_only"] = True
            if index in warnings:
                result["warnings"] = warnings[index]
            results[position] = RowResult.applied(result)
        return


class _Destinations:
    """The distinct destinations of a chunk, each with a unique reference: its own, or `destination_<n>`."""

    def __init__(self):
        self.references: dict[bytes, str] = {}          # a destination (serialized, reference left out) -> reference
        self.destinations: dict[str, Any] = {}          # reference -> the destination, with it

    def reference(self, destination: Any) -> str:
        """The reference of a destination; ValueError when its own reference names another destination."""
        bare = type(destination)()
        bare.CopyFrom(destination)
        bare.reference = ""
        key = bare.SerializeToString(deterministic=True)
        if key in self.references:
            return self.references[key]
        reference = destination.reference
        if reference in self.destinations:
            raise ValueError("%s: reference %r names two different destinations" % (DESTINATION_COLUMN, reference))
        if not reference:
            reference = next("destination_%d" % number for number in range(1, len(self.destinations) + 2)
                             if "destination_%d" % number not in self.destinations)
        bare.reference = reference
        self.references[key], self.destinations[reference] = reference, bare
        return reference


def _rows(base: Any, field: str, rows: Sequence[Mapping[str, Any]], scratch: Any, results: list[RowResult | None],
          *, deduplicated: bool = False) -> tuple[list[tuple[int, Any, list[Any]]], bool]:
    """
    ([(position, element, the row's own destinations)], whether any row has its own). A row that does not fit, has no
    destination at all, references a destination the output does not have, or (`deduplicated`: Google deduplicates
    the events by it, which makes a resend safe) has no transaction_id, fails here (INVALID_ROW).
    """
    routed = "destinations" in base.DESCRIPTOR.fields_by_name
    known = {item.reference for item in base.destinations} if routed else set()
    found, own = [], False
    for position, row in enumerate(rows):
        try:
            values = _nested(row)
            destinations = _row_destinations(values) if routed else []
            if routed and not destinations and not base.destinations:
                raise ValueError("no destination: give the output's `destinations` argument or the row's `%s`" %
                                 DESTINATION_COLUMN)
            element = _element(scratch, field, values)
            if destinations and element.destination_references:
                raise ValueError("give the row's `%s` or its destination_references, not both" % DESTINATION_COLUMN)
            unknown = [item for item in element.destination_references if item not in known] if routed else []
            if unknown:
                raise ValueError("destination_references %s: not the reference of any of the output's destinations"
                                 " (%s)" % (", ".join(unknown), ", ".join(sorted(known)) or "none"))
            if deduplicated and not element.transaction_id:
                raise ValueError("no transaction_id: Google deduplicates events by it, so a call cut short can be "
                                 "sent again")
        except (ValueError, json_format.ParseError) as exc:
            results[position] = RowResult.failed("INVALID_ROW", str(exc))
            continue
        found.append((position, element, destinations))
        own = own or bool(destinations)
    return found, own


def _requests(base: Any, field: str, rows: Sequence[Mapping[str, Any]], scratch: Any,
              results: list[RowResult | None], *,
              deduplicated: bool = False) -> list[tuple[list[Any] | None, list[tuple[int, Any]]]]:
    """
    The chunk's requests: [(their destinations, or None for the output's; [(position, element)])]. When rows carry
    their own destinations, every event names its destinations (a row without: the output's) and a request holds at
    most MAX_DESTINATIONS of them (a row with more fails: INVALID_ROW).
    """
    found, own = _rows(base, field, rows, scratch, results, deduplicated=deduplicated)
    if not own:
        return [(None, [(position, element) for position, element, _ in found])] if found else []
    requests: list[tuple[_Destinations, list[tuple[int, Any]]]] = [(_Destinations(), [])]
    for position, element, destinations in found:
        if not destinations and element.destination_references:      # the output's, by their own references
            wanted = [item for item in base.destinations if item.reference in element.destination_references]
        else:
            wanted = destinations or list(base.destinations)
        if len(wanted) > MAX_DESTINATIONS:
            results[position] = RowResult.failed("INVALID_ROW", "%d destinations: a request takes at most %d" % (
                len(wanted), MAX_DESTINATIONS))
            continue
        current, pending = requests[-1]
        if pending and len(current.destinations) + len(wanted) > MAX_DESTINATIONS:
            requests.append((_Destinations(), []))
            current, pending = requests[-1]
        try:
            references = [current.reference(item) for item in wanted]
        except ValueError as exc:
            results[position] = RowResult.failed("INVALID_ROW", str(exc))
            continue
        del element.destination_references[:]
        element.destination_references.extend(references)
        pending.append((position, element))
    return [(list(current.destinations.values()), pending) for current, pending in requests if pending]


def apply_rows(connection: client.DataManagerConnection, operation: Operation, *, arguments: Mapping[str, Any],
               rows: Sequence[Mapping[str, Any]], call: Callable[..., Any],
               vendor: Mapping[str, Any] | None = None) -> list[RowResult]:
    """One chunk of rows applied through `operation` (one request, more past 10 destinations); a RowResult per row,
    in order."""
    request_class, field = _request_type(operation.service, operation.method)
    base = _base_request(request_class, operation, arguments, dict(vendor or {}), field)
    bound = connection.method(operation.service, operation.method)
    results: list[RowResult | None] = [None] * len(rows)
    requests = _requests(base, field, rows, request_class.pb()(), results,
                         deduplicated=operation.method == "ingest_events")
    for destinations, pending in requests:
        template = base
        if destinations is not None:
            template = type(base)()
            template.CopyFrom(base)
            del template.destinations[:]
            template.destinations.extend(destinations)
        try:
            _send(bound, request_class, template, field=field, pending=pending, results=results, call=call)
        except ConnectorError as exc:
            if len(requests) == 1:
                raise                                  # the chunk's one request: the engine retries or holds it
            for position, _ in pending:
                if results[position] is None:
                    results[position] = RowResult.failed(exc.code or "REQUEST_FAILED", str(exc))
    return [item for item in results if item is not None]
