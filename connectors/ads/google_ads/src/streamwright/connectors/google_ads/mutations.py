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
Apply operations of the google_ads connector (streamwright.core.sdk.apply): `*Service.mutate_*` (create, update and
remove rows of any resource) and the conversion uploads. Nothing is catalogued: the installed library's request type
of a method names the repeated field the rows go in and its element type (an `<Resource>Operation` or an upload record
such as ClickConversion).

A row's columns are the element's field paths: `name`, `network_settings.target_google_search`, or with the resource
prefix GAQL uses (`campaign.name`). For an operation, `op` is create (the default), update (its update_mask is the row's
field columns but `resource_name`; a None value clears the field) or remove (only `resource_name` is read). Partial
failures are per-row results; `validate_only: true` checks every row and changes nothing.
"""

import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Final

from google.protobuf import json_format

from streamwright.connectors.google_ads import client
from streamwright.core.sdk import ConnectorError
from streamwright.core.sdk.apply import ACTION_COLUMN, RowResult

__all__ = ["OPERATION_ACTIONS", "UPLOAD_ACTIONS", "apply_rows"]

OPERATION_ACTIONS: Final = ("create", "update", "remove")
UPLOAD_ACTIONS: Final = ("upload",)
_IDENTIFIER: Final = "resource_name"
# the request fields the adapter sets: a vendor argument may not override them
_OWNED: Final = ("customer_id", "partial_failure", "validate_only")


def _camel(snake: str) -> str:
    return "".join(part.capitalize() for part in snake.split("_"))


def _snake(camel: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", camel).lower()


def _request_type(connection: client.GoogleAdsConnection, service: str, method: str) -> tuple[Any, str]:
    """(an empty request message of `service.method`, the repeated field its rows go in)."""
    try:
        request = connection.client.get_type(_camel(method) + "Request",
                                             **({"version": connection.version} if connection.version else {}))
    except (ValueError, AttributeError) as exc:
        raise ConnectorError("google_ads: %s.%s has no request type in %s" % (service, method, client.sdk_version()),
                             retryable=False) from exc
    repeated = [field for field in request._pb.DESCRIPTOR.fields if field.message_type is not None and (
        field.is_repeated if hasattr(field, "is_repeated") else field.label == field.LABEL_REPEATED)]
    if len(repeated) != 1:
        raise ConnectorError("google_ads: %s.%s does not take a list of rows (apply operations take a mutate_* or "
                             "upload method with one repeated field)" % (service, method), retryable=False)
    return request, repeated[0].name


def _nested(row: Mapping[str, Any], prefix: str) -> tuple[dict[str, Any], list[str]]:
    """The row's field columns as a nested message dict, and their field paths (the resource prefix removed)."""
    message: dict[str, Any] = {}
    paths = []
    for column, value in row.items():
        if column == ACTION_COLUMN:
            continue
        path = column[len(prefix):] if prefix and column.startswith(prefix) else column
        paths.append(path)
        if value is None:
            continue                                          # in an update mask without a value: cleared
        parts = path.split(".")
        target = message
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = value
    return message, paths


def _element(connection: client.GoogleAdsConnection, type_name: str, row: Mapping[str, Any]) -> Any:
    """The request element (an operation or an upload record) of one row; ValueError when the row does not fit."""
    element = connection.client.get_type(type_name, **({"version": connection.version} if connection.version
                                                       else {}))
    fields = element._pb.DESCRIPTOR.fields_by_name
    if "create" not in fields:                                # an upload record: the row is the record
        json_format.ParseDict(_nested(row, "")[0], element._pb)
        return element
    action = row.get(ACTION_COLUMN, "create")
    resource = fields["create"].message_type.name
    message, paths = _nested(row, _snake(resource) + ".")
    if action == "remove":
        if not message.get(_IDENTIFIER):
            raise ValueError("a remove row needs resource_name")
        element.remove = message[_IDENTIFIER]
        return element
    if action == "update":
        if not message.get(_IDENTIFIER):
            raise ValueError("an update row needs resource_name")
        element.update_mask.paths.extend(path for path in paths if path != _IDENTIFIER)
    json_format.ParseDict(message, getattr(element, action)._pb)
    return element


def _failures(connection: client.GoogleAdsConnection, response: Any) -> dict[int, tuple[str, str]]:
    """{element index: (error code, message)} of a partial failure."""
    status = getattr(response, "partial_failure_error", None)
    if status is None or not getattr(status, "details", None):
        return {}
    failure_type = type(connection.client.get_type("GoogleAdsFailure", **(
        {"version": connection.version} if connection.version else {})))
    found = {}
    for detail in status.details:
        for error in failure_type.deserialize(detail.value).errors:
            elements = error.location.field_path_elements
            if elements and elements[0]._pb.HasField("index"):
                found.setdefault(int(elements[0].index), (client.error_code(error), _with_field(error.message,
                                                                                                elements[1:])))
    return found


def _with_field(message: str, elements: Sequence[Any]) -> str:
    """Google's message and the field it is about, within the row's element (`create.manual_cpc`, `gclid`)."""
    raw = [getattr(element, "_pb", element) for element in elements]       # a slice gives the raw messages
    path = ".".join(element.field_name + ("[%d]" % element.index if element.HasField("index") else "")
                    for element in raw)
    return "%s (field: %s)" % (message, path) if path else message


def _result(item: Any) -> dict[str, Any]:
    return json_format.MessageToDict(getattr(item, "_pb", item), preserving_proto_field_name=True)


def apply_rows(connection: client.GoogleAdsConnection, service: str, method: str, *, arguments: Mapping[str, Any],
               rows: Sequence[Mapping[str, Any]], call: Callable[..., Any],
               vendor: Mapping[str, Any] | None = None) -> list[RowResult]:
    """One chunk of rows applied through `service.method`; a RowResult per row, in order."""
    owned = sorted(key for key in (vendor or {}) if key in _OWNED)
    if owned:
        raise ConnectorError("google_ads: %s set by the operation's arguments, not as vendor arguments" %
                             ", ".join(owned), retryable=False)
    request, field = _request_type(connection, service, method)
    type_name = request._pb.DESCRIPTOR.fields_by_name[field].message_type.name
    results: list[RowResult | None] = [None] * len(rows)
    elements, positions = [], []
    for position, row in enumerate(rows):
        try:
            elements.append(_element(connection, type_name, row))
            positions.append(position)
        except (ValueError, json_format.ParseError) as exc:
            results[position] = RowResult.failed("INVALID_ROW", str(exc))
    if elements:
        validate_only = bool(arguments.get("validate_only", False))
        payload = dict(vendor or {}, customer_id=client.customer_id(arguments["customer_id"], "customer_id"),
                       partial_failure=bool(arguments.get("partial_failure", True)), validate_only=validate_only)
        payload[field] = elements
        bound = getattr(connection.service(service), method, None)
        if not callable(bound):
            raise ConnectorError("google_ads: %s has no %s method in %s" % (service, method, client.sdk_version()),
                                 retryable=False)
        response = call(lambda: bound(request=payload))
        failures = _failures(connection, response)
        returned = list(getattr(response, "results", None) or ())
        for index, position in enumerate(positions):
            if index in failures:
                results[position] = RowResult.failed(*failures[index])
            elif validate_only:
                results[position] = RowResult.applied({"validate_only": True})
            else:
                results[position] = RowResult.applied(_result(returned[index]) if index < len(returned) else None)
    return [item for item in results if item is not None]
