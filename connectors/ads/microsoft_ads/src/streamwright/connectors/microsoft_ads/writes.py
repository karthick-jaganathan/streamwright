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
Apply operations of the microsoft_ads connector (streamwright.core.sdk.apply): the `Add*`, `Update*` and `Delete*`
operations of any `*Service` and the conversion uploads (`ApplyOfflineConversions`, ...). Nothing is catalogued: the
installed bingads WSDL's `<Method>Request` type has one list (an `ArrayOf<Item>`) that the rows fill, an item each;
`arguments.request` sets its other fields (AddAdGroups' CampaignId, DeleteCampaigns' AccountId).

A row's columns are the item's fields (`Name`, `Status`), dotted for nested ones (`BiddingScheme.Type`); an `@type`
column (or `BiddingScheme.@type`) names the concrete type of an abstract item or object. A list of plain values
(DeleteCampaigns' CampaignIds, an `ArrayOflong`) takes one column per row, the value. A row that does not fit the item
fails alone (INVALID_ROW). Microsoft's partial errors (`PartialErrors`, `NestedPartialErrors`) fail the rows at their
Index; the others are applied, with the id at their index of the response's ids (`CampaignIds`: {"Id": ...}) if it
returns ids. A NULL field is left out of the item (Microsoft keeps its value).
"""

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Final

from streamwright.connectors.microsoft_ads import client
from streamwright.core.sdk import ConnectorError
from streamwright.core.sdk.apply import ACTION_COLUMN, RowResult

__all__ = ["apply_rows"]

# the response parts that carry the per-item errors, and the endings of the part with the new items' ids
_ERRORS: Final = ("PartialErrors", "NestedPartialErrors")
_IDS: Final = ("Ids", "Identities")
INVALID_ROW: Final = "INVALID_ROW"


def _list_of(value: Any) -> list[Any] | None:
    """The items of an `ArrayOf<Item>` (suds object or plain {Item: [...]}); None when `value` is not one."""
    if client._is_object(value):
        keys = client._keys(value)
        return getattr(value, keys[0]) if len(keys) == 1 and isinstance(getattr(value, keys[0]), list) else None
    if isinstance(value, dict) and len(value) == 1 and isinstance(next(iter(value.values())), list):
        return next(iter(value.values()))
    return value if isinstance(value, list) else None


def _list_field(builder: client._Builder, service: str, method: str) -> tuple[str, str]:
    """(the request's one list field, its item type), e.g. ("CampaignIds", "long") for DeleteCampaigns."""
    template = builder.create("%sRequest" % method, method)
    found = [key for key in client._keys(template) if _list_of(getattr(template, key)) is not None]
    if len(found) != 1:
        raise ConnectorError("microsoft_ads: %s.%s has %s list fields (%s): an apply operation takes a request with "
                             "one list, which its rows fill" % (service, method, len(found), ", ".join(found) or
                                                                "none"), retryable=False)
    return found[0], client._keys(getattr(template, found[0]))[0]


def _is_plain(builder: client._Builder, item_type: str) -> bool:
    """Whether the items are plain values (long, string) or an enumeration rather than objects with fields."""
    try:
        template = builder.factory.create(item_type)
    except Exception:  # suds.TypeNotFound: an XML Schema type (long, string)
        return True
    keys = client._keys(template)
    return bool(keys) and all(getattr(template, key) == key for key in keys)


def _nested(row: Mapping[str, Any]) -> dict[str, Any]:
    """The row's columns as an item mapping: `Budget.Amount` sets Amount of Budget."""
    item: dict[str, Any] = {}
    for column, value in row.items():
        if column == ACTION_COLUMN:
            continue
        *parents, name = column.split(".")
        target = item
        for part in parents:
            target = target.setdefault(part, {})
            if not isinstance(target, dict):
                raise ValueError("column %s: %s is a value, not an object with fields" % (column, part))
        target[name] = value
    return item


def _element(builder: client._Builder, item_type: str, plain: bool, row: Mapping[str, Any], path: str) -> Any:
    """The list item of one row; ConnectorError or ValueError when the row does not fit."""
    if not plain:
        return builder.item(item_type, _nested(row), path)
    columns = [column for column in row if column != ACTION_COLUMN]
    if len(columns) != 1 or row[columns[0]] is None:
        raise ValueError("%s: the items are %s values, so a row is one column with a value (columns: %s)" % (
            path, item_type, ", ".join(columns) or "none"))
    return builder.item(item_type, row[columns[0]], path)


def _describe(errors: Sequence[Mapping[str, Any]]) -> tuple[str | None, str]:
    """(code, message) of an item's errors: the first ErrorCode (else Code), every message with its field."""
    code, messages = None, []
    for error in errors:
        if code is None and (error.get("ErrorCode") or error.get("Code") is not None):
            code = str(error.get("ErrorCode") or error.get("Code"))
        message = error.get("Message") or "; ".join(
            str(nested.get("Message")) for nested in _list_of(error.get("BatchErrors")) or ()
            if isinstance(nested, dict) and nested.get("Message")) or "rejected"
        messages.append("%s (field: %s)" % (message, error["FieldPath"]) if error.get("FieldPath") else message)
    return code, "; ".join(messages)


def _failures(response: Mapping[str, Any]) -> dict[int, tuple[str | None, str]]:
    """{item index: (error code, message)} of the response's partial errors."""
    found: dict[int, list[Mapping[str, Any]]] = {}
    for key in _ERRORS:
        for position, error in enumerate(_list_of(response.get(key)) or ()):
            if isinstance(error, dict) and error.get("Index") is not None:      # a BatchError(Collection)
                found.setdefault(int(error["Index"]), []).append(error)
            else:                                                               # a list of errors per item
                errors = [item for item in _list_of(error) or () if isinstance(item, dict)]
                if errors:
                    found.setdefault(position, []).extend(errors)
    return {index: _describe(errors) for index, errors in found.items()}


def _ids(response: Mapping[str, Any]) -> list[Any]:
    """The new items' ids (or identities), by index: the response part named `*Ids`, if exactly one."""
    parts = [value for key, value in response.items() if key.endswith(_IDS)]
    return list(_list_of(parts[0]) or ()) if len(parts) == 1 else []


def _response(builder: client._Builder, method: str, response: Any) -> dict[str, Any]:
    """The response as {part: plain value}: suds gives a response of one part as that part."""
    parts = client._keys(builder.create("%sResponse" % method, method))
    plain = client._plain(response)
    if len(parts) == 1:
        return {parts[0]: plain}
    return plain if isinstance(plain, dict) else {}


def apply_rows(connection: client.MicrosoftAdsConnection, service: str, method: str, *,
               arguments: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], call: Callable[..., Any],
               vendor: Mapping[str, Any] | None = None) -> list[RowResult]:
    """One chunk of rows applied through `service.method` (one SOAP call); a RowResult per row, in order."""
    request = {**(vendor or {}), **(arguments.get("request") or {})}
    account_id = arguments.get("account_id")
    soap = connection.service(service, account_id if account_id not in (None, "") else client._account_id(request),
                              arguments.get("customer_id") or None)
    operation = client._resolve_method(soap, service, method)
    builder = client._Builder(soap.factory, method)
    field, item_type = _list_field(builder, service, method)
    if field in request:
        raise ConnectorError("microsoft_ads: %s.%s: %s is filled by the rows, not set in `request`" % (
            service, method, field), retryable=False)
    plain = _is_plain(builder, item_type)
    results: list[RowResult | None] = [None] * len(rows)
    elements, positions = [], []
    for position, row in enumerate(rows):
        try:
            elements.append(_element(builder, item_type, plain, row, "%s[%d]" % (field, position)))
            positions.append(position)
        except (ConnectorError, ValueError) as exc:
            results[position] = RowResult.failed(INVALID_ROW, str(exc))
    if elements:
        wrapper = builder.fill(builder.create("%sRequest" % method, method), request, method)
        items = getattr(builder.create("%sRequest" % method, method), field)          # an empty ArrayOf<Item>
        setattr(items, item_type, elements)
        setattr(wrapper, field, items)
        values = {key: getattr(wrapper, key) for key in client._keys(wrapper) if getattr(wrapper, key) is not None}
        response = _response(builder, method, call(lambda: operation(**values)))
        failures, ids = _failures(response), _ids(response)
        for index, position in enumerate(positions):
            if index in failures:
                results[position] = RowResult.failed(*failures[index])
                continue
            found = ids[index] if index < len(ids) else None
            results[position] = RowResult.applied(None if found is None else
                                                  found if isinstance(found, dict) else {"Id": found})
    return [item for item in results if item is not None]
