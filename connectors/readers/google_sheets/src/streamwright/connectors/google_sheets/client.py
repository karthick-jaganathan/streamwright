# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the google_sheets connector knows about the Google Sheets API v4, shared by its engines: the connection rules,
the client (core HTTP; credentials travel in headers, never in a URL), reads of a sheet or A1 range as records with
unique column names, and HTTP errors as ConnectorError.
"""

import json
from collections.abc import Callable, Iterator, Mapping
from typing import Any, Final
from urllib.parse import quote

import google.auth.transport.requests
from google.oauth2.credentials import Credentials as UserCredentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials

from streamwright.core.sdk import ConnectorError
from streamwright.core.sdk.contract import ResolvedConnection
from streamwright.core.sdk.network import HttpClient, HttpError

__all__ = ["SheetsClient", "connect", "read_pages", "translate_error", "validate_connection"]

SHEETS_API_BASE: Final = "https://sheets.googleapis.com/v4/spreadsheets"
TOKEN_URI: Final = "https://oauth2.googleapis.com/token"
SCOPES: Final = ("https://www.googleapis.com/auth/spreadsheets.readonly",)
DEFAULT_BATCH_SIZE: Final = 1000
CREDENTIAL_KEYS: Final = ("service_account_json", "credentials", "access_token", "refresh_token", "api_key")
Call = Callable[..., Any]


def validate_connection(connection: Mapping[str, Any]) -> list[str]:
    """Why a resolved connection cannot be used: no credentials, or a refresh token without its client."""
    if not any(connection.get(key) for key in CREDENTIAL_KEYS):
        return ["one of service_account_json (or credentials), access_token, refresh_token (with client_id and "
                "client_secret) or api_key is required"]
    if connection.get("refresh_token") and not (connection.get("client_id") and connection.get("client_secret")):
        return ["refresh_token needs client_id and client_secret"]
    return []


class SheetsClient:
    """The Sheets API v4 over the core HTTP client; its credentials travel in headers, never in a URL."""

    def __init__(self, http: HttpClient, credentials: Any = None, access_token: str | None = None,
                 api_key: str | None = None, default_spreadsheet_id: Any = None):
        self.http = http
        self.credentials = credentials
        self.access_token = access_token
        self.api_key = api_key
        self.default_spreadsheet_id = default_spreadsheet_id

    def headers(self) -> dict[str, str]:
        if self.credentials is not None:
            if not self.credentials.valid:
                self.credentials.refresh(google.auth.transport.requests.Request())
            return {"Authorization": "Bearer %s" % self.credentials.token}
        if self.access_token:
            return {"Authorization": "Bearer %s" % self.access_token}
        return {"x-goog-api-key": self.api_key} if self.api_key else {}

    def get_spreadsheet(self, spreadsheet_id: Any, fields: str | None = None) -> Any:
        """Spreadsheet metadata (title, sheets list, properties)."""
        return self.http.request("GET", "/%s" % spreadsheet_id, params={"fields": fields} if fields else None,
                                 headers=self.headers())

    def get_values(self, spreadsheet_id: Any, range_name: str, major_dimension: str = "ROWS",
                   value_render_option: str = "FORMATTED_VALUE",
                   date_time_render_option: str = "FORMATTED_STRING", *,
                   extra_params: Mapping[str, Any] | None = None) -> Any:
        """Cell values of a sheet or an A1 range; `extra_params` are vendor values.get query parameters."""
        params = {"majorDimension": major_dimension, "valueRenderOption": value_render_option,
                  "dateTimeRenderOption": date_time_render_option}
        params.update(extra_params or {})  # vendor params (valueRenderOption, dateTimeRenderOption, ...) override
        return self.http.request("GET", "/%s/values/%s" % (spreadsheet_id, quote(range_name, safe="!:")),
                                 params=params, headers=self.headers())


def _service_account_info(raw: Any) -> Mapping[str, Any]:
    """The service account key as a mapping: JSON text, a mapping or the path of a JSON file."""
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        raise ConnectorError("google_sheets: credentials must be a JSON string, dict, or file path")
    text = raw.strip()
    if text.startswith("{"):
        try:
            return json.loads(text)
        except ValueError as err:
            raise ConnectorError("google_sheets: invalid service account JSON: %s" % err) from None
    with open(text, encoding="utf-8") as handle:
        return json.load(handle)


def connect(connection: ResolvedConnection, remember: Callable[[Any], None],
            redact: Callable[[Any], str]) -> SheetsClient:
    """
    A client for the resolved connection. `remember` masks what the connector learns of the credentials (a service
    account's key); `redact` masks the HTTP client's logs. The calls are not retried here: `call` retries and rate
    limits them.
    """
    http = HttpClient(base_url=SHEETS_API_BASE, headers={"Accept": "application/json",
                                                           "User-Agent": "StreamWright-GoogleSheets-Connector"},
                      retry={"max_attempts": 1}, redact=redact)
    spreadsheet = connection.get("spreadsheet_id")
    if connection.get("api_key"):
        return SheetsClient(http, api_key=str(connection["api_key"]), default_spreadsheet_id=spreadsheet)
    if connection.get("access_token"):
        return SheetsClient(http, access_token=str(connection["access_token"]), default_spreadsheet_id=spreadsheet)
    if connection.get("refresh_token"):
        credentials = UserCredentials(None, refresh_token=str(connection["refresh_token"]), token_uri=TOKEN_URI,
                                      client_id=str(connection["client_id"]),
                                      client_secret=str(connection["client_secret"]), scopes=list(SCOPES))
        return SheetsClient(http, credentials=credentials, default_spreadsheet_id=spreadsheet)
    info = _service_account_info(connection.get("credentials") or connection.get("service_account_json"))
    for name in ("private_key", "client_email"):
        if name in info:
            remember(str(info[name]))
    credentials = ServiceAccountCredentials.from_service_account_info(info, scopes=list(SCOPES))
    return SheetsClient(http, credentials=credentials, default_spreadsheet_id=spreadsheet)


# * -----
# * reads
# * -----

def _sheet(properties: Mapping[str, Any]) -> dict[str, Any]:
    grid = properties.get("gridProperties", {})
    return {"sheet_id": properties.get("sheetId"), "title": properties.get("title"), "index": properties.get("index"),
            "row_count": grid.get("rowCount"), "column_count": grid.get("columnCount")}


def _range(sheets: SheetsClient, spreadsheet_id: Any, sheet_name: str | None, call: Call) -> str:
    """The sheet to read as an A1 range: the named one, else the first."""
    if sheet_name:
        return "'%s'" % sheet_name
    meta = call(sheets.get_spreadsheet, spreadsheet_id, "sheets.properties.title")
    listed = meta.get("sheets", [])
    if not listed:
        return "Sheet1"
    return "'%s'" % listed[0].get("properties", {}).get("title", "Sheet1")


def _columns(values: list[list[Any]], header_row: int) -> tuple[list[str], list[list[Any]]]:
    """(unique column names, data rows): the header row's cells (blank: col_N), or col_1.. when header_row is 0."""
    if header_row <= 0:
        return ["col_%d" % (index + 1) for index in range(max(len(row) for row in values))], values
    seen: dict[str, int] = {}
    names = []
    for index, cell in enumerate(values[header_row - 1]):
        name = str(cell).strip() if cell is not None and str(cell).strip() else "col_%d" % (index + 1)
        seen[name] = seen.get(name, 0) + 1
        names.append(name if seen[name] == 1 else "%s_%d" % (name, seen[name]))
    return names, values[header_row:]


def _whole_number(arguments: Mapping[str, Any], key: str, default: int, minimum: int) -> int:
    value = arguments.get(key)
    value = default if value is None else value
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConnectorError("google_sheets: `%s` must be a whole number >= %d" % (key, minimum))
    return value


def read_pages(sheets: SheetsClient, method: str, arguments: Mapping[str, Any],
               call: Call, vendor: Mapping[str, Any] | None = None) -> Iterator[list[dict[str, Any]]]:
    """The records of a read, in batches of `batch_size`; `call` makes the API calls (rate limit, retries)."""
    spreadsheet_id = arguments.get("spreadsheet_id") or sheets.default_spreadsheet_id
    if not spreadsheet_id:
        raise ConnectorError("google_sheets: `spreadsheet_id` must be specified in request arguments or the connection")
    if method == "list_sheets":
        data = call(sheets.get_spreadsheet, spreadsheet_id, "sheets.properties")
        yield [_sheet(item.get("properties", {})) for item in data.get("sheets", [])]
        return
    header_row = _whole_number(arguments, "header_row", 1, 0)
    batch_size = _whole_number(arguments, "batch_size", DEFAULT_BATCH_SIZE, 1)
    range_name = arguments.get("range") or _range(sheets, spreadsheet_id, arguments.get("sheet"), call)
    values = call(sheets.get_values, spreadsheet_id, range_name,
                  value_render_option=arguments.get("value_render_option", "FORMATTED_VALUE"),
                  extra_params=dict(vendor or {})).get("values", [])
    if not values or header_row > len(values):
        return
    names, rows = _columns(values, header_row)
    for start in range(0, len(rows), batch_size):
        yield [{name: row[index] if index < len(row) else None for index, name in enumerate(names)}
               for row in rows[start:start + batch_size]]


def translate_error(exc: Exception) -> ConnectorError | None:
    """The ConnectorError of an HTTP failure; None for exceptions that are not API errors."""
    if not isinstance(exc, HttpError):
        return None
    status, text = exc.status, str(exc)
    if status == 429:
        return ConnectorError("google_sheets: rate limit exceeded (quota): %s" % text, code=429, retryable=True)
    if status is None:
        return ConnectorError("google_sheets: network error: %s" % text, retryable=True)
    if status in (500, 502, 503, 504):
        return ConnectorError("google_sheets: server error (%d): %s" % (status, text), code=status, retryable=True)
    if status == 404:
        return ConnectorError("google_sheets: spreadsheet or range not found: %s" % text, code=404)
    if status in (401, 403):
        return ConnectorError("google_sheets: authentication/permission denied (%d): %s" % (status, text),
                              code=status)
    return ConnectorError("google_sheets: HTTP %d: %s" % (status, text), code=status)
