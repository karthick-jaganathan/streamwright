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
The `google_sheets` connector: Google Sheets API v4 tabular data extraction.

Streams cell values from Google Sheets spreadsheets into DuckDB records.
Supports GCP Service Account JSON keys, OAuth2 refresh/access tokens, and API Keys.
Automatically normalizes 2D spreadsheet matrices into structured records with deduplicated headers.

### 1. `source.yaml` Contract
```yaml
kind: source
name: google_sheets_pipeline

spec:
  config:
    spreadsheet_id: {type: string, description: "Google Sheets document ID"}
  secrets:
    service_account_json: {type: string, description: "GCP Service Account JSON credentials"}

auth:
  provider: google_sheets
  service_account_json: "{{ secrets.service_account_json }}"
  spreadsheet_id: "{{ config.spreadsheet_id }}"
```

### 2. `streams/<stream>.yaml` Contract
```yaml
requests:
  - name: raw_sheet
    sdk: google_sheets
    service: spreadsheet
    method: read              # "read" | "get_values" | "list_sheets"
    arguments:
      spreadsheet_id: "{{ config.spreadsheet_id }}"
      sheet: "Sheet1"         # or range: "Sheet1!A1:Z500"
      header_row: 1           # 1-indexed header row (default: 1; 0 for auto col_1, col_2)
      batch_size: 1000        # chunk size for streaming into DuckDB

transform:
  mode: page
  steps:
    - name: clean_records
      select: |
        SELECT
          record->>'id'                   AS record_id,
          record->>'name'                 AS name,
          (record->>'amount')::DOUBLE     AS amount,
          current_timestamp               AS _synced_at
        FROM raw_sheet

export:
  records:
    step: clean_records
    primary_key: [record_id]
```
"""

import json
import logging
from typing import Any, Dict, Generator, List, Optional, Union
from urllib.parse import quote

import requests
from requests.exceptions import HTTPError, RequestException

from streamwright.core.runtime.components import Connector, ConnectorError, ConnectorSpec

__all__ = ["GoogleSheetsConnector", "GoogleSheetsClient"]

LOG = logging.getLogger("streamwright.connectors.google_sheets")
SHEETS_API_BASE = "https://sheets.googleapis.com/v4/spreadsheets"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
DEFAULT_BATCH_SIZE = 1000


class GoogleSheetsClient:
    """HTTP client wrapper for Google Sheets API v4 with token management."""

    def __init__(
        self,
        credentials: Optional[Any] = None,
        access_token: Optional[str] = None,
        api_key: Optional[str] = None,
        default_spreadsheet_id: Optional[str] = None,
    ):
        self.credentials = credentials
        self.access_token = access_token
        self.api_key = api_key
        self.default_spreadsheet_id = default_spreadsheet_id
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "StreamWright-GoogleSheets-Connector/0.0.1",
        })

    def _get_auth_headers(self) -> Dict[str, str]:
        headers = {}
        if self.credentials:
            import google.auth.transport.requests
            if not self.credentials.valid:
                req = google.auth.transport.requests.Request()
                self.credentials.refresh(req)
            headers["Authorization"] = f"Bearer {self.credentials.token}"
        elif self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return headers

    def get_spreadsheet(self, spreadsheet_id: str, fields: Optional[str] = None) -> Dict[str, Any]:
        """Fetch spreadsheet metadata (title, sheets list, properties)."""
        url = f"{SHEETS_API_BASE}/{spreadsheet_id}"
        params = {}
        if fields:
            params["fields"] = fields
        if self.api_key:
            params["key"] = self.api_key

        headers = self._get_auth_headers()
        res = self.session.get(url, params=params, headers=headers, timeout=30)
        res.raise_for_status()
        return res.json()

    def get_values(
        self,
        spreadsheet_id: str,
        range_name: str,
        major_dimension: str = "ROWS",
        value_render_option: str = "FORMATTED_VALUE",
        date_time_render_option: str = "FORMATTED_STRING",
    ) -> Dict[str, Any]:
        """Fetch cell values from a sheet or specific A1 range."""
        quoted_range = quote(range_name, safe="!:")
        url = f"{SHEETS_API_BASE}/{spreadsheet_id}/values/{quoted_range}"
        params = {
            "majorDimension": major_dimension,
            "valueRenderOption": value_render_option,
            "dateTimeRenderOption": date_time_render_option,
        }
        if self.api_key:
            params["key"] = self.api_key

        headers = self._get_auth_headers()
        res = self.session.get(url, params=params, headers=headers, timeout=30)
        res.raise_for_status()
        return res.json()


class GoogleSheetsConnector(Connector):
    """The `google_sheets` connector for read-only extraction from Google Spreadsheets."""

    spec = ConnectorSpec(
        name="google_sheets",
        title="Google Sheets",
        category="spreadsheets",
        transport="sdk",
        package="google-auth",
        loggers=("streamwright.network", "google.auth"),
    )

    auth_required = ()
    auth_optional = (
        "type",
        "credentials",
        "service_account_json",
        "access_token",
        "refresh_token",
        "client_id",
        "client_secret",
        "api_key",
        "spreadsheet_id",
    )

    def check_request(self, request: Dict[str, Any]) -> List[str]:
        method = request.get("method")
        if method not in ("read", "get_values", "list_sheets"):
            return [f"google_sheets: unknown method {method!r} (supported: read, get_values, list_sheets)"]

        arguments = request.get("arguments") or {}
        if not isinstance(arguments, dict):
            return ["google_sheets: `arguments` must be a mapping"]

        return []

    def connect(self, auth: Dict[str, Any], context: Any) -> GoogleSheetsClient:
        auth_type = (auth.get("type") or "").lower()
        default_spreadsheet_id = auth.get("spreadsheet_id")

        # 1. API Key Auth
        api_key = auth.get("api_key")
        if api_key:
            context.secret(str(api_key))
            return GoogleSheetsClient(api_key=str(api_key), default_spreadsheet_id=default_spreadsheet_id)

        # 2. Access Token (direct bearer)
        access_token = auth.get("access_token")
        if access_token:
            context.secret(str(access_token))
            return GoogleSheetsClient(access_token=str(access_token), default_spreadsheet_id=default_spreadsheet_id)

        # 3. OAuth2 Refresh Token
        refresh_token = auth.get("refresh_token")
        client_id = auth.get("client_id")
        client_secret = auth.get("client_secret")
        if refresh_token and client_id and client_secret:
            context.secret(str(refresh_token))
            context.secret(str(client_secret))
            from google.oauth2.credentials import Credentials
            creds = Credentials(
                None,
                refresh_token=str(refresh_token),
                token_uri="https://oauth2.googleapis.com/token",
                client_id=str(client_id),
                client_secret=str(client_secret),
                scopes=SCOPES,
            )
            return GoogleSheetsClient(credentials=creds, default_spreadsheet_id=default_spreadsheet_id)

        # 4. Service Account Key
        raw_creds = auth.get("credentials") or auth.get("service_account_json")
        if raw_creds:
            if isinstance(raw_creds, str):
                raw_str = raw_creds.strip()
                if raw_str.startswith("{"):
                    try:
                        creds_dict = json.loads(raw_str)
                    except Exception as err:
                        raise ConnectorError(f"google_sheets: invalid service account JSON: {err}")
                else:
                    # File path
                    with open(raw_str, "r", encoding="utf-8") as f:
                        creds_dict = json.load(f)
            elif isinstance(raw_creds, dict):
                creds_dict = raw_creds
            else:
                raise ConnectorError("google_sheets: credentials must be a JSON string, dict, or file path")

            # Redact secrets
            if "private_key" in creds_dict:
                context.secret(str(creds_dict["private_key"]))
            if "client_email" in creds_dict:
                context.secret(str(creds_dict["client_email"]))

            from google.oauth2.service_account import Credentials
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            return GoogleSheetsClient(credentials=creds, default_spreadsheet_id=default_spreadsheet_id)

        raise ConnectorError(
            "google_sheets: auth requires either `service_account_json` (or `credentials`), "
            "`access_token`, `refresh_token` (with client_id and client_secret), or `api_key`"
        )

    def request(
        self, client: GoogleSheetsClient, request: Dict[str, Any], context: Any
    ) -> Generator[List[Dict[str, Any]], None, None]:
        problems = self.check_request(request)
        if problems:
            raise ConnectorError("; ".join(problems))

        method = request.get("method")
        arguments = request.get("arguments") or {}

        spreadsheet_id = arguments.get("spreadsheet_id") or client.default_spreadsheet_id
        if not spreadsheet_id:
            raise ConnectorError("google_sheets: `spreadsheet_id` must be specified in request arguments or auth")

        # Method: list_sheets
        if method == "list_sheets":
            try:
                data = client.get_spreadsheet(spreadsheet_id, fields="sheets.properties")
            except Exception as exc:
                err = self.error(exc)
                raise err if err else exc

            sheets = []
            for item in data.get("sheets", []):
                props = item.get("properties", {})
                grid = props.get("gridProperties", {})
                sheets.append({
                    "sheet_id": props.get("sheetId"),
                    "title": props.get("title"),
                    "index": props.get("index"),
                    "row_count": grid.get("rowCount"),
                    "column_count": grid.get("columnCount"),
                })
            yield sheets
            return

        # Method: read / get_values
        range_name = arguments.get("range")
        sheet_name = arguments.get("sheet")
        header_row = int(arguments.get("header_row", 1))
        batch_size = int(arguments.get("batch_size") or DEFAULT_BATCH_SIZE)
        if batch_size <= 0:
            batch_size = DEFAULT_BATCH_SIZE

        value_render = arguments.get("value_render_option", "FORMATTED_VALUE")

        # Determine target range
        if not range_name:
            if sheet_name:
                range_name = f"'{sheet_name}'"
            else:
                # Query first sheet title
                try:
                    meta = client.get_spreadsheet(spreadsheet_id, fields="sheets.properties.title")
                    sheet_list = meta.get("sheets", [])
                    if sheet_list:
                        first_title = sheet_list[0].get("properties", {}).get("title", "Sheet1")
                        range_name = f"'{first_title}'"
                    else:
                        range_name = "Sheet1"
                except Exception as exc:
                    err = self.error(exc)
                    raise err if err else exc

        try:
            res_data = client.get_values(
                spreadsheet_id=spreadsheet_id,
                range_name=range_name,
                value_render_option=value_render,
            )
        except Exception as exc:
            err = self.error(exc)
            raise err if err else exc

        values = res_data.get("values", [])
        if not values:
            return

        # Header processing
        if header_row > 0:
            if len(values) < header_row:
                return
            raw_headers = values[header_row - 1]
            data_rows = values[header_row:]

            # Deduplicate and sanitize headers
            seen = {}
            headers = []
            for i, h in enumerate(raw_headers):
                name = str(h).strip() if h is not None and str(h).strip() else f"col_{i+1}"
                if name in seen:
                    seen[name] += 1
                    name = f"{name}_{seen[name]}"
                else:
                    seen[name] = 1
                headers.append(name)
        else:
            # header_row == 0: generate col_1, col_2, ...
            max_cols = max(len(row) for row in values) if values else 0
            headers = [f"col_{i+1}" for i in range(max_cols)]
            data_rows = values

        batch = []
        for row in data_rows:
            record = {}
            for i, h in enumerate(headers):
                val = row[i] if i < len(row) else None
                record[h] = val
            batch.append(record)

            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def error(self, exc: Exception) -> Optional[ConnectorError]:
        if isinstance(exc, HTTPError) and exc.response is not None:
            status = exc.response.status_code
            try:
                body = exc.response.json()
                msg = body.get("error", {}).get("message") or exc.response.text
            except Exception:
                msg = exc.response.text

            if status == 429:
                return ConnectorError(f"google_sheets: rate limit exceeded (quota): {msg}", code=429, retryable=True)
            if status in (500, 502, 503, 504):
                return ConnectorError(f"google_sheets: server error ({status}): {msg}", code=status, retryable=True)
            if status == 404:
                return ConnectorError(f"google_sheets: spreadsheet or range not found: {msg}", code=404, retryable=False)
            if status in (401, 403):
                return ConnectorError(f"google_sheets: authentication/permission denied ({status}): {msg}", code=status, retryable=False)
            return ConnectorError(f"google_sheets: HTTP {status}: {msg}", code=status, retryable=False)

        if isinstance(exc, RequestException):
            return ConnectorError(f"google_sheets: network error: {exc}", retryable=True)

        return None
