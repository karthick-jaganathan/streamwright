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

from unittest.mock import MagicMock, patch
import pytest
import requests
import responses

from streamwright.core.runtime.components import ConnectorError
from streamwright.connectors.google_sheets.connector import (
    GoogleSheetsConnector,
    GoogleSheetsClient,
)


class DummyContext:
    def __init__(self):
        self.secrets = []

    def secret(self, val):
        self.secrets.append(val)


def test_spec():
    connector = GoogleSheetsConnector()
    assert connector.spec.name == "google_sheets"
    assert connector.spec.category == "spreadsheets"
    assert connector.spec.transport == "sdk"
    assert connector.spec.package == "google-auth"


def test_check_request():
    connector = GoogleSheetsConnector()
    assert connector.check_request({"method": "invalid"}) != []
    assert connector.check_request({"method": "read", "arguments": "not-a-dict"}) != []
    assert connector.check_request({"method": "read", "arguments": {}}) == []
    assert connector.check_request({"method": "get_values", "arguments": {}}) == []
    assert connector.check_request({"method": "list_sheets", "arguments": {}}) == []


def test_missing_auth():
    connector = GoogleSheetsConnector()
    ctx = DummyContext()
    with pytest.raises(ConnectorError, match="auth requires either `service_account_json`"):
        connector.connect({}, ctx)


def test_connect_api_key():
    connector = GoogleSheetsConnector()
    ctx = DummyContext()
    client = connector.connect({"api_key": "AIzaSyTestKey123"}, ctx)
    assert client.api_key == "AIzaSyTestKey123"
    assert "AIzaSyTestKey123" in ctx.secrets


def test_connect_access_token():
    connector = GoogleSheetsConnector()
    ctx = DummyContext()
    client = connector.connect({"access_token": "ya29.test_token"}, ctx)
    assert client.access_token == "ya29.test_token"
    assert "ya29.test_token" in ctx.secrets


def test_connect_service_account_dict():
    connector = GoogleSheetsConnector()
    ctx = DummyContext()
    dummy_sa = {
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "key123",
        "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA...\n-----END RSA PRIVATE KEY-----\n",
        "client_email": "sa@test-project.iam.gserviceaccount.com",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    with patch("google.oauth2.service_account.Credentials.from_service_account_info") as mock_sa:
        mock_creds = MagicMock()
        mock_sa.return_value = mock_creds
        client = connector.connect({"service_account_json": dummy_sa}, ctx)
        assert client.credentials == mock_creds
        assert dummy_sa["private_key"] in ctx.secrets
        assert dummy_sa["client_email"] in ctx.secrets


@responses.activate
def test_read_records_standard():
    spreadsheet_id = "test_sheet_123"
    responses.add(
        responses.GET,
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/%27Budgets%27",
        json={
            "range": "Budgets!A1:D3",
            "majorDimension": "ROWS",
            "values": [
                ["campaign_id", "campaign_name", "budget", "status"],
                ["cmp_01", "Spring Sale", 1500.50, "ACTIVE"],
                ["cmp_02", "Summer Launch", 2300.00, "PAUSED"],
            ],
        },
        status=200,
    )

    connector = GoogleSheetsConnector()
    client = GoogleSheetsClient(api_key="test_key")
    ctx = DummyContext()

    batches = list(
        connector.request(
            client,
            {
                "method": "read",
                "arguments": {
                    "spreadsheet_id": spreadsheet_id,
                    "sheet": "Budgets",
                    "header_row": 1,
                    "batch_size": 10,
                },
            },
            ctx,
        )
    )

    assert len(batches) == 1
    rows = batches[0]
    assert len(rows) == 2
    assert rows[0] == {
        "campaign_id": "cmp_01",
        "campaign_name": "Spring Sale",
        "budget": 1500.50,
        "status": "ACTIVE",
    }
    assert rows[1] == {
        "campaign_id": "cmp_02",
        "campaign_name": "Summer Launch",
        "budget": 2300.00,
        "status": "PAUSED",
    }


@responses.activate
def test_read_records_ragged_and_duplicate_headers():
    spreadsheet_id = "ragged_sheet_456"
    responses.add(
        responses.GET,
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/%27Data%27",
        json={
            "range": "Data!A1:D4",
            "majorDimension": "ROWS",
            "values": [
                ["name", "name", "", "score"],  # duplicate "name", empty column
                ["Alice", "AliasA", "extra", 95],
                ["Bob"],  # shorter row missing subsequent columns
            ],
        },
        status=200,
    )

    connector = GoogleSheetsConnector()
    client = GoogleSheetsClient(api_key="test_key")
    ctx = DummyContext()

    batches = list(
        connector.request(
            client,
            {
                "method": "read",
                "arguments": {
                    "spreadsheet_id": spreadsheet_id,
                    "sheet": "Data",
                    "header_row": 1,
                },
            },
            ctx,
        )
    )

    assert len(batches) == 1
    rows = batches[0]
    assert len(rows) == 2
    # Check deduplication & column fallback: "name", "name_2", "col_3", "score"
    assert rows[0] == {
        "name": "Alice",
        "name_2": "AliasA",
        "col_3": "extra",
        "score": 95,
    }
    assert rows[1] == {
        "name": "Bob",
        "name_2": None,
        "col_3": None,
        "score": None,
    }


@responses.activate
def test_read_records_no_header():
    spreadsheet_id = "no_header_sheet"
    responses.add(
        responses.GET,
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/%27RawData%27",
        json={
            "range": "RawData!A1:B2",
            "majorDimension": "ROWS",
            "values": [
                ["101", "Item A"],
                ["102", "Item B"],
            ],
        },
        status=200,
    )

    connector = GoogleSheetsConnector()
    client = GoogleSheetsClient(api_key="test_key")
    ctx = DummyContext()

    batches = list(
        connector.request(
            client,
            {
                "method": "read",
                "arguments": {
                    "spreadsheet_id": spreadsheet_id,
                    "sheet": "RawData",
                    "header_row": 0,  # No header row
                },
            },
            ctx,
        )
    )

    assert len(batches) == 1
    rows = batches[0]
    assert len(rows) == 2
    assert rows[0] == {"col_1": "101", "col_2": "Item A"}
    assert rows[1] == {"col_2": "Item B", "col_1": "102"}


@responses.activate
def test_empty_sheet():
    spreadsheet_id = "empty_sheet"
    responses.add(
        responses.GET,
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/%27Empty%27",
        json={"range": "Empty!A1:Z100", "values": []},
        status=200,
    )

    connector = GoogleSheetsConnector()
    client = GoogleSheetsClient(api_key="test_key")
    batches = list(
        connector.request(
            client,
            {"method": "read", "arguments": {"spreadsheet_id": spreadsheet_id, "sheet": "Empty"}},
            DummyContext(),
        )
    )
    assert batches == []


@responses.activate
def test_list_sheets():
    spreadsheet_id = "multi_tab_workbook"
    responses.add(
        responses.GET,
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}?fields=sheets.properties&key=test_key",
        json={
            "sheets": [
                {
                    "properties": {
                        "sheetId": 0,
                        "title": "Summary",
                        "index": 0,
                        "gridProperties": {"rowCount": 100, "columnCount": 10},
                    }
                },
                {
                    "properties": {
                        "sheetId": 89234,
                        "title": "Raw Data",
                        "index": 1,
                        "gridProperties": {"rowCount": 5000, "columnCount": 26},
                    }
                },
            ]
        },
        status=200,
    )

    connector = GoogleSheetsConnector()
    client = GoogleSheetsClient(api_key="test_key")
    batches = list(
        connector.request(
            client,
            {"method": "list_sheets", "arguments": {"spreadsheet_id": spreadsheet_id}},
            DummyContext(),
        )
    )

    assert len(batches) == 1
    sheets = batches[0]
    assert len(sheets) == 2
    assert sheets[0]["title"] == "Summary"
    assert sheets[0]["row_count"] == 100
    assert sheets[1]["title"] == "Raw Data"
    assert sheets[1]["sheet_id"] == 89234


def test_error_mapping():
    connector = GoogleSheetsConnector()

    # Rate limit (429) -> retryable
    resp_429 = requests.Response()
    resp_429.status_code = 429
    resp_429._content = b'{"error": {"message": "Rate limit exceeded"}}'
    err_429 = requests.HTTPError(response=resp_429)
    mapped_429 = connector.error(err_429)
    assert mapped_429 is not None
    assert mapped_429.code == 429
    assert mapped_429.retryable is True

    # 404 Not Found -> non-retryable
    resp_404 = requests.Response()
    resp_404.status_code = 404
    resp_404._content = b'{"error": {"message": "Requested entity was not found."}}'
    err_404 = requests.HTTPError(response=resp_404)
    mapped_404 = connector.error(err_404)
    assert mapped_404 is not None
    assert mapped_404.code == 404
    assert mapped_404.retryable is False

    # 403 Permission Denied -> non-retryable
    resp_403 = requests.Response()
    resp_403.status_code = 403
    resp_403._content = b'{"error": {"message": "The caller does not have permission"}}'
    err_403 = requests.HTTPError(response=resp_403)
    mapped_403 = connector.error(err_403)
    assert mapped_403 is not None
    assert mapped_403.code == 403
    assert mapped_403.retryable is False
