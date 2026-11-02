"""The Sheets client: credentials in headers, sheets as records, errors as ConnectorError."""

from types import MappingProxyType
from unittest.mock import MagicMock, patch

import pytest
import responses

from streamwright.connectors.google_sheets import client
from streamwright.core.io.api_calls import make_caller, standalone_context
from streamwright.core.sdk import ConnectorError, Redactor
from streamwright.core.sdk.network import HttpClient, HttpError

BASE = client.SHEETS_API_BASE


def sheets(**credentials):
    return client.SheetsClient(HttpClient(base_url=BASE, retry={"max_attempts": 1}), **credentials)


def read(method, arguments, **credentials):
    caller = make_caller(standalone_context(Redactor([])), client.translate_error)
    return list(client.read_pages(sheets(**credentials), method, arguments, caller))


def values_url(spreadsheet, sheet):
    return "%s/%s/values/%%27%s%%27" % (BASE, spreadsheet, sheet)


def test_a_connection_needs_credentials_and_a_refresh_token_its_client():
    assert client.validate_connection({"api_key": "k"}) == []
    assert client.validate_connection({"service_account_json": "{}"}) == []
    assert client.validate_connection({"refresh_token": "r", "client_id": "i", "client_secret": "s"}) == []
    assert "is required" in client.validate_connection({"spreadsheet_id": "1"})[0]
    assert client.validate_connection({"refresh_token": "r", "client_id": "i"}) == [
        "refresh_token needs client_id and client_secret"]


def test_connecting_picks_the_credentials_and_masks_a_service_account_key():
    remembered = []
    connection = MappingProxyType({"api_key": "AIzaSyTestKey123", "spreadsheet_id": "s1"})
    sheets_client = client.connect(connection, remembered.append, str)
    assert sheets_client.headers() == {"x-goog-api-key": "AIzaSyTestKey123"}
    assert sheets_client.default_spreadsheet_id == "s1"
    assert client.connect({"access_token": "ya29.t"}, remembered.append, str).headers() == {
        "Authorization": "Bearer ya29.t"}
    key = {"type": "service_account", "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMII\n",
           "client_email": "sa@p.iam"}
    with patch("google.oauth2.service_account.Credentials.from_service_account_info") as from_info:
        from_info.return_value = MagicMock()
        built = client.connect({"service_account_json": key}, remembered.append, str)
    assert built.credentials is from_info.return_value
    assert remembered == [key["private_key"], key["client_email"]]
    with pytest.raises(ConnectorError, match="invalid service account JSON"):
        client.connect({"service_account_json": "{not json"}, remembered.append, str)


@responses.activate
def test_the_header_row_names_the_columns():
    responses.add(responses.GET, values_url("s1", "Budgets"), status=200, json={"values": [
        ["campaign_id", "budget"], ["cmp_01", 1500.5], ["cmp_02", 2300.0]]})
    (page,) = read("read", {"spreadsheet_id": "s1", "sheet": "Budgets", "header_row": 1, "batch_size": 10},
                   api_key="k")
    assert page == [{"campaign_id": "cmp_01", "budget": 1500.5}, {"campaign_id": "cmp_02", "budget": 2300.0}]


@responses.activate
def test_duplicate_and_blank_headers_get_unique_names_and_short_rows_are_padded():
    responses.add(responses.GET, values_url("s1", "Data"), status=200, json={"values": [
        ["name", "name", "", "score"], ["Alice", "AliasA", "extra", 95], ["Bob"]]})
    (page,) = read("read", {"spreadsheet_id": "s1", "sheet": "Data"}, api_key="k")
    assert page == [{"name": "Alice", "name_2": "AliasA", "col_3": "extra", "score": 95},
                    {"name": "Bob", "name_2": None, "col_3": None, "score": None}]


@responses.activate
def test_without_a_header_row_columns_are_numbered_and_pages_follow_batch_size():
    responses.add(responses.GET, values_url("s1", "Raw"), status=200,
                  json={"values": [["101", "Item A"], ["102", "Item B"], ["103", "Item C"]]})
    pages = read("read", {"spreadsheet_id": "s1", "sheet": "Raw", "header_row": 0, "batch_size": 2}, api_key="k")
    assert pages == [[{"col_1": "101", "col_2": "Item A"}, {"col_1": "102", "col_2": "Item B"}],
                     [{"col_1": "103", "col_2": "Item C"}]]


@responses.activate
def test_an_empty_sheet_has_no_pages_and_the_first_sheet_is_the_default():
    responses.add(responses.GET, "%s/s1?fields=sheets.properties.title" % BASE, status=200,
                  json={"sheets": [{"properties": {"title": "Tab One"}}]})
    responses.add(responses.GET, values_url("s1", "Tab%20One"), status=200, json={"values": []})
    assert read("read", {"spreadsheet_id": "s1"}, api_key="k") == []


@responses.activate
def test_list_sheets():
    responses.add(responses.GET, "%s/wb?fields=sheets.properties" % BASE, status=200, json={"sheets": [
        {"properties": {"sheetId": 0, "title": "Summary", "index": 0, "gridProperties": {"rowCount": 100,
                                                                                         "columnCount": 10}}},
        {"properties": {"sheetId": 89234, "title": "Raw Data", "index": 1}}]})
    (page,) = read("list_sheets", {"spreadsheet_id": "wb"}, api_key="k")
    assert page[0] == {"sheet_id": 0, "title": "Summary", "index": 0, "row_count": 100, "column_count": 10}
    assert page[1]["sheet_id"] == 89234 and page[1]["row_count"] is None


def test_the_spreadsheet_comes_from_the_call_or_the_connection():
    with pytest.raises(ConnectorError, match="`spreadsheet_id` must be specified"):
        read("list_sheets", {}, api_key="k")


@pytest.mark.parametrize(("arguments", "message"), [({"header_row": -1}, "`header_row` must be a whole number >= 0"),
                                                    ({"batch_size": 0}, "`batch_size` must be a whole number >= 1"),
                                                    ({"batch_size": True}, "`batch_size` must be a whole number >= 1")])
def test_numbers_are_checked_when_read(arguments, message):
    with pytest.raises(ConnectorError, match=message):
        read("read", dict(arguments, spreadsheet_id="s1", sheet="x"), api_key="k")


def test_http_failures_become_connector_errors():
    limited = client.translate_error(HttpError("Rate limit exceeded", status=429))
    assert (limited.code, limited.retryable) == (429, True)
    missing = client.translate_error(HttpError("Requested entity was not found.", status=404))
    assert (missing.code, missing.retryable) == (404, False)
    assert client.translate_error(HttpError("denied", status=403)).retryable is False
    assert client.translate_error(HttpError("reset")).retryable is True
    assert client.translate_error(ValueError("not ours")) is None


@responses.activate
def test_the_api_key_travels_in_a_header_never_in_the_url():
    responses.add(responses.GET, "%s/s1?fields=sheets.properties" % BASE, json={"sheets": []}, status=200)
    read("list_sheets", {"spreadsheet_id": "s1"}, api_key="test_key")
    request = responses.calls[0].request
    assert request.headers["x-goog-api-key"] == "test_key" and "test_key" not in request.url


@responses.activate
def test_vendor_params_reach_the_values_get_request():
    responses.add(responses.GET, values_url("s1", "Data"), status=200, json={"values": [["id"], ["x"]]})
    caller = make_caller(standalone_context(Redactor([])), client.translate_error)
    list(client.read_pages(sheets(api_key="k"), "read", {"spreadsheet_id": "s1", "sheet": "Data"}, caller,
                           {"dateTimeRenderOption": "SERIAL_NUMBER", "majorDimension": "COLUMNS"}))
    url = responses.calls[-1].request.url
    assert "dateTimeRenderOption=SERIAL_NUMBER" in url and "majorDimension=COLUMNS" in url
