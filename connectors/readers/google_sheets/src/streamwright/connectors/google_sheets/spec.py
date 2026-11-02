# Copyright 2026 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""What the google_sheets connector accepts: its connection fields and the reads of the Sheets API v4 (pure data)."""

from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, OperationKind

__all__ = ["CONNECTOR"]

_SPREADSHEET = Field("spreadsheet_id", "string", description="the spreadsheet's id (default: the connection's)")
_HEADER_ROW = Field("header_row", "integer", default=1,
                    description="the 1-indexed row of the column names; 0: none (col_1, col_2, ...)")
_RENDER = Field("value_render_option", "string", default="FORMATTED_VALUE",
                choices=("FORMATTED_VALUE", "UNFORMATTED_VALUE", "FORMULA"), description="how cell values are rendered")
_BATCH = Field("batch_size", "integer", default=1000, description="the records per page")


def _read(method: str, description: str, *arguments: Field) -> Operation:
    return Operation(service="spreadsheet", method=method, kind=OperationKind.READ, description=description,
                     arguments=arguments)


CONNECTOR = ConnectorDefinition(
    name="google_sheets",
    title="Google Sheets",
    family="spreadsheets",
    summary="Google Sheets API v4 cell values as records",
    connection=(
        Field("service_account_json", "string", secret=True,
              description="a service account key: JSON text or the path of a JSON file"),
        Field("credentials", "string", secret=True, description="the same as service_account_json"),
        Field("access_token", "string", secret=True, description="an OAuth access token"),
        Field("refresh_token", "string", secret=True, description="an OAuth refresh token (with client_id and secret)"),
        Field("client_id", "string", secret=True, description="the OAuth client id of the refresh token"),
        Field("client_secret", "string", secret=True, description="the OAuth client secret of the refresh token"),
        Field("api_key", "string", secret=True, description="an API key (public spreadsheets)"),
        Field("spreadsheet_id", "string", description="the default spreadsheet of the calls"),
    ),
    operations=(
        _read("read", "A sheet (or A1 range) as records; the header row names the columns.",
              _SPREADSHEET, Field("sheet", "string", description="the sheet (tab) title; default: the first sheet"),
              Field("range", "string", description="an A1 range such as Sheet1!A1:Z500 (instead of `sheet`)"),
              _HEADER_ROW, _RENDER, _BATCH),
        _read("get_values", "An A1 range as records; the header row names the columns.",
              _SPREADSHEET, Field("range", "string", required=True, description="an A1 range such as Sheet1!A1:Z500"),
              _HEADER_ROW, _RENDER, _BATCH),
        _read("list_sheets", "The sheets (tabs) of a spreadsheet.", _SPREADSHEET),
    ),
    adapters={
        "source": "streamwright.connectors.google_sheets.engines.source:GoogleSheetsSourceAdapter",
        "spark": "streamwright.connectors.google_sheets.engines.spark:GoogleSheetsSparkAdapter",
    },
    validator="streamwright.connectors.google_sheets.client:validate_connection",
    loggers=("streamwright.network", "urllib3", "google.auth"),  # HTTP client calls; google.auth: token refresh
)
