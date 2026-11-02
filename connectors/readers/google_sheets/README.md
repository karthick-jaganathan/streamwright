# Google Sheets Connector (`google_sheets`)

Read-only tabular data extraction from Google Spreadsheets via the Google Sheets API v4.

Streams rows directly into DuckDB SQL transformations and exports to Parquet, DuckDB, Singer, or cloud destinations.

## Supported Auth Methods

1. **Service Account JSON (Recommended)**:
   ```yaml
   auth:
     provider: google_sheets
     service_account_json: "{{ secrets.gcp_service_account_json }}"
   ```
   *Make sure the target Google Sheet is shared with the Service Account email (`client_email`) with at least "Viewer" permissions.*

2. **OAuth 2.0 (Refresh Token)**:
   ```yaml
   auth:
     provider: google_sheets
     client_id: "{{ secrets.client_id }}"
     client_secret: "{{ secrets.client_secret }}"
     refresh_token: "{{ secrets.refresh_token }}"
   ```

3. **API Key (Public Spreadsheets)**:
   ```yaml
   auth:
     provider: google_sheets
     api_key: "{{ secrets.google_api_key }}"
   ```

## Supported Methods

### `read` / `get_values`
Extracts rows from a specific sheet tab or A1 cell range.

```yaml
requests:
  - name: raw_sheet
    sdk: google_sheets
    service: spreadsheet
    method: read
    arguments:
      spreadsheet_id: "{{ config.spreadsheet_id }}"
      sheet: "Budgets"              # or range: "Budgets!A1:G1000"
      header_row: 1                 # 1-indexed (default: 1; 0 for generated col_1, col_2...)
      batch_size: 1000              # streaming chunk size
      value_render_option: "FORMATTED_VALUE"  # or "UNFORMATTED_VALUE"
```

### `list_sheets`
Lists metadata for all tabs/sheets inside a spreadsheet workbook.

```yaml
requests:
  - name: workbook_tabs
    sdk: google_sheets
    service: spreadsheet
    method: list_sheets
    arguments:
      spreadsheet_id: "{{ config.spreadsheet_id }}"
```

## Engines

The package registers one entry point, `streamwright.connectors` -> `streamwright.connectors.google_sheets:CONNECTOR`
(connector contract v1). Engines load the adapter they need from `CONNECTOR.adapters`; the vendor facts (credential
handling, the Sheets client, column naming) live once in `client.py`.

| Engine | Adapter | Needs |
|---|---|---|
| source (`streamwright run`, DuckDBJob call steps) | `engines/source.py` | nothing more |
| SparkJob | `engines/spark.py` | the `[spark]` extra (`pip install "./connectors/readers/google_sheets[spark]"`); reads on the driver and pins the records for replay |

The user guide (`guide.md`, shipped with the package) has the generated tables of connection fields and arguments.
