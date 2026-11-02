# Google Sheets (`google_sheets`)

Google Sheets API v4 tabular data extraction.

Streams cell values from Google Sheets spreadsheets into DuckDB records.
Supports GCP Service Account JSON keys, OAuth2 refresh/access tokens, and API Keys.
Automatically normalizes 2D spreadsheet matrices into structured records with deduplicated headers.

## Connection

### `source.yaml` contract

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

## Operations

### `streams/<stream>.yaml` contract

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

## Engines

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`, DuckDBJob call steps) | `engines/source.py` | the Sheets API v4 over the core HTTP client; credentials in headers |
| SparkJob | `engines/spark.py` | runs on the Spark driver: records are fetched through the same client and pinned in the batch manifest for replay, so executors never call Google; `pip install "streamwright-google-sheets[spark]"` |

### SparkJob

```yaml
connections:
  reporting_sheets:
    provider: google_sheets
    service_account_json: "{{ secrets.gcp_service_account }}"
    spreadsheet_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"   # optional default spreadsheet
steps:
  - name: quarterly_targets
    connection: reporting_sheets
    service: spreadsheet
    method: read
    arguments:
      sheet: "Q3 Targets"
      header_row: 1
      value_render_option: FORMATTED_VALUE
    sql: "SELECT department, region, CAST(target_amount AS DOUBLE) AS target FROM input WHERE department IS NOT NULL"
```

Reads are driver-side and pinned: Spark never receives credentials in its configuration, and replaying a batch reuses
the recorded records. `google_sheets` is read-only (no `write` operation).
