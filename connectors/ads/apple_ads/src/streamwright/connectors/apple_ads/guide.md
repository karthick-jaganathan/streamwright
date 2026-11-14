# Apple Ads (`apple_ads`)

Apple Search Ads API campaigns, ad groups, keywords, and reports extraction.

Connects to the Apple Ads REST API using bearer token authentication.
Inherits the StreamWright HTTP engine with body-based offset pagination, account context headers, and DuckDB SQL transformations.

## Connection

### `source.yaml` contract

```yaml
kind: source
name: apple_ads_pipeline
spec:
  config:
    ad_account_id: {type: string, description: "Apple Ads ad account ID"}
  secrets:
    apple_ads_token: {type: string, required: true}

auth:
  provider: apple_ads
  type: bearer
  token: "{{ secrets.apple_ads_token }}"

http:
  base_url: https://api.ads.apple.com
  headers:
    X-AP-Context: "adAccountId={{ config.ad_account_id }}"
  paginator:
    type: offset
    in: body
    offset_param: pagination.offset
    limit_param: pagination.limit
    page_size: 100
    total_path: pagination.totalCount
  records:
    path: result
```

### Authentication & Security

- `provider`: `apple_ads`
- `type`: `bearer`
- Credentials: `token` must be a `{{ secrets.* }}` reference. Credentials and tokens are never written in source configs and are redacted from all logs.
- Security: Sandboxed HTTP transport restricted to read requests against Apple Ads endpoints.

## Operations

The connector offers one operation, `http.request`: each `requests:` item of a stream is an `http:` request, run by the
engine's HTTP reader with the source's `http:` defaults (base URL, headers, paginator, records, retry, rate limit).

### `streams/<stream>.yaml` contract

```yaml
requests:
  - name: raw_campaigns
    http:
      path: /v1/campaigns/query
      method: POST
      json:
        pagination:
          pageSize: 100
          fetchTotalCount: true

transform:
  mode: page
  steps:
    - name: final_campaigns
      select: |
        SELECT 
          (record->>'id')::BIGINT AS campaign_id,
          record->>'name' AS campaign_name,
          record->>'status' AS status,
          (record->>'$.dailyBudgetAmount.amount')::DOUBLE AS daily_budget
        FROM raw_campaigns

export:
  campaigns:
    step: final_campaigns
    primary_key: [campaign_id]
```

### Transform & Data Shaping

- Emits records as extracted JSON items from the `records.path` response envelope (`result`).
- In `transform` steps, raw JSON records are accessible via DuckDB JSON operators (`record->>'field'`).

### Vendor arguments

Any key in an `http` request's `params` block is a query parameter forwarded verbatim to the API; the paths and
parameters you call are whatever the API exposes, never a closed list in the connector (contract v1.3 §6b). For
example `params: {statusFilter: ENABLED}` reaches the request URL unchanged; pagination cursors the paginator
injects take precedence.

## DuckDBJob

A complete DuckDBJob that calls this connector (`streamwright duckdb validate` checks it offline; only the account values and the secret are yours):

```yaml
kind: DuckDBJob
version: 1
name: apple_ads_campaigns
spec:
  config:
    ad_account_id: {type: string, required: true, description: "the ad account"}
  secrets:
    apple_ads_token: {type: string, required: true}

connections:
  apple_ads_api:
    provider: apple_ads
    type: bearer
    token: "{{ secrets.apple_ads_token }}"
    base_url: https://api.ads.apple.com
    headers:
      X-AP-Context: "adAccountId={{ config.ad_account_id }}"
  out:
    provider: files
    roots: [output]

steps:
  - name: campaigns
    connection: apple_ads_api
    request:
      path: /v1/campaigns/query
      method: POST
      json: {pagination: {pageSize: 100, fetchTotalCount: true}}
      records: {path: result}
    paginator: {type: offset, in: body, offset_param: pagination.offset, limit_param: pagination.limit,
                page_size: 100, total_path: pagination.totalCount}
    retry: {codes: [429, 500, 502, 503, 504], max_attempts: 5, backoff: exponential}
    sql: |
      SELECT (record->>'id')::BIGINT AS campaign_id,
             record->>'name' AS campaign_name,
             record->>'status' AS status,
             (record->>'$.dailyBudgetAmount.amount')::DOUBLE AS daily_budget
      FROM records

outputs:
  - name: campaigns_file
    connection: out
    step: campaigns
    mode: overwrite
    path: apple_ads_campaigns_{{ config.ad_account_id }}.parquet
    format: parquet
```

- **Connection**: the authentication keys and the HTTP defaults (`base_url`, `headers`, `params_encoding`,
  `paginator`, `records`, `retry`, `rate_limit`) written directly on it. There is no `http:` block (that is a
  source manifest's): `validate` rejects keys the connector would ignore.
- **Step**: `request: {path, method, params, headers, json, records}`, plus `paginator`, `retry`, `rate_limit`.
  `path` is relative to the connection's `base_url`, or an absolute `https://` URL. This is a REST API connector:
  a step takes `request`, never `service`/`method` (those are SDK connectors' calls).
- **SQL**: the step's `sql` reads the table `records`, one JSON column `record`; extract with `->>`.

```bash
STREAMWRIGHT_SECRET_APPLE_ADS_TOKEN=... streamwright duckdb run campaigns.yaml --set ad_account_id=AD_ACCOUNT_ID
```

## Engines

### Execution & Behavior

- **Transport**: `http` (StreamWright HTTP engine).
- **Pagination**: Configured via body-based offset paginator (`in: body`).
- **Context**: Requires `X-AP-Context` header specifying `adAccountId`.

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`, DuckDBJob call steps) | `streamwright.core.io.http` | the engine's HTTP reader runs the source's `http:` requests: the connector declares the authentication it takes, the source declares the API |
