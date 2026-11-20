# Amazon Ads (`amazon_ads`)

Amazon Advertising API Sponsored Products, Profiles, and Ads extraction.

Connects to the Amazon Advertising API using OAuth2 refresh token grant or static Bearer token.
Inherits the StreamWright HTTP engine with offset pagination, regional endpoint configuration, and DuckDB SQL transformations.

## Connection

### `source.yaml` contract

```yaml
kind: source
name: amazon_ads_pipeline
spec:
  config:
    api_url: {type: string, default: "https://advertising-api.amazon.com"}
    profile_id: {type: string, required: false, description: "Amazon Advertising Profile ID"}
    start_date: {type: date, default: "-30d"}   # the first day report streams read
  secrets:
    amazon_client_id: {type: string, required: true}
    amazon_client_secret: {type: string, required: true}
    amazon_refresh_token: {type: string, required: true}

auth:
  provider: amazon_ads
  type: oauth2_refresh_token
  token_url: https://api.amazon.com/auth/o2/token
  client_id: "{{ secrets.amazon_client_id }}"
  client_secret: "{{ secrets.amazon_client_secret }}"
  refresh_token: "{{ secrets.amazon_refresh_token }}"

http:
  base_url: "{{ config.api_url }}"
  headers:
    Amazon-Advertising-API-ClientId: "{{ secrets.amazon_client_id }}"
    Amazon-Advertising-API-Scope: "{{ config.profile_id }}"
  paginator:
    type: offset
    offset_param: startIndex
    limit_param: count
    page_size: 100
```

### Authentication & Security

- `provider`: `amazon_ads`
- `type`: `oauth2_refresh_token` or `bearer`
- Credentials: `client_id`, `client_secret`, and `refresh_token` must be `{{ secrets.* }}` references. Credentials and tokens are never written in source configs and are redacted from all logs.
- Security: Sandboxed HTTP transport restricted to read requests against Amazon Advertising endpoints.

## Operations

The connector offers one operation, `http.request`: the `operation:` of each `requests:` item of a stream is an HTTP
request, run by the engine's HTTP reader with the source's `http:` defaults (base URL, headers, paginator, records,
retry, rate limit).

### `streams/<stream>.yaml` contract

```yaml
requests:
  - name: raw_sp_campaigns
    operation:
      path: /v2/sp/campaigns
      method: GET

transform:
  mode: page
  steps:
    - name: final_sp_campaigns
      select: |
        SELECT 
          record->>'campaignId' AS campaign_id,
          record->>'name' AS campaign_name,
          record->>'campaignType' AS campaign_type,
          record->>'state' AS state,
          (record->>'dailyBudget')::DOUBLE AS daily_budget
        FROM raw_sp_campaigns

export:
  sp_campaigns:
    step: final_sp_campaigns
    primary_key: [campaign_id]
```

### Transform & Data Shaping

- Emits records as extracted JSON items from the response body.
- In `transform` steps, raw JSON records are accessible via DuckDB JSON operators (`record->>'field'`).

### Vendor arguments

Any key in an HTTP `operation:`'s `params` block is a query parameter forwarded verbatim to the API; the paths and
parameters you call are whatever the API exposes, never a closed list in the connector (contract v1.3 §6b). For
example `params: {statusFilter: ENABLED}` reaches the request URL unchanged; pagination cursors the paginator
injects take precedence.

## Performance reports

Metrics come from v3 reports, which are async: POST the report, poll it until `COMPLETED`, then download its gzip
JSON file. Each part of the `async_job` is an HTTP request, like a stream's `operation:`:

```yaml
incremental:
  cursor_field: date
  start: "{{ config.start_date }}"
  window: 30d                      # spCampaigns daily reports span at most 31 days
  lookback: 14d                    # attributed sales restate for up to 14 days
requests:
  - name: raw_sp_campaign_report
    async_job:
      submit:
        operation:
          path: /reporting/reports
          method: POST
          headers: {Content-Type: application/vnd.createasyncreportrequest.v3+json}
          json:
            name: "sp_campaigns {{ window.start | date('%Y-%m-%d') }}"
            startDate: "{{ window.start | date('%Y-%m-%d') }}"
            endDate: "{{ window.end | date('%Y-%m-%d') }}"
            configuration:
              adProduct: SPONSORED_PRODUCTS
              reportTypeId: spCampaigns
              groupBy: [campaign]
              columns: [date, campaignId, campaignName, impressions, clicks, cost, purchases7d, sales7d]
              timeUnit: DAILY
              format: GZIP_JSON
      poll:
        operation:
          path: "/reporting/reports/{{ submit.reportId }}"   # GET by default
        every: 30s
        timeout: 3h                # large accounts' reports can take a while
        done_when: {path: status, equals: COMPLETED}
        fail_when: {path: status, equals: FAILED}
      download: {url: "{{ poll.url }}", format: json, compression: gzip}
transform:
  mode: page
  steps:
    - name: sp_campaign_report
      select: |
        SELECT (record->>'date')::DATE                       AS date,
               record->>'campaignId'                         AS campaign_id,
               coalesce((record->>'impressions')::BIGINT, 0) AS impressions,
               coalesce((record->>'clicks')::BIGINT, 0)      AS clicks,
               coalesce((record->>'cost')::DOUBLE, 0)        AS cost
        FROM raw_sp_campaign_report
export:
  sp_campaign_report:
    step: sp_campaign_report
    primary_key: [campaign_id, date]
```

- The download is a presigned URL: it is fetched without the source's headers and token. An empty file (a window
  without traffic) gives 0 rows.
- Other reports: change `reportTypeId` (`spAdvertisedProduct`, `spTargeting`, ...), `groupBy` and `columns`.
- In a DuckDBJob or SparkJob, the same `async_job:` goes on a call step instead of `operation:` (with
  `{{ config.* }}` dates: call steps have no windows).

**Entity sync or reports.** The list endpoints (`/v2/sp/campaigns`, `/v2/sp/adGroups`, `/v2/sp/keywords`) return the
whole profile, each item with its `campaignId`: read each once and join in SQL instead of one request per campaign.
For metrics, one report covers every campaign of the profile.

**Case-sensitive values.** Enums are upper case: `SPONSORED_PRODUCTS`, `DAILY`, `GZIP_JSON`, statuses `COMPLETED`
and `FAILED`.

## DuckDBJob

A complete DuckDBJob that calls this connector (`streamwright duckdb validate` checks it offline; only the account values and the secret are yours):

```yaml
kind: DuckDBJob
version: 1
name: amazon_ads_sp_campaigns
spec:
  config:
    profile_id: {type: string, required: true, description: "the advertising profile"}
    client_id: {type: string, required: true, description: "the Login with Amazon client id, sent as a header"}
  secrets:
    amazon_client_id: {type: string, required: true}
    amazon_client_secret: {type: string, required: true}
    amazon_refresh_token: {type: string, required: true}

connections:
  amazon_ads_api:
    provider: amazon_ads
    type: oauth2_refresh_token
    token_url: https://api.amazon.com/auth/o2/token
    client_id: "{{ secrets.amazon_client_id }}"
    client_secret: "{{ secrets.amazon_client_secret }}"
    refresh_token: "{{ secrets.amazon_refresh_token }}"
    http:
      base_url: https://advertising-api.amazon.com
      headers:
        Amazon-Advertising-API-ClientId: "{{ config.client_id }}"    # headers take no secrets: the public id
        Amazon-Advertising-API-Scope: "{{ config.profile_id }}"
  out:
    provider: files
    roots: [output]

steps:
  - name: sp_campaigns
    connection: amazon_ads_api
    operation:
      path: /v2/sp/campaigns
      method: GET
      paginator: {type: offset, offset_param: startIndex, limit_param: count, page_size: 100}
    policy:
      retry: {codes: [429, 500, 502, 503, 504], max_attempts: 5, backoff: exponential}
    sql: |
      SELECT record->>'campaignId' AS campaign_id,
             record->>'name' AS campaign_name,
             record->>'state' AS state,
             (record->>'dailyBudget')::DOUBLE AS daily_budget
      FROM records

outputs:
  - name: sp_campaigns_file
    connection: out
    step: sp_campaigns
    mode: overwrite
    path: amazon_ads_sp_campaigns_{{ config.profile_id }}.parquet
    format: parquet
```

- **Connection**: the authentication keys, the HTTP defaults in `http: {base_url, headers, params_encoding,
  paginator, records}` and the default `policy: {retry, rate_limit}`. `validate` rejects keys the connector would
  ignore.
- **Step**: `operation: {path, method, params, headers, json, records, paginator}`, plus `policy: {retry,
  rate_limit, timeout, on_error}`. `path` is relative to the connection's `http.base_url`, or an absolute `https://`
  URL. This is a REST API connector: a step's operation is an HTTP request, never a call (`service`, `arguments`:
  an SDK connector's).
- **SQL**: the step's `sql` reads the table `records`, one JSON column `record`; extract with `->>`.

```bash
STREAMWRIGHT_SECRET_AMAZON_CLIENT_ID=... STREAMWRIGHT_SECRET_AMAZON_CLIENT_SECRET=... \
  STREAMWRIGHT_SECRET_AMAZON_REFRESH_TOKEN=... streamwright duckdb run sp_campaigns.yaml \
  --set profile_id=PROFILE_ID --set client_id=CLIENT_ID
```

## Engines

### Execution & Behavior

- **Transport**: `http` (StreamWright HTTP engine).
- **Regional Endpoints**: North America (`https://advertising-api.amazon.com`), Europe (`https://advertising-api-eu.amazon.com`), Far East (`https://advertising-api-fe.amazon.com`).
- **Pagination**: Configured via offset paginator (`startIndex`/`count`).

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`, DuckDBJob call steps) | `streamwright.core.sdk.engines.source` (HttpSourceAdapter) | the engine's HTTP reader runs each request's `operation:` with the source's `http:` defaults: the connector declares the authentication it takes, the source declares the API |
