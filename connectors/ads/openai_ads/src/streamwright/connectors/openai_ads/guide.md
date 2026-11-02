# OpenAI Ads (`openai_ads`)

OpenAI Advertising API performance and metadata extraction.

Connects to the OpenAI Ads HTTP API using bearer token authentication.
Inherits the StreamWright HTTP engine with cursor pagination, rate-limit retries, and DuckDB SQL transformations.

## Connection

### `source.yaml` contract

```yaml
kind: source
name: openai_ads
spec:
  config:
    account_ids: {type: list, items: string, description: "Ad account IDs to sync"}
  secrets:
    openai_ads_key: {type: string, required: true}

auth:
  provider: openai_ads
  type: bearer
  token: "{{ secrets.openai_ads_key }}"

http:
  base_url: https://api.ads.openai.com
  paginator: {type: cursor, token_path: last_id, param: after, has_more_path: has_more}
  records: {path: data}
  retry: {codes: [429, 500, 502, 503, 504], max_attempts: 5, backoff: exponential}
```

### Authentication & Security

- `provider`: `openai_ads`
- `type`: `bearer`
- Credentials: `token` must be a `{{ secrets.* }}` reference. Credentials are never written in source configs and are redacted from all logs.
- Security: Sandboxed HTTP transport restricted to read requests against OpenAI Ads endpoints.

## Operations

The connector offers one operation, `http.request`: each `requests:` item of a stream is an `http:` request, run by the
engine's HTTP reader with the source's `http:` defaults (base URL, headers, paginator, records, retry, rate limit).

### `streams/<stream>.yaml` contract

```yaml
# The campaigns of each account in config.account_ids.
partitions:
  - {name: account_id, values: "{{ config.account_ids }}"}
requests:
  - name: raw_campaigns
    http:
      path: /v1/campaigns
      method: GET
      params:
        account_id: "{{ partition.account_id }}"
        limit: 100

transform:
  mode: page
  steps:
    - name: final_campaigns
      select: |
        SELECT 
          record->>'id' AS campaign_id,
          record->>'name' AS campaign_name,
          record->>'status' AS status,
          (record->>'daily_budget')::DOUBLE AS daily_budget,
          record->>'created_at' AS created_at
        FROM raw_campaigns

export:
  campaigns:
    step: final_campaigns
    primary_key: [campaign_id]
```

### Transform & Data Shaping

- Emits records as extracted JSON items from the `records.path` response envelope (`data`).
- In `transform` steps, raw JSON records are accessible via DuckDB JSON operators (`record->>'field'`).

### Vendor arguments

Any key in an `http` request's `params` block is a query parameter forwarded verbatim to the API; the paths and
parameters you call are whatever the API exposes, never a closed list in the connector (contract v1.3 §6b). For
example `params: {statusFilter: ENABLED}` reaches the request URL unchanged; pagination cursors the paginator
injects take precedence.

## Engines

### Execution & Behavior

- **Transport**: `http` (StreamWright HTTP engine).
- **Pagination**: Configured via cursor paginator (`last_id` -> `after`).
- **Retries**: Configured for 429 rate limits and 5xx server errors with exponential backoff.

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`, DuckDBJob call steps) | `streamwright.core.io.http` | the engine's HTTP reader runs the source's `http:` requests: the connector declares the authentication it takes, the source declares the API |
