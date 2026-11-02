# LinkedIn Ads (`linkedin_ads`)

LinkedIn Marketing Solutions API campaigns, creatives, and analytics extraction.

Connects to the LinkedIn REST API using OAuth2 bearer token authentication.
Inherits the StreamWright HTTP engine with offset pagination, dotted query params encoding, and DuckDB SQL transformations.

## Connection

### `source.yaml` contract

```yaml
kind: source
name: linkedin_ads_pipeline
spec:
  config:
    account_ids: {type: list, items: string, description: "Sponsored Ad Account IDs"}
    api_version: {type: string, default: "202401"}
  secrets:
    linkedin_access_token: {type: string, required: true}

auth:
  provider: linkedin_ads
  type: bearer
  token: "{{ secrets.linkedin_access_token }}"

http:
  base_url: https://api.linkedin.com
  headers:
    LinkedIn-Version: "{{ config.api_version }}"
    X-Restli-Protocol-Version: "2.0.0"
  params_encoding: dotted
  paginator:
    type: offset
    offset_param: start
    limit_param: count
    page_size: 100
    total_path: paging.total
  records:
    path: elements
```

### Authentication & Security

- `provider`: `linkedin_ads`
- `type`: `bearer`
- Credentials: `token` must be a `{{ secrets.* }}` reference. Credentials are never written in source configs and are redacted from all logs.
- Security: Sandboxed HTTP transport restricted to read requests against LinkedIn REST API endpoints.

## Operations

The connector offers one operation, `http.request`: each `requests:` item of a stream is an `http:` request, run by the
engine's HTTP reader with the source's `http:` defaults (base URL, headers, paginator, records, retry, rate limit).

### `streams/<stream>.yaml` contract

```yaml
partitions:
  - {name: account_id, values: "{{ config.account_ids }}"}

requests:
  - name: raw_campaigns
    http:
      path: /rest/adCampaigns
      method: GET
      params:
        q: search
        search.account.values[0]: "urn:li:sponsoredAccount:{{ partition.account_id }}"

transform:
  mode: page
  steps:
    - name: final_campaigns
      select: |
        SELECT 
          partition->>'account_id' AS account_id,
          (record->>'id')::BIGINT AS campaign_id,
          record->>'name' AS campaign_name,
          record->>'status' AS status,
          (record->>'$.dailyBudget.amount')::DOUBLE AS daily_budget
        FROM raw_campaigns

export:
  campaigns:
    step: final_campaigns
    primary_key: [account_id, campaign_id]
```

### Transform & Data Shaping

- Emits records as extracted JSON items from the `records.path` response envelope (`elements`).
- In `transform` steps, raw JSON records are accessible via DuckDB JSON operators (`record->>'field'`).

### Vendor arguments

Any key in an `http` request's `params` block is a query parameter forwarded verbatim to the API; the paths and
parameters you call are whatever the API exposes, never a closed list in the connector (contract v1.3 §6b). For
example `params: {statusFilter: ENABLED}` reaches the request URL unchanged; pagination cursors the paginator
injects take precedence.

## Engines

### Execution & Behavior

- **Transport**: `http` (StreamWright HTTP engine).
- **Pagination**: Configured via offset paginator (`start`/`count` bounded by `paging.total`).
- **Encoding**: Uses `params_encoding: dotted` for Rest.li nested URL parameter syntax.

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`, DuckDBJob call steps) | `streamwright.core.io.http` | the engine's HTTP reader runs the source's `http:` requests: the connector declares the authentication it takes, the source declares the API |
