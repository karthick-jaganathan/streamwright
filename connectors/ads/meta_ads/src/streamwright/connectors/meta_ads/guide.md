# Meta Ads (`meta_ads`)

Meta Marketing API campaigns, ad sets, ads, and insights extraction.

Connects to Meta Marketing API using official `facebook_business` SDK.
Supports object reads and graph edge traversals (`get_campaigns`, `get_insights`) with automatic pagination and throttling handling.

## Connection

### `source.yaml` contract

```yaml
kind: source
name: meta_ads_pipeline
spec:
  config:
    account_ids: {type: list, items: string, description: Ad account IDs without the act_ prefix}
    start_date: {type: date, default: "-30d"}
  secrets:
    meta_access_token: {type: string, required: true}

auth:
  provider: meta_ads
  access_token: "{{ secrets.meta_access_token }}" # System-user or long-lived user token
  # Optional:
  # app_id: "123456789"
  # app_secret: "{{ secrets.meta_app_secret }}"  # Enables appsecret_proof (declare meta_app_secret in spec.secrets)
  # api_version: "v21.0"
```

### Authentication & Security

- `provider`: `meta_ads`
- Credentials: `access_token` and optional `app_secret` must be `{{ secrets.* }}` references. Credentials and appsecret proofs are never written in source configs and are redacted from all logs.
- Security: Sandboxed SDK client. Only read-only operations (`api_get` and `get_*` edge methods) are permitted. The
  object (service) and method are resolved against the installed `facebook_business` library, which is the authority
  on which objects, edges and fields exist; the connector never enumerates them (contract v1.3 §6b). A typo is caught
  before a run, with a "did you mean" suggestion drawn from the SDK.

### Vendor arguments

Any argument that is not a declared one (`id`, `fields`, `params`) is a Graph API parameter forwarded verbatim into
the request's `params` (e.g. `breakdowns`, `time_increment`, `action_breakdowns`, `filtering`):

```yaml
arguments:
  id: "act_1234567890"
  fields: ["impressions", "spend"]
  breakdowns: ["age", "gender"]   # a vendor Graph API parameter, forwarded as-is
```

## Operations

### `streams/<stream>.yaml` contract

```yaml
# One row per campaign per day, for each account in config.account_ids.
partitions:
  - {name: account_id, values: "{{ config.account_ids }}"}
incremental:
  cursor_field: date
  start: "{{ config.start_date }}"
  window: 7d
  lookback: 28d                    # attribution windows restate results for up to 28 days
requests:
  # An edge of an object (here the account's insights); paged by the connector
  - name: raw_campaign_insights
    sdk: meta_ads
    service: AdAccount
    method: get_insights
    arguments:
      id: "act_{{ partition.account_id }}"
      fields: [account_id, campaign_id, campaign_name, date_start, impressions, clicks, spend]
      params:
        level: campaign
        time_increment: 1
        time_range: {since: "{{ window.start }}", until: "{{ window.end }}"}
  # One object's fields: service Campaign, method api_get, arguments {id, fields}

transform:
  mode: page
  steps:
    - name: campaign_insights
      select: |
        SELECT record->>'account_id'                         AS account_id,
               record->>'campaign_id'                        AS campaign_id,
               record->>'campaign_name'                      AS campaign_name,
               (record->>'date_start')::DATE                 AS date,
               coalesce((record->>'impressions')::BIGINT, 0) AS impressions,
               coalesce((record->>'clicks')::BIGINT, 0)      AS clicks,
               coalesce((record->>'spend')::DOUBLE, 0)       AS spend
        FROM raw_campaign_insights

export:
  campaign_insights:
    step: campaign_insights
    primary_key: [campaign_id, date]
```

### Transform & Data Shaping

- Emits records as JSON dictionaries representing API entity fields. Numeric metric fields (e.g. `spend`, `clicks`) are returned as strings by Meta API and should be cast in SQL transforms.
- A transform reads each record from the request's table as the column `record` (and the partition's values as
  `partition`): extract values with `->>` (`record->>'campaign_id'`) and cast the numbers, which the API sends as
  text.

## Engines

### Execution & Behavior

- **Transport**: `sdk` (`facebook_business`).
- **Services & Methods**: resolved dynamically against the installed `facebook_business` library (contract v1.3 §6b).
  The read-only policy is the operation families `*.api_get` (read any object) and `*.get_*` (read any of its edges);
  which objects (`AdAccount`, `Campaign`, `Ad`, ...), edges and fields exist is whatever the installed SDK exposes.
- **Streaming**: Yields rows in cursor pages of up to 500 records.
- **Throttling**: backs off as the `x-business-use-case-usage` headers ask. An error carries its Graph API code and
  subcode; the spec's `retry_codes` (throttling and temporary codes such as `4`, `17`, `613`, `80004`) are retried by
  default, as are errors Meta flags transient and HTTP 5xx. A stream's `retry: {codes: [...]}` replaces that list.

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`, DuckDBJob call steps) | `engines/source.py` | the `facebook_business` SDK; object reads and graph edges, paged by the connector |
