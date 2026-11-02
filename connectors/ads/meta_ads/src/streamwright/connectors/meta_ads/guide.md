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
  secrets:
    meta_access_token: {type: string, required: true}
    meta_app_secret: {type: string, required: false}

auth:
  provider: meta_ads
  access_token: "{{ secrets.meta_access_token }}" # System-user or long-lived user token
  # Optional:
  # app_id: "123456789"
  # app_secret: "{{ secrets.meta_app_secret }}"  # Enables appsecret_proof verification
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
requests:
  # Method 1: Edge traversal on AdAccount (e.g. insights)
  - name: campaign_insights
    sdk: meta_ads
    service: AdAccount
    method: get_insights
    arguments:
      id: "act_1234567890"             # Account ID prefixed with act_
      fields:
        - "campaign_id"
        - "campaign_name"
        - "impressions"
        - "clicks"
        - "spend"
        - "date_start"
        - "date_stop"
      params:
        level: "campaign"
        time_range:
          since: "{{ window.start }}"
          until: "{{ window.end }}"

  # Method 2: Entity inspection (e.g. read campaign details)
  # - name: campaign_details
  #   sdk: meta_ads
  #   service: Campaign
  #   method: api_get
  #   arguments:
  #     id: "120210000000"
  #     fields: ["id", "name", "objective", "status", "daily_budget"]

transform:
  mode: page
  steps:
    - name: final_insights
      select: |
        SELECT 
          campaign_id::BIGINT AS campaign_id,
          campaign_name,
          impressions::BIGINT AS impressions,
          clicks::BIGINT AS clicks,
          spend::DOUBLE AS spend,
          date_start AS report_date
        FROM campaign_insights

export:
  insights:
    step: final_insights
    primary_key: [campaign_id, report_date]
```

### Transform & Data Shaping

- Emits records as JSON dictionaries representing API entity fields. Numeric metric fields (e.g. `spend`, `clicks`) are returned as strings by Meta API and should be cast in SQL transforms.
- In `transform` steps, request results are available as relational tables in DuckDB SQL.

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
