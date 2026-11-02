# Google Ads (`google_ads`)

Google Ads API campaign, ad group, keyword, and metrics extraction.

Connects to Google Ads API using official GoogleAdsClient.
Supports GAQL (Google Ads Query Language) stream queries with parameterization, customer listing, and automatic token management.

## Connection

### `source.yaml` contract

```yaml
kind: source
name: google_ads_pipeline
spec:
  secrets:
    gads_developer_token: {type: string, required: true}
    gads_client_id: {type: string, required: true}
    gads_client_secret: {type: string, required: true}
    gads_refresh_token: {type: string, required: true}

auth:
  provider: google_ads
  developer_token: "{{ secrets.gads_developer_token }}"
  client_id: "{{ secrets.gads_client_id }}"
  client_secret: "{{ secrets.gads_client_secret }}"
  refresh_token: "{{ secrets.gads_refresh_token }}"
  # Optional:
  # login_customer_id: "1234567890"   # Manager account MCC ID
  # api_version: "v18"
```

### Authentication & Security

- `provider`: `google_ads`
- Credentials: `developer_token`, `client_id`, `client_secret`, and `refresh_token` must be `{{ secrets.* }}` references. Credentials are never written in source configs and are redacted from all logs.
- Security: Sandboxed Google Ads API client. The read-only policy is `GoogleAdsService.search`/`search_stream`
  (GAQL) and the `get_*`/`list_*` read methods of any `*Service`; the installed `google-ads` library is the
  authority on which services, methods and request fields exist (contract v1.3 §6b), so a typo is caught before a
  run with a "did you mean" suggestion. Mutations never match the read-only families.

### Vendor arguments

Any argument that is not declared is a request field forwarded verbatim to the Google Ads request (the adapter owns
`customer_id`, `query` and the paging token):

```yaml
arguments:
  customer_id: "{{ partition.customer_id }}"
  query: {gaql: {select: ["campaign.id"], from: "campaign"}}
  summary_row_setting: SUMMARY_ROW_WITH_RESULTS   # a vendor search-request field, forwarded as-is
```

## Operations

### `streams/<stream>.yaml` contract

```yaml
requests:
  # Method 1: GAQL search_stream query
  - name: campaign_performance
    sdk: google_ads
    service: GoogleAdsService
    method: search_stream
    arguments:
      customer_id: "1234567890"        # Or referenced from partition: "{{ partition.customer_id }}"
      query: |
        SELECT
          campaign.id,
          campaign.name,
          campaign.status,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          segments.date
        FROM campaign
        WHERE segments.date >= '{{ window.start }}'
          AND segments.date <= '{{ window.end }}'

  # Method 2: List accessible client accounts under MCC
  # - name: accessible_accounts
  #   sdk: google_ads
  #   service: CustomerService
  #   method: list_accessible_customers

transform:
  mode: page
  steps:
    - name: final_campaigns
      select: |
        SELECT 
          campaign.id::BIGINT AS campaign_id,
          campaign.name AS campaign_name,
          campaign.status AS campaign_status,
          metrics.impressions::BIGINT AS impressions,
          metrics.clicks::BIGINT AS clicks,
          (metrics.cost_micros::DOUBLE / 1000000.0) AS cost,
          segments.date AS report_date
        FROM campaign_performance

export:
  campaigns:
    step: final_campaigns
    primary_key: [campaign_id, report_date]
```

### Transform & Data Shaping

- Emits records as nested JSON dictionaries matching GAQL field paths (e.g. `campaign.id`, `metrics.clicks`).
- In `transform` steps, nested fields can be addressed directly in DuckDB SQL using dot notation or struct unpacking.

## Engines

### Execution & Behavior

- **Transport**: `sdk` (`google-ads`).
- **Services & Methods**: resolved dynamically against the installed `google-ads` library (contract v1.3 §6b).
  - `GoogleAdsService.search_stream` / `search`: Arguments: `customer_id` (string/int, required), `query` (GAQL
    string or query dict, required); any other argument is a request field (vendor argument).
  - `<AnyService>.get_*` / `list_*` (e.g. `CustomerService.list_accessible_customers`): the request fields are vendor
    arguments; the installed library decides which methods exist.
- **Streaming**: Yields rows in gRPC response stream chunks.
- **Retries**: an error carries every Google Ads error name and its gRPC status; by default the spec's `retry_codes`
  (`RESOURCE_EXHAUSTED`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, `ABORTED`, `RESOURCE_TEMPORARILY_EXHAUSTED`,
  `INTERNAL_ERROR`, `TRANSIENT_ERROR`) are retried, honouring the API's retry delay. A stream's
  `retry: {codes: [...]}` replaces that list.

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`, DuckDBJob call steps) | `engines/source.py` | the Google Ads client (`google-ads`); GAQL reads through `GoogleAdsService` and the accessible customers |
