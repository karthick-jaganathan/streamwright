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
  config:
    customer_ids: {type: list, items: string, description: Customer IDs without dashes}
    start_date: {type: date, default: "-30d"}
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
# One row per campaign per day, for each customer in config.customer_ids.
partitions:
  - {name: customer_id, values: "{{ config.customer_ids }}"}
incremental:
  cursor_field: date
  start: "{{ config.start_date }}"
  window: 7d
  lookback: 30d                      # conversions are restated: re-read the last 30 days each run
requests:
  - name: raw_campaign_performance
    sdk: google_ads
    service: GoogleAdsService
    method: search_stream
    arguments:
      customer_id: "{{ partition.customer_id }}"
      query:
        gaql:                        # the query builder (see Query builder); GAQL text also works for fixed queries
          select: [customer.id, campaign.id, campaign.name, campaign.status, segments.date,
                   metrics.impressions, metrics.clicks, metrics.cost_micros]
          from: campaign
          where:
            - {field: segments.date, op: BETWEEN, type: date, value: ["{{ window.start }}", "{{ window.end }}"]}
            - {field: campaign.status, op: IN, type: enum, value: [ENABLED, PAUSED]}

transform:
  mode: page
  steps:
    - name: campaign_performance
      select: |
        SELECT record->>'$.customer.id'                                 AS customer_id,
               record->>'$.campaign.id'                                 AS campaign_id,
               record->>'$.campaign.name'                               AS campaign_name,
               record->>'$.campaign.status'                             AS status,
               (record->>'$.segments.date')::DATE                       AS date,
               coalesce((record->>'$.metrics.impressions')::BIGINT, 0)  AS impressions,
               coalesce((record->>'$.metrics.clicks')::BIGINT, 0)       AS clicks,
               round((record->>'$.metrics.cost_micros')::BIGINT * 0.000001, 2) AS cost
        FROM raw_campaign_performance

export:
  campaign_performance:
    step: campaign_performance
    primary_key: [customer_id, campaign_id, date]
```

Other read methods: `CustomerService.list_accessible_customers` lists the accounts a manager (MCC) login can reach;
any `<Service>.get_*` / `list_*` read method of the installed library works the same way.

### Transform & Data Shaping

- Each record is the API row as nested JSON, matching the GAQL field paths (`campaign.id`, `metrics.clicks`).
- A transform reads it from the request's table as the column `record`: extract values with `->>`
  (`record->>'$.campaign.id'`, which returns text) and cast numbers and dates. IDs are 64-bit: keep them as text or
  BIGINT. Money fields are in micros.

## Query builder

`query` takes either form:

- **`{gaql: {...}}`, the `gaql` query builder** (recommended). It writes the GAQL from a typed mapping, so input
  values are checked and escaped, never pasted into the query text, and a list input fills an `IN` directly. Use it
  whenever the query uses a `config`, partition or window value.
- **GAQL text**, as you would write it in the Google Ads query builder or API docs. The text is sent as written and
  may not contain `{{ }}` references, so it suits fixed queries (GAQL's relative dates, such as
  `segments.date DURING LAST_7_DAYS`, work in it).

```yaml
query: |
  SELECT campaign.id, campaign.name FROM campaign
  WHERE campaign.advertising_channel_type IN ('SEARCH', 'SHOPPING')
```

The builder form, with the channels coming from an input (plus a status and a date filter):

```yaml
query:
  gaql:
    select: [campaign.id, campaign.name, campaign.advertising_channel_type]
    from: campaign
    where:
      - {field: campaign.status, op: "=", type: enum, value: ENABLED}
      - {field: campaign.advertising_channel_type, op: IN, type: enum, value: "{{ config.channels }}",
         skip_if_empty: true}
      - {field: segments.date, op: BETWEEN, type: date, value: ["{{ window.start }}", "{{ window.end }}"]}
    order_by: [campaign.id]          # or "metrics.clicks DESC"
    limit: 100
```

- Keys: `select` (field paths), `from` (one resource), `where` (items joined with AND), `order_by`, `limit`.
- A `where` item: `field`, `op`, `type` and `value`.
  - `op`: `=` `!=` `>` `>=` `<` `<=` `IN` `NOT IN` `LIKE` `NOT LIKE` `BETWEEN` `IS NULL` `IS NOT NULL`
    `CONTAINS ANY` `CONTAINS ALL` `CONTAINS NONE`.
  - `type` says how the value is written: `int` (checked numbers), `string` (quoted and escaped), `enum` (bare
    names, e.g. `SEARCH`), `date` (`YYYY-MM-DD`).
  - `IN`, `NOT IN` and `CONTAINS *` take a list, or a reference to a list input (`"{{ config.channels }}"`, in sources
    and jobs alike); `BETWEEN` takes `[low, high]`; `IS NULL` and `IS NOT NULL` take none.
  - `skip_if_empty: true` drops the item when its value (or a `BETWEEN` bound) is missing or empty.
- In a DuckDBJob or SparkJob call step, the same mapping goes in the step's `arguments.query`; the records reach the
  step's `sql` as `record` JSON (`record->>'$.campaign.id'`).

Grammar: https://developers.google.com/google-ads/api/docs/query/grammar

## Engines

### Execution & Behavior

- **Transport**: `sdk` (`google-ads`).
- **Services & Methods**: resolved dynamically against the installed `google-ads` library (contract v1.3 §6b).
  - `GoogleAdsService.search_stream` / `search`: Arguments: `customer_id` (string/int, required), `query` (a
    `{gaql: {...}}` query builder mapping, or GAQL text; required); any other argument is a request field (vendor
    argument).
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
