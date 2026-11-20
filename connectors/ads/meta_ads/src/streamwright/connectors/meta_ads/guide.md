# Meta Ads (`meta_ads`)

Meta Marketing API campaigns, ad sets, ads, and insights extraction; creates, updates and deletes of objects, and
Conversions API events (apply outputs).

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
- Security: Sandboxed SDK client. Requests and call steps only read (`api_get` and `get_*` edge methods); writes
  (`create_*`, `api_update`, `api_delete`, `AdsPixel.create_event`) run only as job apply outputs. The
  object (service) and method are resolved against the installed `facebook_business` library, which is the authority
  on which objects, edges and fields exist; the connector never enumerates them (contract v1.3 §6b). A typo is caught
  before a run, with a "did you mean" suggestion drawn from the SDK.

### Vendor arguments

Any argument that is not a declared one (`id`, `fields`, `params`, `is_async`) is a Graph API parameter forwarded
verbatim into the request's `params` (e.g. `breakdowns`, `time_increment`, `action_breakdowns`, `filtering`):

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
    operation:
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

## Performance reports

For short ranges, read the `get_insights` edge directly (above). For many ad sets or ads, or long ranges, run an
async insights report: `is_async: true` on a `get_insights` read starts the report and returns its report run
`{id, report_run_id}`; poll the `AdReportRun`, then read its rows. Only reads that run as a report take `is_async`
(others fail with an error).

```yaml
partitions:
  - {name: account_id, values: "{{ config.account_ids }}"}
incremental: {cursor_field: date, start: "{{ config.start_date }}", window: 28d, lookback: 28d}
requests:
  - name: raw_ad_set_insights_report
    async_job:
      submit:
        operation:
          service: AdAccount
          method: get_insights
          arguments:
            id: "act_{{ partition.account_id }}"
            is_async: true
            fields: [account_id, campaign_id, adset_id, adset_name, date_start, impressions, clicks, spend]
            params:
              level: adset
              time_increment: 1
              time_range: {since: "{{ window.start }}", until: "{{ window.end }}"}
      poll:
        operation:
          service: AdReportRun     # the report run, not the account (service defaults to the submit's)
          method: api_get
          arguments: {id: "{{ submit.id }}", fields: [async_status, async_percent_completion]}
        every: 15s
        timeout: 1h
        done_when: {path: async_status, equals: Job Completed}
        fail_when: {path: async_status, in: [Job Failed, Job Skipped]}
      results:
        operation:
          service: AdReportRun
          method: get_insights     # the report's rows, paged by the connector
          arguments: {id: "{{ submit.id }}", params: {limit: 500}}
transform:
  mode: page
  steps:
    - name: ad_set_insights_report
      select: |
        SELECT record->>'adset_id'                           AS ad_set_id,
               (record->>'date_start')::DATE                 AS date,
               coalesce((record->>'impressions')::BIGINT, 0) AS impressions,
               coalesce((record->>'spend')::DOUBLE, 0)       AS spend
        FROM raw_ad_set_insights_report
export:
  ad_set_insights_report:
    step: ad_set_insights_report
    primary_key: [ad_set_id, date]
```

In a DuckDBJob or SparkJob, the same `async_job:` goes on a call step instead of `operation:` (with
`{{ config.* }}` dates: call steps have no windows).

**Entity sync or reports.** Read entities from the account's edges (`AdAccount.get_campaigns`,
`AdAccount.get_ad_sets`, `AdAccount.get_ads`), partitioned by account, not from each campaign's edges partitioned by
campaign (one call per campaign). For metrics, one account-level `get_insights` with `level: adset` (or `ad`) covers
every object in the account.

**Case-sensitive values.** Graph API enums are lower case: `level: adset`, `date_preset: last_30d`,
`time_increment: 1`. Report run statuses are as Meta sends them: `Job Completed`, `Job Failed`, `Job Skipped`.

## Apply outputs (writes)

A DuckDBJob or SparkJob output can write to Meta, a row per object or event (the engine guide's Apply outputs section
has the policies: chunks, unknown outcomes, `state resend|skip`):

- `*.create_*` (any edge: `AdAccount.create_campaign`, `AdAccount.create_ad_set`, `Campaign.create_ad_set`, ...): a
  request per row, its columns the new object's parameters. Arguments: `id` (required, the parent, e.g.
  `act_1234567890`) and `params` (every row's parameters; a row's columns win). Not idempotent: a chunk cut short is
  held for you (`on_unknown`). At most 100 rows a chunk. Result: the new object's `id`.
- `*.api_update`: a request per row; the `id` column names the object, the other columns are the parameters to
  change (argument `params` as above). `*.api_delete`: a request per row by `id` (`op: remove`, needs
  `allow_remove: true`). Both idempotent; at most 100 rows a chunk. A row without `id` fails as `INVALID_ROW`.
- `AdsPixel.create_event` (the Conversions API): one request per chunk of up to 1000 events, `data` the rows.
  Arguments: `id` (required, the pixel or dataset id) and `params` (e.g. `test_event_code`). Columns are the server
  event's fields (`event_name`, `event_time` in Unix seconds, `action_source`, `event_id`, `user_data`,
  `custom_data`, ...). Meta deduplicates on `event_name` + `event_id`, so a chunk cut short is sent again. Result per
  row: the call's `events_received` and `fbtrace_id`; a rejected call fails all its rows.
- Values: structs and lists are sent as JSON; a text value that is a JSON object (`'{"em": ["<sha256>"]}'`) is sent
  as the object; NULLs (also inside structs) are not sent, so use `nulls: skip_field`; an empty `params` value is
  left out (an optional config input can fill it). Cast DECIMALs to DOUBLE. Hash `user_data` in SQL
  (`sha256(lower(trim(email)))`).
- Errors: a row Meta rejects fails alone, with its Graph API code (`100`, ...) and message (and Meta's
  `error_user_msg`); a throttled or temporary error (the `retry_codes`, transient, 5xx) cuts the chunk short: the
  rows already sent keep their results and the rest stay in flight.

```yaml
outputs:
  - name: send
    connection: meta
    step: events
    operation:
      service: AdsPixel
      method: create_event
      arguments:
        id: "{{ config.pixel_id }}"
        params: {test_event_code: "{{ config.test_event_code }}"}   # empty: left out
    apply:
      nulls: skip_field
      on_row_error: continue
```

## Engines

### Execution & Behavior

- **Transport**: `sdk` (`facebook_business`).
- **Services & Methods**: resolved dynamically against the installed `facebook_business` library (contract v1.3 §6b).
  Reads are the operation families `*.api_get` (read any object) and `*.get_*` (read any of its edges); writes are
  `*.create_*`, `*.api_update`, `*.api_delete` and `AdsPixel.create_event` (apply outputs, above); which objects (`AdAccount`, `Campaign`, `Ad`, ...), edges and fields exist is whatever the installed SDK exposes.
- **Streaming**: Yields rows one Graph API page at a time (`params.limit` sets the page size); each page is one request, logged as `page N`.
- **Throttling**: backs off as the `x-business-use-case-usage` headers ask. An error carries its Graph API code and
  subcode; the spec's `retry_codes` (throttling and temporary codes such as `4`, `17`, `613`, `80004`) are retried by
  default, as are errors Meta flags transient and HTTP 5xx. A stream's `retry: {codes: [...]}` replaces that list.

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`, DuckDBJob call steps and apply outputs) | `engines/source.py` | the `facebook_business` SDK; object reads and graph edges, paged by the connector; writes |
