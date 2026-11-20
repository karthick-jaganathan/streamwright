# Microsoft Advertising (`microsoft_ads`)

Microsoft Advertising (Bing Ads) campaign, reporting, and customer extraction.

Connects to Microsoft Advertising API (v13) using SOAP SDK / OAuth2 tokens.
Supports the read-only operations of any service the installed `bingads` WSDL exposes (campaign management, reporting,
customer management, ad insight, ...); the connector never enumerates them (contract v1.3 §6b).

## Connection

### `source.yaml` contract

```yaml
kind: source
name: microsoft_ads_pipeline
spec:
  config:
    account_ids: {type: list, items: string}
  secrets:
    msads_developer_token: {type: string, required: true}
    msads_client_id: {type: string, required: true}
    msads_refresh_token: {type: string, required: true}

auth:
  provider: microsoft_ads
  developer_token: "{{ secrets.msads_developer_token }}"
  client_id: "{{ secrets.msads_client_id }}"
  refresh_token: "{{ secrets.msads_refresh_token }}"
  # Optional:
  # client_secret: "{{ secrets.msads_client_secret }}"   # web apps only; declare it in spec.secrets
  # customer_id: "1234567"
  # account_id: "9876543"
  # environment: "production"          # "production" | "sandbox"
```

### Authentication & Security

- `provider`: `microsoft_ads`
- Credentials: `developer_token`, `client_id`, `refresh_token`, and optional `client_secret` must be `{{ secrets.* }}` references. Credentials are never written in source configs and are redacted from all logs.
- Security: Sandboxed SOAP client. The read-only policy is the `Get*`, `Search*` and `Find*` methods of any
  `*Service`, plus the two report operations; the installed `bingads` WSDL is the authority on which services and
  operations exist (contract v1.3 §6b). An operation the WSDL does not declare fails with a clear error naming it and
  the closest operations.

### Vendor arguments

Any argument that is not declared (`request`, `account_id`, `customer_id`) is a SOAP request field, merged into
`request` (a field the request type lacks fails with a clear error naming it):

```yaml
arguments:
  request:
    AccountId: "{{ partition.account_id }}"
  ReturnAdditionalFields: "ProductTargetingSetting"   # a vendor request field, forwarded into the SOAP request
```

## Operations

### `streams/<stream>.yaml` contract

```yaml
# The campaigns of each account in config.account_ids.
partitions:
  - {name: account_id, values: "{{ config.account_ids }}"}
requests:
  - name: raw_campaigns
    operation:
      service: CampaignManagementService
      method: GetCampaignsByAccountId
      arguments:
        request:                         # the fields of the SOAP request
          AccountId: "{{ partition.account_id }}"
          CampaignType: [Search, Shopping, PerformanceMax]   # without it, only Search
        # account_id / customer_id: the SOAP header's account and customer (defaults: the request's, the connection's)
      records: {path: Campaign}          # one record per campaign in the response
  # Reports: an async_job request (see Performance reports)

transform:
  mode: page
  steps:
    - name: campaigns
      select: |
        SELECT partition->>'account_id'         AS account_id,
               record->>'Id'                    AS campaign_id,
               record->>'Name'                  AS campaign_name,
               record->>'Status'                AS status,
               record->>'CampaignType'          AS campaign_type,
               (record->>'DailyBudget')::DOUBLE AS daily_budget
        FROM raw_campaigns

export:
  campaigns:
    step: campaigns
    primary_key: [account_id, campaign_id]
```

### Transform & Data Shaping

- Emits records as nested JSON dictionaries representing deserialized SOAP responses.
- A transform reads each record from the request's table as the column `record` (and the partition's values as
  `partition`): extract values with `->>` (`record->>'Id'`); `records: {path: Campaign}` makes each item of the
  response's `Campaign` list one record.

## Performance reports

Reports are async: the request is an `async_job` that submits a `ReportRequest`, polls it, then downloads the zipped
CSV. A copy-pasteable stream (one row per campaign per day, a 30-day window per report):

```yaml
partitions:
  - {name: account_id, values: "{{ config.account_ids }}"}
incremental: {cursor_field: date, start: "{{ config.start_date }}", window: 30d, lookback: 30d}
requests:
  - name: raw_campaign_performance
    async_job:
      submit:
        operation:
          service: ReportingService
          method: SubmitGenerateReport
          arguments:
            request:
              ReportRequest:
                "@type": CampaignPerformanceReportRequest   # SOAP type, built by the connector
                Aggregation: Daily
                Format: Csv
                ExcludeReportHeader: true
                ExcludeReportFooter: true
                Columns: [TimePeriod, AccountId, CampaignId, CampaignName, Impressions, Clicks, Spend]
                Scope: {AccountIds: ["{{ partition.account_id }}"]}
                Time: {CustomDateRangeStart: "{{ window.start }}", CustomDateRangeEnd: "{{ window.end }}"}
      poll:
        operation:                 # service defaults to the submit's (ReportingService)
          method: PollGenerateReport
          arguments: {request: {ReportRequestId: "{{ submit.result }}"}}   # the submit returns the bare id
        every: 15s
        timeout: 30m
        done_when: {path: Status, equals: Success}
        fail_when: {path: Status, equals: Error}
      download: {url: "{{ poll.ReportDownloadUrl }}", format: csv, compression: zip}
transform:
  mode: page
  steps:
    - name: campaign_performance
      select: |                    # report values are text: cast them
        SELECT record->>'AccountId'                          AS account_id,
               record->>'CampaignId'                         AS campaign_id,
               record->>'CampaignName'                       AS campaign_name,
               (record->>'TimePeriod')::DATE                 AS date,
               coalesce((record->>'Impressions')::BIGINT, 0) AS impressions,
               coalesce((record->>'Clicks')::BIGINT, 0)      AS clicks,
               coalesce((record->>'Spend')::DOUBLE, 0)       AS spend
        FROM raw_campaign_performance
export:
  campaign_performance:
    step: campaign_performance
    primary_key: [account_id, campaign_id, date]
```

- Other reports: change `"@type"` (`AdGroupPerformanceReportRequest`, `KeywordPerformanceReportRequest`, ...) and
  `Columns`. The download URL is fetched without the connection's credentials; an empty URL (no data in the range)
  gives 0 rows, not an error.
- In a DuckDBJob or SparkJob, the same `async_job:` goes on a call step instead of `operation:` (use
  `{{ config.* }}` or `Time: {PredefinedTime: Yesterday}`: call steps have no windows).

**Entity sync or reports.** `GetAdGroupsByCampaignId` (and other per-campaign reads) partitioned by campaign is one
call per campaign (N+1), slow on large accounts. For metrics, use a report scoped to the account: it covers every
campaign and ad group in one job. For entity settings, partition by account where the API has an account-level read
(`GetCampaignsByAccountId`), and keep per-campaign fan-out for what has none. A campaign without ad groups reads as 0
rows.

**Case-sensitive values.** Enum values are PascalCase as the API spells them: `Aggregation: Daily`,
`Format: Csv`, `PredefinedTime: Last30Days`, `CampaignType: [Search, Shopping]`. `TimePeriod` is not allowed as a
column with `Aggregation: Summary`.

## Engines

### Execution & Behavior

- **Transport**: `sdk` (`bingads`).
- **Services & Methods**: resolved dynamically against the installed `bingads` WSDL (contract v1.3 §6b). The read-only
  policy is the `Get*`, `Search*` and `Find*` methods of any `*Service`, plus `ReportingService.SubmitGenerateReport`
  / `PollGenerateReport`; which services and operations exist is whatever the WSDL declares.
- **Streaming**: Yields responses converted to record dicts.
- **Retries**: an error carries every SOAP error code; the spec's `retry_codes` (`0` InternalError, `117`
  CallRateExceeded, `207` ConcurrentRequestOverLimit, OAuth `temporarily_unavailable`/`server_error`) are retried by
  default, waiting 60s after `117`/`207`. A stream's `retry: {codes: [...]}` replaces that list.

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`, DuckDBJob call steps) | `engines/source.py` | the `bingads` SOAP SDK |
