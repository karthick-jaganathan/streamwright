# DuckDBJob guide

`kind: DuckDBJob` runs a job in an in-process DuckDB: steps build temp views and tables, outputs write them. It
uses the engine-neutral jobs framework of `streamwright.core.jobs` (the one SparkJob uses): the same manifest
loading, typed inputs, checker, JSON schema, state store, batch runner and write rules. A job's `name` (its name in
the state store and in its batch ids) is lowercase letters, digits and `_`, in dotted parts if you like:
`google_ads.upload_conversions` (at most 128 characters). Step, output and connection names have no dots: they name
tables.

## Commands
`streamwright validate|plan|run JOB` route `kind: DuckDBJob` files to this engine (`streamwright duckdb ...` is the
explicit form, with the same options). Validation never needs secret values. Exit codes: 0 success (a run the lock
policy skipped too), 1 invalid job, failed run or lock held, 2 usage or input error.

- `validate JOB [--config F] [--set NAME=VALUE] [--strict] [--graph] [--input FILE|-]`, `plan JOB`: no secrets, no
  state; `--input` also checks a payload against `spec.input`.
- `run JOB [--secrets F] [--set NAME=VALUE] [--input FILE|-] [--summary [FILE]] [--job-key KEY] [--state-lock-policy
  fail|wait|prune] [--state-lock-timeout 5m] [--once|--drain|--available-now|--follow [--interval 10s]]
  [--max-messages N] [--failed-job-policy abandon|retry|block] [--max-job-attempts N] [--apply-state-migrations]
  [--dry-run]`: unfinished batches of the run's scope resume first (a failed one is abandoned, retried or stops the
  run: see Delivery);
  `--summary` reports `status` `ok`, `skipped` or `failed`; `--job-key` overrides `runtime.state.job_key`. `--set`
  takes lists as `a,b,c`. The batch modes and `--max-messages` are for jobs that consume a broker (see Brokers). A
  run only checks the state tables: create them once with `streamwright state migrate --url URL`, or let the run do
  it with `--apply-state-migrations` (see State, scopes and locks: Migrations).
- `streamwright state ACTION --url URL` (or `$STREAMWRIGHT_STATE_URL`) looks after the batches of every job, from
  its state database alone - no job manifest, no config (see State, scopes and locks: Operating): `list [--job NAME]
  [--scope TEXT] [--status S] [--request-id ID] [--limit N] [--json]`, `show (JOB_ID | --request-id ID)` (with its
  outbox: events and apply rows, their outcomes; `--request-id`: the run's `req=sw_...` of its log lines), `retry |
  abandon JOB_ID`, `resend | skip JOB_ID [--output NAME]` (apply rows with an unknown outcome: see Apply outputs),
  `leases --job NAME`, `purge --job NAME [--older-than 30d] [--scope TEXT]`, `migrations`, `migrate [--dry-run]`,
  `check`.
- Options belong to their command: a source's `--stream` or `--output` given to a job is a usage error (exit 2).
- The live help of every command and option: `streamwright <command> --help` (MCP: `get_cli_reference`, e.g.
  `duckdb run`, `state`, `state list`).

## Manifest
- `spec.config` / `spec.secrets`: typed input definitions (`type`, `items`, `required`, `default`), as in a source,
  with optional constraints `enum`, `pattern` (matched in full), `minimum`, `maximum`, `max_length` (a list input:
  each item), checked on every value given (`--set`, files, lookups).
- `spec.input`: the rows a run is given (`--input`), as the table `input` (see Input payloads).
  Secrets come from `--secrets FILE` or `STREAMWRIGHT_SECRET_<NAME>` and may appear **only in a connection's
  credential keys** (the keys its adapter or connector declares as secret).
- `connections`: `provider` plus that provider's keys. `duckdb` (a database file, attached under the connection's
  name) and `files` (local `roots`) are built in; other providers need their connector's DuckDB adapter
  (`pip install streamwright-s3`, `-gcs`, `-azure-blob`, `-postgres`, `-mysql`, `-deltalake`). A connection only a
  call step uses needs the connector itself. Credentials go into per-connection, per-root scoped DuckDB secrets.
- `steps` (one namespace with outputs; order comes from the SQL the parser reads and `{{ step.column }}`
  references):
  - `sql`: DuckDB SQL over earlier steps (`materialize: view | table`). Tables in a `duckdb` connection are
    qualified (`wh.main.orders`) or read with a `read: table` step.
  - `read`: a connection and `read:` (files: `parquet | csv | json | auto` with `path`, `options`, `schema_mode`,
    `row_index`, `columns`, `column_groups`, `path_pattern`, `vocabulary`, `on_unmapped_column`; databases:
    `table` or `query`). `path` may be `{{ step.column }}`. Planned reads pin their file list in the batch.
  - call: a connection and an `operation:` its connector runs (a call `{service, method, arguments}`, or an HTTP
    request `{path, ...}`) or an `async_job:` (a report job), optional `partitions: [{from, field, name}]` (crossed) and `sql` over the table `records` (column `record`); see Call
    steps.
  - `source` + `stream`: a StreamWright source folder (relative to the job's folder) and one of its streams.
  - consume: a broker connection (`kafka`, `sqs`) with `operation: {service: message, method: consume}` (see Brokers).
  - `log:` (any step): log its rows each batch, on the `streamwright.steps` logger with the step's log context:
    `message` (required; `{column}` placeholders of the step's columns - a dotted name such as `{campaign.name}` is one column - NULL as `null`), `level`
    (`debug | info | warning`, default `info`), `max_rows` (1..1000, default 20). One line per row,
    `step NAME: MESSAGE`, then `step NAME: ... and N more row(s)`; none for zero rows. The engine reads
    `LIMIT max_rows + 1` (a `COUNT(*)` only when there are more); a view step runs its query again. Lines are
    redacted and cut to 1000 characters; a placeholder the step's result lacks fails the step. `streamwright serve`
    logs only `step NAME: N row(s) (row values are not logged by streamwright serve)`.
    ```yaml
    - name: skipped_actions
      sql: SELECT i.name, e.resource_name FROM input AS i JOIN existing_actions AS e USING (name)
      log: {message: "conversion action {name} already exists ({resource_name}): not created", max_rows: 50}
    ```
- `outputs`: `connection`, `step` or `sql`, and a required `mode` (publish outputs take none: see Brokers; apply
  outputs neither: see Apply outputs). What else is allowed depends on the writer:
  tables (`append | overwrite | merge`, `table`, `primary_key`), files (`overwrite | error`, `path`, `format`:
  `csv | tsv | json | jsonl | parquet`; `path` is the file, or with a `file_name` template the folder: `file_name`
  (and `layout`, its folders, e.g. `{today:%Y/%m}`) names one file, or with `split_by` one file per split (see File
  names); or
  `partition_by` for Hive folders), Delta
  (`append | overwrite | merge` with `replace_where` or `whole_table`, `condition`, `when_*`, `target_predicate`,
  `schema_evolution`). Modes are the same words on every engine. A key the writer does not read is an error;
  modes that are not safe to replay (table `append`, files `error`) are warnings.
- `runtime`: `threads`, `memory_limit`, `temp_directory`, `timeout_seconds` (per batch), `limits`
  (`reference_values`, `read_groups`, `events`, `apply_rows`) and `state`: `{connection: NAME, schema: streamwright}` (a
  `duckdb`, `postgres` or `mysql` connection) or `{sqlite: PATH}`, plus `ttl_seconds`, `job_key` (a batch with that
  key runs once per scope), `failed_job_policy` and `max_job_attempts` (see Delivery), `apply_migrations` (below),
  `scope` and `lock` (below).

## State, scopes and locks
`runtime.state` (`sqlite: PATH`, or `connection: NAME` of a `duckdb`, `postgres` or `mysql` connection, with
`schema`, default `streamwright`) keeps every run as a batch in the `sw_*` tables: `sw_jobs` (status, pinned manifest,
lock tokens, rows written, duration, attempts), `sw_sequences`, `sw_leases`, `sw_receipts`, `sw_events`, `sw_signatures`
(created and upgraded by migrations, below); tables of an earlier release are imported with their batches and store
identity. Without `runtime.state` a run keeps nothing (an in-memory store),
and scopes, locks and `--state-lock-*` do not apply.

**Migrations.** A run, and every state action, only checks the state tables: while a migration is pending (or the
tables do not exist yet) it fails (exit 1, `the state tables need migrations: ...`), so the schema changes only when
you apply it: `streamwright state migrate --url URL` as a deployment step, and `streamwright state check --url URL`
(exit 1 while one is pending) as its gate. For development, `run --apply-state-migrations` or
`runtime.state.apply_migrations: true` (e.g. in a local overlay) let the run create or upgrade them itself. An
in-memory state (no `runtime.state`, or `sqlite: ":memory:"`) is always created.

**Which to use**
- One run at a time per job: nothing to add (the scope is `""`).
- The same job for many clients (one run per client, clients in parallel): `scope: "client:{{ config.client }}"`.
- Clients that share resources (an account two clients may both list): add `lock.each` over the list input, so two
  runs never process the same item at once.
- One resource every run of the job needs, whatever its scope (a shared report): `lock: {token: report}` (policy
  `fail` or `wait`). Lease tokens belong to the job name: locks never span two jobs.

```yaml
spec:
  config:
    client:   {type: string}
    accounts: {type: list, items: string}      # --set accounts=111,222
runtime:
  state:
    sqlite: state.db
    scope: "client:{{ config.client }}"
    lock:
      each: "{{ config.accounts }}"            # exactly one list input (a comma text input works too)
      as: account                               # the item's name in token (default: item)
      token: "account:{{ account }}"
      policy: prune                             # fail (default) | wait | prune
      timeout: 5m                               # policy wait only: seconds or 30s, 5m, 1h
```

**What a run does**
- Its lease holds the scope's token (`scope:<scope>`), `lock.token` if any, and one token per `lock.each` item; it
  is renewed while the run lasts and fences every output write. One run per scope is active.
- Everything it reads back is its scope's: unfinished batches (resumed first), a failed batch (abandoned, retried or
  stopping the scope, not other scopes: see Delivery), `job_key`, carried bookmarks, batch sequences and the outbox.
- When a token is held: `fail` stops (exit 1, `... lease is held: account:111 (held by HOST:PID:ID until TIME)`);
  `wait` retries with backoff until `timeout`, then fails; `prune` drops the held items (the list input, and every
  query that renders it, is narrowed to the free ones; logged as a WARNING) and runs the rest. A prune run left with
  no item, or whose scope is held, does nothing, is recorded as `skipped` and exits 0.
- A batch pins its inputs in its manifest: `inputs.scope`, `inputs.config` (the values, narrowed; never secrets) and
  `inputs.tokens`. Resuming or retrying it takes the same tokens and runs with those values, even if defaults (dates)
  changed. Batch ids are `<job>-<sequence>` and `<job>-<scope hash>-<sequence>` with a scope.
- Delta outputs commit `txnAppId` `<job>:<output>:<store id>[:<scope>]` with `txnVersion` = the batch sequence: a
  replayed batch is never written twice, and scopes appending to one table never collide.

**Outputs shared by scopes.** Scopes run side by side, so an output every scope writes must not overwrite the
others: put the scope's value in the path or file name (`path: "exports/{{ config.client }}"`, `file_name:
"{client_id}_{today}.tsv"` with `split_by`, or `{batch_id}`, which carries the scope hash), or append/merge with
the scope's column in the key and `replace_where` / `target_predicate` limited to it.

**Validation** (`validate`, MCP `validate_duckdb_job`) rejects: `each` that is not exactly `{{ config.NAME }}`;
`token` without `{{ <as> }}` when `each` is set; a lock with neither `token` nor `each`; tokens starting with
`scope:`; `policy: prune` without `each`; a bad `timeout`; a `scope` that is not text of at most 185 characters.
`run` rejects `--state-lock-*` without a persistent `runtime.state`, prune without `each`, a timeout with a
policy other than `wait`, and a rendered lock token or job key longer than 191 characters (exit 2). 191 is the
length of the state tables' key columns: it keeps MySQL's composite utf8mb4 primary keys within InnoDB's 3072
bytes; the scope gets 185 because its token is `scope:<scope>`.

**Operating**
`streamwright state ACTION --url URL` works on the state database alone (the URL of `runtime.state`: a sqlite file
is `sqlite:///path` relative to where you run it, a connection is its `postgresql://` or `mysql://` URL, or set
`$STREAMWRIGHT_STATE_URL`): no job manifest, no config, for DuckDBJob and SparkJob alike.
- `list [--job NAME] [--scope client:acme] [--status failed] [--request-id ID] [--limit N] [--json]`: job, job id,
  status (`received`, `processing`, `committed`, `acked`, `completed`, `failed`, `abandoned`, `skipped`), attempts,
  scope, the run's request id, update time, rows written (WRITTEN), outbox rows by status (OUTBOX) and error, newest
  first; `--job NAME --scope S --limit 1` is a scope's newest batch.
- `show JOB_ID`: the batch with that job id, as JSON (`job`, `job_id`, `sequence`, `scope`, ...): its pinned manifest
  (`inputs`), lock tokens, rows, duration, output receipts and outbox; `show --request-id sw_...`: the batches of
  that run, as a JSON list.
- `retry JOB_ID`: it resumes from the status it failed in at the job's next run, with the values it pinned (run the
  job to resume it); `abandon JOB_ID`: the batch is given up; its scope runs new batches again.
- `leases --job NAME`: every token (`scope:<scope>`, lock tokens) with owner, epoch, expiry and whether it is held.
- `purge --job NAME --older-than 30d [--scope TEXT]`: deletes terminal batches, their receipts and outbox rows;
  sequences are kept.
- `migrations` / `migrate [--dry-run]` / `check`: the tables' migrations, applied or pending; apply them; exit 1
  while one is pending.
- retry, abandon, resend and skip take the batch's tokens, so they fail while a run of its scope holds them. A
  batch's job id is `<job>-<sequence>` (`<job>-<scope hash>-<sequence>` with a scope); `--job NAME` picks one when
  two jobs share a job id.

## Call steps
A call step runs a connector call on the driver, once per partition, and loads the records into a temp table in
batches, as JSON. A `source` + `stream` step loads its records the same way.

- **The `records` table.** With `sql`, the step is that query over the table `records`, which has **one column,
  `record` (JSON)**: one row per record, as the connector returned it. Without `sql`, the records' top-level keys
  become the step's columns. `records` exists only inside the step's own `sql`.
- **Partitions.** `partitions: [{from, field, name}]` calls once per distinct, non-null value of `field` in the
  earlier step `from` (several partitions are crossed); the call's arguments read the value as
  `{{ partition.NAME }}` (`name` defaults to `field`). The values are pinned in the batch, so a replay makes the same
  calls. **Partition values are not columns of `records`**: to keep one, request it in the call (e.g. select
  `campaign.id` in the query) and extract it from `record`.
- **Extract scalars with `->>`.** `record->>'$.campaign.id'` returns VARCHAR; cast it for other types
  (`(record->>'$.metrics.clicks')::BIGINT`). Dot notation (`record.campaign.id`) and `->` keep the value as JSON,
  so a CSV or TSV output writes it as a quoted JSON string (`"""1234567890"""`), while Parquet hides the issue.

```yaml
- name: ad_groups
  connection: google_ads_api
  operation:
    service: GoogleAdsService
    method: search_stream
    arguments:
      customer_id: "{{ config.customer_id }}"
      query: {gaql: {select: [campaign.id, ad_group.id, ad_group.name], from: ad_group,
                     where: [{field: campaign.id, op: "=", type: int, value: "{{ partition.campaign_id }}"}]}}
  partitions: [{from: campaigns, field: campaign_id}]
  sql: |
    SELECT record->>'$.campaign.id'               AS campaign_id,
           record->>'$.ad_group.id'               AS ad_group_id,
           record->>'$.ad_group.name'             AS ad_group_name
    FROM records
```

A REST API connector (an `http.request` connector: restapi, openai_ads, ...) takes an HTTP request as its
`operation:` - `{path, method, params, headers, json, records, paginator}` - instead of a call. Its connection holds the authentication keys, the HTTP defaults in `http: {base_url, headers,
params_encoding, paginator, records}` and the default `policy: {retry, rate_limit}`; the request's `path` is relative
to `http.base_url`, or an absolute URL. Each REST connector's guide has a complete job.

```yaml
- name: campaigns
  connection: ads_api            # provider: openai_ads, type: bearer, token, base_url: https://api.ads.openai.com
  operation:
    path: /v1/campaigns
    params: {account_id: "{{ config.account_id }}", limit: 100}
    records: {path: data}        # where the records are in each response
    paginator: {type: cursor, token_path: last_id, param: after, has_more_path: has_more}
  policy:
    retry: {codes: [429, 500, 502, 503, 504], max_attempts: 5, backoff: exponential}
  sql: SELECT record->>'id' AS campaign_id, record->>'name' AS campaign_name FROM records
```

### Async reports

A call step may take `async_job:` instead of `operation:` (one or the other, not both) to run a report job: the
same grammar as a source request's `async_job` - `submit`, `poll` (`operation`, `every`, `timeout`, `done_when`,
`fail_when`) and exactly one of `results` or `download`. Each part's `operation:` is what the connector takes: a
call for SDK connectors (Microsoft Ads, Meta Ads, ...), an HTTP request for REST connectors (Amazon Ads, restapi,
...); `poll.operation.service` defaults to the submit's. The records land in `records` like a call step's.

```yaml
- name: report
  connection: ads                  # provider: microsoft_ads
  async_job:
    submit:
      operation:
        service: ReportingService
        method: SubmitGenerateReport
        arguments:
          request:
            ReportRequest:
              "@type": CampaignPerformanceReportRequest
              Aggregation: Daily
              Format: Csv
              ExcludeReportHeader: true
              ExcludeReportFooter: true
              Columns: [TimePeriod, AccountId, CampaignId, CampaignName, Impressions, Clicks, Spend]
              Scope: {AccountIds: ["{{ config.account_id }}"]}
              Time: {PredefinedTime: Yesterday}
    poll:
      operation: {method: PollGenerateReport, arguments: {request: {ReportRequestId: "{{ submit.result }}"}}}
      every: 15s
      timeout: 30m
      done_when: {path: Status, equals: Success}
      fail_when: {path: Status, equals: Error}
    download: {url: "{{ poll.ReportDownloadUrl }}", format: csv, compression: zip}
  sql: SELECT record->>'CampaignId' AS campaign_id, (record->>'Impressions')::BIGINT AS impressions FROM records
```

- `{{ submit.* }}` / `{{ poll.* }}` are the responses (a response that is not a mapping is `{{ submit.result }}`).
  The download URL is fetched without the connection's credentials; an empty URL or file gives no records.
- Call steps have no incremental windows (`{{ window.* }}` is source-only): set the range with `{{ config.* }}`, a
  relative range of the API (`PredefinedTime`, `date_preset`) or step partitions.
- Inline or a source step: inline suits a one-off, self-contained job. Use a source stream (and a `source` +
  `stream` step) when the report is reused, read incrementally (windows, lookback, bookmarks) or shared by several
  jobs.

## File names
A file output's `path` is the file; with `file_name` it is the folder, and `file_name` names the file inside it
(`layout` adds subfolders). With `split_by: [col_a, col_b]` the output writes one file per distinct combination,
and both templates may use `{col_a}` and `{col_b}`; without `split_by` they name the one file.

| Placeholder | Renders |
|---|---|
| `{today}`, `{today:%Y-%m-%d}` | the batch's UTC date (default format `%Y%m%d`) |
| `{now}`, `{batch_ts}`, `{now:%Y%m%d%H%M%S}` | the batch's UTC time (default format `%Y%m%dT%H%M%SZ`) |
| `{today-1d:%Y%m%d}`, `{now+2h:%H}` | a time with offsets: `+`/`-` N then `s m h d w`, combinable (`-1d+6h`) |
| `{batch_id}` | the batch's job id (no offset or format) |
| `{seq}` | the split's 5-digit number, `00001`, splits ordered by their values (no offset or format) |
| `{col}`, `{col:%Y%m}` | a `split_by` column's value, reduced to `[A-Za-z0-9._-]`; with a format (or offset), a date, timestamp or ISO text column |

- Formats are strftime directives, rendered in UTC and English: `Y y m d e H M S I j u w U G V a A b B p Z z s F %`;
  `%-d` drops zero padding. `%T` is not supported (colons are unsafe in names): use `%H%M%S`.
- `today`, `now` and `batch_ts` are the batch's time, pinned when the batch starts, so a replay renders the same
  names and overwrites the same files.
- `file_name` may not contain `/`; `layout` may (`{today:%Y/%m}`, `client={client_id}`). Literal text may use letters,
  digits and `. _ - = , @ +`; `..` is never allowed.
- `file_name` needs at least one placeholder; for a fixed name, set the file as `path` and leave `file_name` out.
  With `split_by`, two splits that render the same name fail the run: use a split column or `{seq}`.

```yaml
outputs:
  - name: daily_export
    step: campaign_stats
    connection: exports
    mode: overwrite
    path: reports
    format: tsv
    split_by: [customer_id]
    layout: "{today:%Y/%m}"
    file_name: "campaigns_{customer_id}_{today-1d:%Y%m%d}.tsv"
```

## Brokers: consume and publish
A job consumes a topic or queue with one consume step and publishes events with publish outputs, on the Kafka and SQS
connectors' DuckDB adapters (`pip install streamwright-kafka`, `streamwright-sqs`); the shapes are SparkJob's.

```yaml
connections:
  bus: {provider: kafka, bootstrap_servers: "{{ config.brokers }}"}
  wh: {provider: duckdb, path: warehouse.duckdb}
steps:
  - name: orders
    connection: bus
    operation:
      service: message
      method: consume
      arguments: {destination: orders, group_id: orders-loader, from: earliest, max_wait: 5s}
    sql: |                       # optional: over the table `input`; default SELECT * FROM input
      SELECT message_id, key, json_extract_string(body, '$.id') AS order_id, headers['source'] AS origin
      FROM input
outputs:
  - {name: save, connection: wh, step: orders, table: orders, mode: merge, primary_key: [order_id]}
  - name: loaded
    connection: bus
    operation:
      service: message
      method: publish
      arguments: {destination: orders-loaded, key: order_id}
    sql: SELECT order_id, origin FROM orders
```

- `input`: the batch's messages: `message_id`, `key`, `body` (text), `headers` (`MAP(VARCHAR, VARCHAR)`),
  `timestamp`. A job has at most one consume step; its `operation.arguments` are the connector's (Kafka: `destination`,
  `group_id`, `from`, `options`; SQS: the queue URL as `destination`, `visibility_timeout`), plus `max_messages` and
  `max_wait`.
- A batch: poll, then run the steps, write the outputs (each with its receipt), compute the publish outputs into the
  batch's outbox, commit, then acknowledge the messages (Kafka commits the offsets, SQS deletes them) and relay the
  outbox. A batch that fails is not acknowledged: its pinned messages are processed again by `streamwright state retry`
  or the next run, and Kafka redelivers them to the group. Delivery is at least once; an event's dedupe key is
  `job:batch:output` (Kafka: each event's `dedupe-key` header, `job:batch:output:N`).
- A publish output takes `sql` or `step`, and `operation.arguments` (`destination`; Kafka `key`: the column that keys each
  event; SQS `group_id`); no `mode`, `table` or `path`. Each row is one event (a JSON object), at most
  `runtime.limits.events` (10000) per output and batch. After the writes, `_commits` (output, table_name, version,
  rows, batch_id) and `_file_errors` (step, path, reason) describe them, for events about what was written.
- Modes: `--once` (default) one batch; `--drain` batches until a poll finds nothing; `--available-now` batches until
  the messages there at the start are processed (Kafka end offsets; SQS runs as `--drain`); `--follow` polls until
  interrupted (exit 130), pausing `--interval` (10s) after an empty poll. `--max-messages N` (500) caps a batch; the
  step's `max_messages` wins; `0` only resumes unfinished batches and relays the outbox. A job without a consume step
  refuses the modes other than `--once` (each batch would be new work). `runtime.timeout_seconds` bounds each batch.

## Apply outputs
An apply output sends its rows to a connector's **apply operation** - a row each: create or update objects, send
conversions or events - and keeps every row's outcome. It names the operation like a call: a connection of a
connector (not a DuckDB adapter) and `operation: {service, method, arguments}`; `apply:` says how the rows are sent
and judged, `policy:` how the calls retry. `streamwright connectors NAME` and the connector's guide list its apply
operations and their arguments.

```yaml
outputs:
  - name: upload_conversions
    connection: ads_api                       # e.g. provider: google_ads
    step: valid_conversions                   # or sql: each row is one operation's payload
    operation:
      service: ConversionUploadService
      method: upload_click_conversions
      arguments: {customer_id: "{{ config.customer_id }}"}
    apply:
      chunk_size: 2000                          # rows per call; default and maximum: the operation's max_chunk
      on_row_error: fail                        # fail (default): a rejected row makes the run exit 1 | continue
      nulls: error                              # error (default) | skip_field | clear
      on_unknown: hold                          # hold (default) | reconcile
      allow_remove: false                       # rows may use the operation's destructive actions (e.g. op: remove)
    policy:
      retry: {max_attempts: 5, backoff: exponential}
      rate_limit: {requests: 5, per: 1s}
    log:                                        # once the rows are sent: a line per row of the SELECT
      sql: |
        SELECT gclid, outcome.error_code AS code, outcome.error_message AS error
        FROM upload_conversions WHERE outcome.status = 'failed'
      message: "gclid {gclid} rejected: {code}: {error}"
      level: warning                            # debug | info (default) | warning; max_rows: 20 (1..1000)
```

- **Rows.** Each column is a field of the vendor's object (its guide says which, e.g. `campaign.name`,
  `network_settings.target_google_search`); a column `op` picks the row's action when the operation has several
  (create, update, remove; default the first). A row whose `op` is not an action, a destructive action without
  `allow_remove: true`, or a NULL under `nulls: error` fails alone (`INVALID_ACTION`, `REMOVE_NOT_ALLOWED`,
  `NULL_VALUE`) and is never sent. `skip_field` leaves NULL fields out; `clear` sends them as "clear this field".
- **Delivery (at least once, honest).** The rows are committed to the batch's outbox with the batch, with the
  rendered `arguments` pinned; then, chunk by chunk, each chunk is marked in flight before its call and its rows'
  outcomes (the vendor's result, or an error code and message) are stored after it. A row the vendor rejects fails
  alone; a call the vendor rejects as a whole fails its rows. A completed batch is never applied again.
- **Unknown outcomes.** A call cut short (the run stopped mid-call, or a transient error outlasted the retries)
  leaves its rows in flight. An idempotent operation (one the vendor deduplicates, e.g. conversion uploads) sends
  them again at the next run. Otherwise `on_unknown: hold` stops the run (exit 1) until you check the vendor and run
  `streamwright state resend --url URL JOB_ID` (send them again: they may be applied twice) or
  `streamwright state skip --url URL JOB_ID` (mark them failed, `UNKNOWN_OUTCOME`); `reconcile` asks the connector what exists, where it can. A non-idempotent
  operation's call is sent once (`policy.retry` does not apply to it): a call that timed out may have been applied,
  so it is never resent within the run.
- **Failed rows.** Under `on_row_error: fail`, a rejected row fails its batch (exit 1); the next run deals with it
  as `runtime.state.failed_job_policy` says. `abandon` (default) sends nothing again. `retry` sends again only the
  batch's `failed` rows of its `on_row_error: fail` outputs (never an applied row; in-flight rows keep the rules
  above; rows skipped as `UNKNOWN_OUTCOME` stay failed); a row keeps its attempts and last error until then.
  Rejected again, the batch fails again, retried while `max_job_attempts` allows. `block` waits for
  `streamwright state retry` (which sends every failed row of the batch again, skipped rows excepted) or
  `streamwright state abandon`.
- **Caps.** `runtime.limits.apply_rows` (100000) caps the rows of one output and batch: a runaway join fails the
  batch before any call. `streamwright state show --url URL JOB_ID` lists every row's status, attempts, result and
  error.
- **`log:` (apply outputs only).** Once a run has sent a batch's rows of the output, `output NAME: MESSAGE` per row
  of `sql` (one SELECT; default: every row) on `streamwright.output`, up to `max_rows`. `sql` reads a table named
  after the output: its columns, and `outcome` (`status` applied|failed, `error_code`, `error_message`, `result`
  JSON of what the vendor returned, `row` from 1). `{outcome.status}`, `{outcome.result.resource_name}` reach into
  it; `{{ batch.id }}` is the batch. It runs in an in-memory DuckDB with only the results (no files, no network); a
  `log:` that cannot run is a warning, never a failure; under serve it logs only the number of rows.
- **REST API connectors** (those that declare `http.send`: restapi and the HTTP ads connectors) take a request
  instead of a call: `operation: {path, method, params, headers, json, records}` with the connection's base URL,
  headers and auth. `method` is POST (default), PUT, PATCH or DELETE; PUT and DELETE are idempotent (retried per
  `policy.retry`), POST and PATCH are sent once. Each row is one request whose body is the row (without `op`), or
  `json` rendered with `{{ row }}` / `{{ row.<column> }}` (also in `path`, `params`, `headers`, e.g.
  `/campaigns/{{ row.id }}`); a 4xx fails only its row (error code: the status). `json` with `{{ rows }}` (e.g.
  `{elements: "{{ rows }}"}`) sends the chunk's rows in one request instead, every row getting the call's outcome
  (with `records: {path}` a list, item n for row n). A request cut short keeps the rows sent before it; the rest stay
  in flight. The output's `policy` defaults to the connection's.
- **Dry runs** are the vendor's, as an argument of its operation where it has one (google_ads: `validate_only:
  true` checks every row and changes nothing): a job that wants one declares a config input and passes it on.
- The run's recap line says `NAME 3 applied, 2 rejected` for an apply output (`NAME N rows` for a table or file);
  the `--summary` and serve's run result give it `applied` and `rejected` (and `rows`, the applied rows, as
  before); a log line per batch says `applied: ... rows=N, failed=M`. `streamwright state list`
  shows a batch's rows by status in `OUTBOX` (`3 applied, 2 failed`); `WRITTEN` counts only table and file
  rows.

## Input payloads
A job may take its rows from the caller instead of reading them: `spec.input` declares them, `--input FILE` (or `-`
for stdin; a JSON array of objects or NDJSON) gives them, and the steps read them as the table `input` (the table a
consume step fills: a job has one or the other).

```yaml
spec:
  input:
    columns:                                  # typed like spec.config inputs, not required unless `required: true`
      gclid: {type: string, required: true}
      conversion_value: {type: number, minimum: 0}
      currency_code: {type: string, enum: [USD, EUR]}
    max_rows: 10000                           # default 10000
    max_bytes: 104857600                      # default 100 MB, checked before parsing
    unknown_columns: reject                   # reject (default) | drop
    retain: until_complete                    # until_complete (default) | none
steps:
  - name: valid_conversions
    sql: SELECT * FROM input WHERE conversion_value > 0
```

- The payload is read and checked before a batch exists: a row that does not fit (an unknown or missing column, a
  type, a constraint, a repeated key, nesting deeper than 32) fails the run with every problem listed (exit 2). The
  columns are loaded with their declared types (string VARCHAR, integer BIGINT, number DOUBLE, boolean BOOLEAN, date
  DATE, list `T[]`), with no inference and no file.
- The rows are pinned in the batch, so a retry reads the same rows, and removed from the state store when the batch
  completes (only their SHA-256 and count stay). `retain: none` never stores them: a failed batch then cannot be
  resumed (abandon it). Row values are never logged.
- A job with `spec.input` needs `--input` to start new work (`--max-messages 0` only resumes); `--job-key` makes a
  given payload's work run once.

## Serving over HTTP
`streamwright serve SERVE_YAML` is the job CLI as a REST API: `POST /v1/jobs/JOB/runs` with `{"set": {...},
"input": [...]}` is `duckdb run --set ... --input ...`; `streamwright worker SERVE_YAML --processes N` runs what it
queues, N at once (sw_requests in the state store, created at start; `pip install "streamwright[serve]"`).
`streamwright queue purge SERVE_YAML --older-than 30d` deletes ended runs (cron); `queue migrate` upgrades the tables.
`sudo streamwright systemd install` writes the systemd units of serve, worker and a daily purge (`/etc/systemd/system`).

```yaml
# serve.yaml: nothing is reachable unless it is listed
bind: 127.0.0.1:8080                        # a non-loopback address needs `auth`
state: {url: "postgresql://sw:{{ secrets.state_password }}@pg/streamwright"}   # optional on loopback: queue.db
# or state: {connection: {provider: postgres, host: pg, database: streamwright, user: sw,
#                         password: "{{ secrets.state_password }}"}, schema: sw}
metrics: {enabled: true}                    # Prometheus text at /metrics (an authenticated caller's jobs)
auth:
  api_keys: [{id: crm_prod, sha256_env: SW_KEY_CRM_SHA256, claims: {tenant: t1}}]   # digests only
  jwt: {issuer: https://idp.example.com, audience: streamwright, jwks_url: https://idp.example.com/jwks}
jobs:
  client_feed_upload:
    manifest: jobs/client_feed_upload.yaml
    principals: [crm_prod]                  # API key ids or JWT subjects
    set: {tenant_id: {from_claim: tenant}}  # bound (or {value: ...}); every other spec.config input is settable
    payload: {max_bytes: 5MB}
    server_options: {fan_out_concurrency: 4}   # the operator's run flags; never a request's
    limits: {concurrency: 4, rate: {requests: 10, per: 1m}, timeout: 2h, wait_seconds: 30}
    on_busy: reject                         # a run of the same scope is active: 409, or queue
```

- **Never settable by a request:** inputs bound in serve.yaml, inputs with a `lookup:` (a value would skip the
  lookup), secrets. A request body has only `set`, `input` and `options.async`; anything else is `422`.
- **Refused at start:** a settable text input used in SQL (`sql`, a read `query`, `arguments.query`, a lookup
  `query`) needs an `enum` or `pattern` of letters, digits and `_ - . : @ +`; one used in a connection, `path`,
  `file_name`, `layout`, `table`, `arguments.path`/`destination` or a lookup `command`/`secret_id`, letters, digits
  and `_ - : @ + = ,` (no `/ . * ? [ { %`). Numbers, booleans and dates are always safe.
- **Idempotency-Key** (required for jobs with apply outputs) is per caller and job, bound to the body (another body:
  `422`), and is the run's job key: a run a worker takes over resumes its batches.
- A worker claims one run per scope at once, renews its claim, runs it in a child process (killed past
  `limits.timeout`); a dead worker's runs are taken over after its claim (a minute) ends. Callers see an error code;
  the cause is in the worker's log and the run's row. Endpoints: `GET /v1/jobs`, `POST /v1/jobs/JOB/runs|validate`,
  `GET /v1/jobs/JOB/plan|status`, `GET /v1/runs/ID`, `/healthz`, `/readyz`, `/openapi.json`, `/metrics`. Several
  hosts behind a load balancer share one `state` database; `limits.rate` and `concurrency` hold across them.

## Lookups and accounts
Inputs can be fetched at run time instead of given up front: `spec.lookups` declares where values come from, and an
input binds one with `lookup: {from: NAME, field: PATH}` (in `spec.config` or `spec.secrets`). An explicit value
(`--set`, `--config`, `--secrets`, `STREAMWRIGHT_SECRET_*`) wins, and a lookup nothing needs never runs.
`runtime.fan_out` runs the whole job once per item (an account): one manifest serves one account, several, or every
account of a tenant.

```yaml
spec:
  config:
    tenant_id:   {type: string, required: false}
    account_ids: {type: list, items: string, required: false}
    account_id:         {type: string, lookup: {from: account, field: account_id}}   # the pass's row
    login_customer_id:  {type: string, lookup: {from: account, field: manager_id}}
  secrets:
    client_secret: {type: string, lookup: {from: app_secrets, field: credentials.clientSecret}}   # once per run
    refresh_token: {type: string, lookup: {from: account, field: refresh_token}}             # per account
  lookups:
    accounts:
      type: database                       # database | command | function | aws_secretsmanager
      connection: creds_db                 # a postgres, mysql, duckdb or sqlite connection of the job
      query: |
        SELECT account_id, manager_id, refresh_token_enc FROM accounts
        WHERE status = 'active' AND (CAST(:tenant_id AS text) IS NULL OR tenant_id = :tenant_id)
          AND (CAST(:account_ids AS text) IS NULL OR account_id IN :account_ids)
      params: {tenant_id: "{{ config.tenant_id }}", account_ids: "{{ config.account_ids }}"}
      rows: many                           # one (default: exactly one row) | many (for fan_out)
      secret_fields: [refresh_token_enc]
      transforms:                          # per row, when its pass starts: function | command (stdin) | builtin
        refresh_token: {type: function, function: "acme.crypto:decrypt",
                        arguments: {ciphertext: "{{ row.refresh_token_enc }}"}}
    app_secrets:
      type: command                        # an argument list, never a shell
      command: [secrets-cli, read, ads/app]
      format: auto                         # auto | text | json | json_string
runtime:
  fan_out: {over: accounts, as: account, concurrency: 4, on_failure: continue, on_empty: fail}
  state:
    scope: "account:{{ config.account_id }}"     # required with fan_out: each account keeps its own state
```
- Run it with `--set account_ids=a1` (one), `--set account_ids=a1,a2` (several) or `--set tenant_id=t1`
  (the tenant's accounts). Command and function lookups and transforms need `--allow-lookup-exec`
  (exit 2 without it). `--fan-out-concurrency N` and `--fan-out-on-failure continue|stop` override the manifest.
- Templates in a lookup read `{{ config.* }}`, `{{ secrets.* }}`, the item's `{{ <as>.COLUMN }}`, and in transforms
  `{{ row.COLUMN }}`. In a query, `:name` parameters are bound: after IN a list expands to (?, ?, ...) and a missing
  one is (NULL); elsewhere a missing one is NULL (cast it where the database must know its type:
  `CAST(:ids AS text) IS NULL` on PostgreSQL). `field` is a dot path (`credentials.clientId`, `items[0].id`) and
  goes through text holding JSON.
- A lookup that reads the item runs once per pass; the others once per run. Every value bound to a secret,
  `secret_fields` column and transform input and output is masked (logs, errors, summaries, state). A retry looks
  the credentials up again.
- A pass is a complete run in its own scope (`failed_job_policy`, attempts, leases per account): one account's
  failure does not stop the others (`on_failure: stop` starts no new pass). Exit 0 when every pass succeeds, else 1;
  a WARNING line per failed pass, and `--summary` lists `passes` (item, status, job ids, outputs, error).
- `validate` checks what a binding names, that `over` is a `rows: many` lookup or a list input, and that the scope
  names the item. A connection only lookups use needs no engine adapter. State actions run only the lookups the
  state connection needs.

## Log context and labels
Inside a job run every line says whom it is about, so runs and `runtime.fan_out` passes that run side by side can be
told apart. Text lines carry a tag after the logger name: the job, its `runtime.log.labels`, the fan-out item, the
step, output or lookup at work, and the run's request id (`req=`). JSON lines carry each key (`job`, `labels`, `item`,
`scope`, `job_id`, `step`, `output`, `lookup`, `request_id`), for every logger, SDK lines included:

```
[...] INFO streamwright.source [ads_campaigns tenant=t1 region=eu account=a1 step=campaigns req=sw_2Lw...]: ...
[...] INFO streamwright.duckdb [ads_campaigns tenant=t1 region=eu account=a1 output=save req=sw_2Lw...]: ...
```

Every run gets a new request id (`sw_` and 22 URL-safe characters), the same on all its lines, passes and threads,
and kept in its batches' metadata: grep one id for one run, and `streamwright state show --url URL --request-id ID`
shows the batches it made.

```yaml
runtime:
  log:
    labels:                           # shown in this order; values use {{ config.* }} only (never secrets)
      tenant: "{{ config.tenant_id }}"
      region: "{{ config.region }}"
```
Labels are rendered when a run (or a fan-out pass, after its lookups) starts; one whose input has no value is left
out. At most 8; names are lowercase letters, digits and `_`, and not one of the built-in keys. Outside a job run
(`validate`, `state`, a source) lines have no tag.
With runtime.fan_out, passes that write into one DuckDB database file run one at a time (a WARNING says so):
DuckDB attaches a database file once per process.

## Delivery
Every run takes its lease (its scope's and lock tokens; renewed while it runs; every output write is fenced by it),
resumes its scope's unfinished batches first and skips outputs that already have a receipt. Delta writes commit an
application transaction, so a write whose receipt was lost is not repeated.

**A failed batch** is dealt with by its scope's next run, before new work, as `runtime.state.failed_job_policy`
says (`--failed-job-policy` overrides it for one run):
- `abandon` (default): the run gives it up (status `abandoned`, its error kept; a WARNING `abandoned: job_id=...,
  reason=failed job policy abandon, error=...`) and runs new work. What the batch did not write is not written, and a
  consumed batch's pinned messages are not acknowledged by it. An abandoned batch keeps its `job_key`, so later runs
  with that key do nothing (the WARNING says so): use `retry` for keyed jobs. Right for snapshot jobs, where the
  next run's data replaces the lost batch's.
- `retry`: the run resumes it from the status it failed in, with the inputs it pinned (config values, tokens,
  messages), skipping the outputs that have a receipt, then runs new work. This repeats while the batch has been
  started fewer than `runtime.state.max_job_attempts` times (default 3, the first run included; `--max-job-attempts
  N`; only with `retry`), each retry a WARNING (`retry: job_id=..., attempt=2 of 3, resumes_from=processing,
  error=...`); then the run stops as with `block`. Right for incremental and broker jobs, where every batch must land.
- `block`: the run stops (exit 1, `batch ... failed earlier and is not retried (failed job policy block): ...`) until
  `streamwright state retry --url URL JOB_ID` (allowed whatever the attempts) or `streamwright state abandon --url URL
  JOB_ID`. Right where a person must look at every failure.
- Other scopes keep running either way. `streamwright state list` shows each batch's ATTEMPTS.

## Execution policy
Every step and output takes the one execution-policy vocabulary in its `policy:` group (connector contract v1.2 §6a),
with the same keys and defaults as a source stream's `policy:` and a SparkJob's:

- `retry {max_attempts, backoff, max_delay, codes}` — a transient read or write is retried through the one shared
  retry. On a call step the connector's own retries apply; on a `read` step or an output a transient I/O error is
  retried.
- `rate_limit {requests, per}` and `page_size` — throttle and page a **call step** (`page_size` also bounds the batch
  size records load in; an HTTP step's `paginator` is in its `operation:`).
- `timeout` (seconds, or a duration like `30s`) — bounds each call, read or write.
- `on_error: fail | skip | warn` — `fail` (default) fails the batch; `skip`/`warn` tolerate a failed step (an empty
  placeholder table lets dependents run) or output (a skipped receipt lets the batch complete), `warn` only logging.

A key an engine does not honour on that surface is an `unsupported-argument` error naming the engine (for example
`rate_limit` on an output), never silently ignored.
