# DuckDBJob guide

`kind: DuckDBJob` runs a job in an in-process DuckDB: steps build temp views and tables, outputs write them. It
uses the engine-neutral jobs framework of `streamwright.core.jobs` (the one SparkJob uses): the same manifest
loading, typed inputs, checker, JSON schema, state store, batch runner and write rules.

## Commands
`streamwright validate|plan|run JOB` route `kind: DuckDBJob` files to this engine (`streamwright duckdb ...` is the
explicit form, with the same options). Validation never needs secret values. Exit codes: 0 success (a run the lock
policy skipped too), 1 invalid job, failed run or lock held, 2 usage or input error.

- `validate JOB [--config F] [--set NAME=VALUE] [--strict] [--graph]`, `plan JOB`: no secrets, no state.
- `run JOB [--secrets F] [--set NAME=VALUE] [--summary [FILE]] [--job-key KEY] [--state-lock-policy
  fail|wait|prune] [--state-lock-timeout 5m] [--once|--drain|--available-now|--follow [--interval 10s]]
  [--max-messages N] [--failed-job-policy abandon|retry|block] [--max-job-attempts N] [--apply-state-migrations]
  [--dry-run]`: unfinished batches of the run's scope resume first (a failed one is abandoned, retried or stops the
  run: see Delivery);
  `--summary` reports `status` `ok`, `skipped` or `failed`; `--job-key` overrides `runtime.state.job_key`. `--set`
  takes lists as `a,b,c`. The batch modes and `--max-messages` are for jobs that consume a broker (see Brokers). A
  run only checks the state tables: create them once with `streamwright duckdb state migrate JOB`, or let the run do
  it with `--apply-state-migrations` (see State, scopes and locks: Migrations).
- `streamwright duckdb state ACTION JOB [--overlay F] [--config F] [--set ...] [--secrets F]`: `list [--limit N] [--scope TEXT] [--json]`, `status`,
  `show JOB_ID`, `retry JOB_ID [--no-run]`, `abandon JOB_ID`, `leases [--json]`, `purge [--older-than 30d] [--scope
  TEXT]`, `migrate [--dry-run]`, `check` (see State, scopes and locks); `streamwright state migrate|show|check --url
  URL`: the state tables of a database URL.
- Options belong to their command: a source's `--stream` or `--output` given to a job, `--strict` or `--log-level`
  to a state action other than `retry` (which runs the job and takes its `--allow-connector` and logging options) is
  a usage error (exit 2).
- The live help of every command and option: `streamwright <command> --help` (MCP: `get_cli_reference`, e.g.
  `duckdb run`, `duckdb state`, `state migrate`).

## Manifest
- `spec.config` / `spec.secrets`: typed input definitions (`type`, `items`, `required`, `default`), as in a source.
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
  - call: a connection and a connector call (`service`/`method`/`arguments`, or `request`), optional
    `partitions: [{from, field, name}]` (crossed) and `sql` over the table `records` (column `record`); see Call
    steps.
  - `source` + `stream`: a StreamWright source folder (relative to the job's folder) and one of its streams.
  - consume: a broker connection (`kafka`, `sqs`) with `service: message`, `method: consume` (see Brokers).
- `outputs`: `connection`, `step` or `sql`, and a required `mode` (publish outputs take none: see Brokers). What else is allowed depends on the writer:
  tables (`append | overwrite | merge`, `table`, `primary_key`), files (`overwrite | error`, `path`, `format`:
  `csv | tsv | json | jsonl | parquet`; `path` is the file, or with a `file_name` template the folder: `file_name`
  (and `layout`, its folders, e.g. `{today:%Y/%m}`) names one file, or with `split_by` one file per split (see File
  names); or
  `partition_by` for Hive folders), Delta
  (`append | overwrite | merge` with `replace_where` or `whole_table`, `condition`, `when_*`, `target_predicate`,
  `schema_evolution`). Modes are the same words on every engine. A key the writer does not read is an error;
  modes that are not safe to replay (table `append`, files `error`) are warnings.
- `runtime`: `threads`, `memory_limit`, `temp_directory`, `timeout_seconds` (per batch), `limits`
  (`reference_values`, `read_groups`, `events`) and `state`: `{connection: NAME, schema: streamwright}` (a
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
you apply it: `streamwright duckdb state migrate JOB` (or `streamwright state migrate --url URL`) as a deployment
step, and `state check` (exit 1 while one is pending) as its gate. For development, `run --apply-state-migrations` or
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
Every `streamwright duckdb state ACTION JOB` takes the job's required config values (`--set`, `--config`), as a run
does; they select the scope for `status` and `retry`. Only the secrets of the state connection are needed (`retry`,
which then runs the job, needs them all).
- `list [--scope client:acme] [--limit N] [--json]`: id, status (`received`, `processing`, `committed`, `acked`,
  `completed`, `failed`, `abandoned`, `skipped`), sequence, scope, update time, rows and error, newest first, of all
  scopes unless `--scope`; `status`: the newest batch of the job's scope.
- `show JOB_ID`: the batch with that job id, as JSON (`job_id`, `sequence`, `scope`, ...): its pinned manifest (`inputs`), lock tokens, rows, duration and output receipts.
- `retry JOB_ID --set client=acme ...`: give the config values of the batch's scope (another scope is refused); it
  resumes from the status it failed in, then the job runs its unfinished batches (`--no-run`: only mark it).
- `abandon JOB_ID`: the batch is given up; its scope runs new batches again.
- `leases`: every token (`scope:<scope>`, lock tokens) with owner, epoch, expiry and whether it is held.
- `purge --older-than 30d [--scope TEXT]`: deletes terminal batches and their receipts; sequences are kept.
- `migrate [--dry-run]` / `check`: create or upgrade the job's state tables, or exit 1 while one is pending.
- retry and abandon take the batch's tokens, so they fail while a run of its scope holds them. A batch's job id
  is `<job>-<sequence>` (`<job>-<scope hash>-<sequence>` with a scope).

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
  so a CSV or TSV output writes it as a quoted JSON string (`"""18497872503"""`), while Parquet hides the issue.

```yaml
- name: ad_groups
  connection: google_ads_api
  service: GoogleAdsService
  method: search_stream
  partitions: [{from: campaigns, field: campaign_id}]
  arguments:
    customer_id: "{{ config.customer_id }}"
    query: {gaql: {select: [campaign.id, ad_group.id, ad_group.name], from: ad_group,
                   where: [{field: campaign.id, op: "=", type: int, value: "{{ partition.campaign_id }}"}]}}
  sql: |
    SELECT record->>'$.campaign.id'               AS campaign_id,
           record->>'$.ad_group.id'               AS ad_group_id,
           record->>'$.ad_group.name'             AS ad_group_name
    FROM records
```

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
    service: message
    method: publish
    arguments: {destination: orders-loaded, key: order_id}
    sql: SELECT order_id, origin FROM orders
```

- `input`: the batch's messages: `message_id`, `key`, `body` (text), `headers` (`MAP(VARCHAR, VARCHAR)`),
  `timestamp`. A job has at most one consume step; its `arguments` are the connector's (Kafka: `destination`,
  `group_id`, `from`, `options`; SQS: the queue URL as `destination`, `visibility_timeout`), plus `max_messages` and
  `max_wait`.
- A batch: poll, then run the steps, write the outputs (each with its receipt), compute the publish outputs into the
  batch's outbox, commit, then acknowledge the messages (Kafka commits the offsets, SQS deletes them) and relay the
  outbox. A batch that fails is not acknowledged: its pinned messages are processed again by `state retry` or the
  next run, and Kafka redelivers them to the group. Delivery is at least once; an event's dedupe key is
  `job:batch:output` (Kafka: each event's `dedupe-key` header, `job:batch:output:N`).
- A publish output takes `sql` or `step`, and `arguments` (`destination`; Kafka `key`: the column that keys each
  event; SQS `group_id`); no `mode`, `table` or `path`. Each row is one event (a JSON object), at most
  `runtime.limits.events` (10000) per output and batch. After the writes, `_commits` (output, table_name, version,
  rows, batch_id) and `_file_errors` (step, path, reason) describe them, for events about what was written.
- Modes: `--once` (default) one batch; `--drain` batches until a poll finds nothing; `--available-now` batches until
  the messages there at the start are processed (Kafka end offsets; SQS runs as `--drain`); `--follow` polls until
  interrupted (exit 130), pausing `--interval` (10s) after an empty poll. `--max-messages N` (500) caps a batch; the
  step's `max_messages` wins; `0` only resumes unfinished batches and relays the outbox. A job without a consume step
  refuses the modes other than `--once` (each batch would be new work). `runtime.timeout_seconds` bounds each batch.

## Lookups and accounts
Inputs can be fetched at run time instead of given up front: `spec.lookups` declares where values come from, and an
input binds one with `lookup: {from: NAME, field: PATH}` (in `spec.config` or `spec.secrets`). An explicit value
(`--set`, `--config`, `--secrets`, `STREAMWRIGHT_SECRET_*`) wins, and a lookup nothing needs never runs.
`runtime.fan_out` runs the whole job once per item (an account): one manifest serves one account, several, or every
account of a client.

```yaml
spec:
  config:
    client_id:   {type: string, required: false}
    account_ids: {type: list, items: string, required: false}
    account_id:         {type: string, lookup: {from: account, field: account_id}}   # the pass's row
    login_customer_id:  {type: string, lookup: {from: account, field: manager_id}}
  secrets:
    client_secret: {type: string, lookup: {from: app_creds, field: credentials.clientSecret}}   # once per run
    refresh_token: {type: string, lookup: {from: account, field: refresh_token}}             # per account
  lookups:
    accounts:
      type: database                       # database | command | function | aws_secretsmanager
      connection: creds_db                 # a postgres, mysql, duckdb or sqlite connection of the job
      query: |
        SELECT account_id, manager_id, refresh_token_enc FROM ads_accounts
        WHERE status = 'active' AND (CAST(:client_id AS text) IS NULL OR client_id = :client_id)
          AND (CAST(:account_ids AS text) IS NULL OR account_id IN :account_ids)
      params: {client_id: "{{ config.client_id }}", account_ids: "{{ config.account_ids }}"}
      rows: many                           # one (default: exactly one row) | many (for fan_out)
      secret_fields: [refresh_token_enc]
      transforms:                          # per row, when its pass starts: function | command (stdin) | builtin
        refresh_token: {type: function, function: "acme.crypto:decrypt",
                        arguments: {ciphertext: "{{ row.refresh_token_enc }}"}}
    app_creds:
      type: command                        # an argument list, never a shell
      command: [get_secret, -s, google-ads/app]
      format: auto                         # auto | text | json | json_string
runtime:
  fan_out: {over: accounts, as: account, concurrency: 4, on_failure: continue, on_empty: fail}
  state:
    scope: "account:{{ config.account_id }}"     # required with fan_out: each account keeps its own state
```
- Run it with `--set account_ids=acc_101` (one), `--set account_ids=acc_101,acc_102` (several) or `--set
  client_id=c42` (the client's accounts). Command and function lookups and transforms need `--allow-lookup-exec`
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
told apart. Text lines carry a tag after the logger name: the job, its `runtime.log.labels`, the fan-out item, and the
step, output or lookup at work. JSON lines carry each key (`job`, `labels`, `item`, `scope`, `job_id`, `step`,
`output`, `lookup`), for every logger, SDK lines included:

```
[...] INFO streamwright.source [ads_campaigns client=c42 network=3 account=acc_101 step=campaigns]: ...
[...] INFO streamwright.duckdb [ads_campaigns client=c42 network=3 account=acc_101 output=save]: ...
```

```yaml
runtime:
  log:
    labels:                           # shown in this order; values use {{ config.* }} only (never secrets)
      client:  "{{ config.client_id }}"
      network: "{{ config.network_id }}"
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
  `state retry JOB JOB_ID` (allowed whatever the attempts) or `state abandon JOB JOB_ID`. Right where a person must
  look at every failure.
- Other scopes keep running either way. `state list` shows each batch's ATTEMPTS.

## Execution policy
Every step and output takes the one execution-policy vocabulary (connector contract v1.2 §6a), with the same keys and
defaults as a source stream and a SparkJob:

- `retry {max_attempts, backoff, max_delay, codes}` — a transient read or write is retried through the one shared
  retry. On a call step the connector's own retries apply; on a `read` step or an output a transient I/O error is
  retried.
- `rate_limit {requests, per}` and `paginator` / `page_size` — throttle and page a **call step** (`page_size` also
  bounds the batch size records load in).
- `timeout` (seconds, or a duration like `30s`) — bounds each call, read or write.
- `on_error: fail | skip | warn` — `fail` (default) fails the batch; `skip`/`warn` tolerate a failed step (an empty
  placeholder table lets dependents run) or output (a skipped receipt lets the batch complete), `warn` only logging.

A key an engine does not honour on that surface is an `unsupported-argument` error naming the engine (for example
`rate_limit` on an output), never silently ignored.
