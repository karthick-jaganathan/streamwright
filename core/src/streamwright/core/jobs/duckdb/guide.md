# DuckDBJob guide

`kind: DuckDBJob` runs a job in an in-process DuckDB: steps build temp views and tables, outputs write them. It
uses the engine-neutral jobs framework of `streamwright.core.jobs` (the one SparkJob uses): the same manifest
loading, typed inputs, checker, JSON schema, state store, batch runner and write rules.

## Commands
`streamwright validate|plan|run JOB` route `kind: DuckDBJob` files to this engine (`streamwright duckdb ...` also
works); `streamwright duckdb state list|retry|abandon JOB [--batch ID]` manages batches. Validation never needs
secret values. Exit codes: 0 success, 1 invalid job or failed run, 2 usage or input error.

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
- `outputs`: `connection`, `step` or `sql`, and a required `mode`. What else is allowed depends on the writer:
  tables (`append | overwrite | merge`, `table`, `primary_key`), files (`overwrite | error`, `path`, `format`:
  `csv | tsv | json | jsonl | parquet`; `path` is the file, or with a `file_name` template the folder: `file_name`
  (and `layout`, its folders, e.g. `{today:%Y/%m}`) names one file, or with `split_by` one file per split (see File
  names); or
  `partition_by` for Hive folders), Delta
  (`append | overwrite | merge` with `replace_where` or `whole_table`, `condition`, `when_*`, `target_predicate`,
  `schema_evolution`). Modes are the same words on every engine. A key the writer does not read is an error;
  modes that are not safe to replay (table `append`, files `error`) are warnings.
- `runtime`: `threads`, `memory_limit`, `temp_directory`, `timeout_seconds`, `limits` (`reference_values`,
  `read_groups`) and `state`: `{connection: NAME, schema: streamwright}` (a `duckdb` or `postgres` connection) or
  `{sqlite: PATH}`, plus `ttl_seconds` and `run_key` (a batch with that key runs once).

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
| `{batch_id}` | the batch id (no offset or format) |
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

## Delivery
Every run takes the job's lease (renewed while it runs; every output write is fenced by it), resumes unfinished
batches first and skips outputs that already have a receipt. Delta writes commit an application transaction, so a
write whose receipt was lost is not repeated. A failed batch stops the job until `state retry` (it resumes from the
status it failed in) or `state abandon`.

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
