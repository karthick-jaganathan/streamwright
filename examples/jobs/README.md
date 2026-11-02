# DuckDBJob examples and reference

`kind: DuckDBJob` runs a job in an in-process DuckDB: steps build temp views and tables, outputs write them. It
uses the engine-neutral jobs framework of `streamwright.core.jobs` (the one SparkJob uses): the same manifest
loading, typed inputs, checker, JSON schema, state store, batch runner and write rules.

Layout: one folder per system, one file per job, named for what it produces: `<system>/<job>.yaml` (the job's
`name:` is `<system>_<job>`). Its overlays sit beside it as `<job>.<environment>.yaml` (e.g. `files/orders.local.yaml`)
and `streamwright validate` checks each one merged onto its job. Run a job by its file:
`streamwright run examples/jobs/files/orders.yaml --overlay examples/jobs/files/orders.local.yaml`.

| Example | Shows |
|---|---|
| `files/orders.yaml` | local file reads, SQL steps, table merge, `split_by` export |
| `deltalake/orders.yaml` | Delta Lake merge with schema evolution (`streamwright-deltalake`) |
| `kafka/order_files.yaml` | a connector call step, dynamic file paths, row indexes, Delta merge |
| `google_ads/campaigns.yaml`, `openai_ads/ad_groups.yaml` | connector call steps (SDK and HTTP) |
| `openai_ads/ad_group_metrics.yaml` | a step that runs a StreamWright source's stream |
| `google_ads/ad_groups.yaml` | `partitions`: campaigns, then one call per campaign for its ad groups; step `retry.codes` |

The manifest reference, commands and delivery guarantees are in the packaged DuckDBJob guide:
[`core/src/streamwright/core/jobs/duckdb/guide.md`](../../core/src/streamwright/core/jobs/duckdb/guide.md)
(also served by the MCP server, `get_engine_guide DuckDBJob`).
