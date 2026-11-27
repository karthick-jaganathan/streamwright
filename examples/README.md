# Examples

One folder per system, laid out the same way everywhere:

```
<system>/
  README.md                 how to run its jobs
  config/                   templates of what you supply (secrets.yaml.example)
  jobs/<resource>/<action>.yaml        one job per file; its name is <system>_<resource>_<action>
  payloads/<resource>/<action>.json    the rows a job takes with --input (placeholders, no real accounts)
  data/                     sample files a job reads
serve/                      streamwright serve: jobs as a REST API
sources/                    source configurations (kind: source): ads/<network>, readers/<reader>_demo
```

An overlay sits beside its job as `<action>.<environment>.yaml` (`files/jobs/orders/sync.local.yaml`), and
`streamwright validate examples` checks every job, overlay, source and serve.yaml. Nothing in them names a real
account: give yours with `--set customer_id=... --set login_customer_id=...`.

| Example | Shows |
|---|---|
| `files/jobs/orders/sync.yaml` | local file reads, SQL steps, a table merge, a `split_by` export, offline (see `files/README.md`) |
| `deltalake/jobs/orders/merge.yaml` | a Delta Lake merge with schema evolution (`streamwright-deltalake`) |
| `kafka/jobs/order_files/ingest.yaml` | a consume step, dynamic file paths, row indexes, a Delta merge |
| `google_ads/jobs/campaigns/sync.yaml`, `openai_ads/jobs/ad_groups/sync.yaml` | connector call steps (SDK and HTTP) |
| `google_ads/jobs/ad_groups/sync.yaml` | `partitions`: campaigns, then one call per campaign for its ad groups; step `retry.codes` |
| `openai_ads/jobs/ad_group_metrics/sync.yaml` | a step that runs a StreamWright source's stream |
| `google_ads/jobs/conversion_actions/create.yaml` | an apply output (create): existing ones skipped and logged (a step's `log:`), the results logged (the output's `log:`) |
| `google_ads/jobs/conversions/upload.yaml` | an apply output from `--input` rows: one search for the actions named (`partitions` + GAQL `IN`), unknown ones skipped, calls of `chunk_size` rows, rejected rows logged from the results |
| `google_ads/jobs/conversions/upload_folder.yaml` | a folder, file by file, 5 at once (`runtime.fan_out` over a `files` lookup); each file uploaded once (`runtime.state.job_key`) |
| `google_ads/jobs/campaigns/create.yaml`, `update.yaml` | `mutate_campaigns`: create (`op: create`, partial failure) and update (`op: update`, `nulls: skip_field`) |
| `meta_ads/jobs/conversions/send_events.yaml` | an apply output to the Conversions API: events built and hashed in SQL (`sha256`), a call per chunk, rejected rows logged |
| `microsoft_ads/jobs/campaigns/create.yaml`, `conversion_goals/create.yaml` | SOAP `Add*` operations: existing names skipped, dotted columns for nested fields, `@type` for a concrete type, new ids logged |
| `microsoft_ads/jobs/conversions/upload.yaml` | an apply output on a SOAP API (`ApplyOfflineConversions`) from `--input` rows: unknown goals skipped, calls of `chunk_size` rows, rejected rows logged from the results |
| `meta_ads/jobs/campaigns/create.yaml`, `custom_conversions/create.yaml` | Graph API `create_*` edges: a request per row, a rejected row fails alone, JSON text sent as an object |
| `linkedin_ads/jobs/conversions/send_events.yaml` | an apply output of a REST API connector: the operation is a request, `json: {elements: "{{ rows }}"}` sends a chunk per request, emails hashed in SQL |
| `google_data_manager/jobs/conversions/ingest.yaml`, `audiences/ingest.yaml` | Data Manager ingestion: identifiers normalized and hashed in SQL as Google asks, nested fields as STRUCT, LIST or JSON, a request per chunk whose `request_id` each row keeps |
| `google_data_manager/jobs/conversions/ingest_by_action_name.yaml` | two connectors in one job: actions looked up by name with google_ads, each Data Manager event's own `destination` becomes the request's destinations and the event's `destination_references` |
| `google_data_manager/jobs/requests/status.yaml` | an asynchronous outcome read back: a call step for the request status, failed records logged by index, merged into a table |
| `serve/serve.yaml` | the Google Ads jobs over HTTP: API keys, claims, Swagger UI, metrics, workers |

The manifest reference, commands and delivery guarantees are in the packaged DuckDBJob guide:
[`core/src/streamwright/core/jobs/duckdb/guide.md`](../core/src/streamwright/core/jobs/duckdb/guide.md)
(also served by the MCP server, `get_engine_guide DuckDBJob`).
