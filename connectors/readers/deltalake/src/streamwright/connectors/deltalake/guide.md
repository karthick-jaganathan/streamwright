# Delta Lake (`deltalake`)

Delta Lake tables in local folders or object storage (S3 and S3-compatible stores, Google Cloud Storage, Azure Blob
Storage): reads and queries (`streamwright run` sources), reads in DuckDBJob and SparkJob steps, and appends,
overwrites and merges in their outputs. Every engine reads and writes **only inside the connection's `roots`**; a
table path is a URL or an absolute path inside a root, or a path relative to the first root.

## Connection

```yaml
# source.yaml (auth:) or a job's connections.<name>:
provider: deltalake
roots: ["s3://acme-lake/delta/"]             # required: references allowed, e.g. "{{ config.roots }}"
aws_access_key_id: "{{ secrets.aws_key }}"   # credentials: secret references only
aws_secret_access_key: "{{ secrets.aws_secret }}"
aws_region: us-east-1
# SparkJob: the object-store connection whose credentials and settings reach the tables
# storage: lake_s3
```

- `roots` is required: the source engine's DuckDB sandbox is fail-closed (it reads only inside them:
  allowed_directories, external access off, configuration locked), and every path is checked against them first.
- The S3 (`aws_*`, `s3_*`), GCS (`gcs_*`) and Azure (`azure_*`) keys reach DuckDB only as bound parameters of a
  temporary secret scoped to the roots on their scheme, and delta-rs as storage options; credentials are `***` in
  every log line and error (the account key inside an Azure connection string too).
- Spark takes credentials from the object-store connection named by `storage` (s3, gcs, azure_blob, files) and ignores
  the connection's own storage keys; `schema`, `schema_location` and `schema_comment` are the defaults of the tables
  its writes register in the catalog.

## Operations

deltalake offers the shared table operations (`streamwright.core.sdk.operations.tables`) and one operation of its own,
`table.query`; the Engines column says which engine takes an argument.

- **`table.read`** reads a table: its `columns`, the rows every `where` condition holds for, at most `limit` rows.
  On Spark the version read is pinned in the batch (a replay reads the same data; `version` reads an older one). On
  DuckDB a step reads it as `read: delta`.
- **`table.write`** (DuckDBJob and SparkJob outputs) writes a step's rows in one Delta transaction: `append`;
  `overwrite` of what `replace_where` selects, or of the `whole_table` (one of them is required); `merge` with a
  `condition` (using `target.` and `source.`) and when_* clauses (`{action, set, predicate}`), its target pruned by a
  `target_predicate` derived from the source rows (e.g. `target.day IN ({{ step.day }})`). Every write carries an
  application transaction (`<job>:<output>`, the batch sequence), so replaying a batch never writes twice. Schemas are
  strict unless `schema_evolution: add_columns`.
- **`table.query`** (source engine only) runs ONE read-only DuckDB SELECT: it may call `delta_scan` only on a literal
  path inside the roots, and read `delta_table`, the table at `path`; `$name` parameters take the values of `params`
  (bound, never pasted). Other engines refuse it: read the table and use a step's `sql`.

### Vendor arguments

On the Spark engine, any undeclared `table.read`/`table.write` argument is a Delta reader/writer option forwarded
verbatim to `.option()` (e.g. `readChangeFeed: true`, `startingVersion: 3`); the adapter owns `versionAsOf`,
`mergeSchema` and the transaction keys, so passing one as a vendor option is rejected (contract v1.3 §6b).

## Engines

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`) | `engines/source.py` | DuckDB's delta extension on a locked sandbox of its own; pages of 1,000 records |
| DuckDBJob | `engines/duckdb.py` | reads with delta_scan; writes with delta-rs (`pip install "streamwright-deltalake[duckdb]"`) |
| SparkJob | `engines/spark.py` | delta-spark through the `storage` connection's filesystem; `pip install "streamwright-deltalake[spark]"` |

