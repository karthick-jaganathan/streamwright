# Google Cloud Storage (`gcs`)

csv, tsv, json, jsonl and parquet objects in Google Cloud Storage. Every engine reads and writes **only inside the
connection's `roots`**: `gs://bucket/prefix/` URL prefixes. A path is a `gs://` URL inside a root, or a key relative to
the first root.

## Connection

```yaml
# source.yaml (auth:) or a job's connections.<name>:
provider: gcs
roots: ["gs://acme-data/exports/"]     # references allowed, e.g. ["{{ config.bucket_root }}"]
key_id: "{{ secrets.gcs_key_id }}"     # an HMAC key (Cloud Storage > Settings > Interoperability)
secret: "{{ secrets.gcs_secret }}"
```

- The HMAC key is optional: without `key_id` and `secret` the source and DuckDB engines read anonymously (public
  buckets), and Spark uses the platform's credentials. Spark never receives a key from a connection: a Spark connection
  that names one is refused.
- `key_id` and `secret` must each be exactly one `{{ secrets.NAME }}` reference; their values are `***` in every log
  line and error, and reach DuckDB only as bound parameters of a secret scoped to the roots.
- `roots` may be literals or `{{ config.* }}` references; they are checked again once resolved.

## Operations

gcs offers the shared file operations (`streamwright.core.sdk.operations.files`) and adds no argument of its own; the
Engines column of each table says which engine takes an argument (each engine declares it once).

- **`object.read`** reads a path, a glob (`*`, `[ab]`, `**` for any folders) or a list of them. `?` is never a glob
  character: DuckDB would read a URL's query as connection settings (`?s3_endpoint=`), so any `?`, `%`, `#` or
  backslash in a root, a path or a listed key is refused before anything is read. On the source engine `match` (a
  regex over the keys below the folder `path`) and `recursive` select objects; on DuckDB and Spark the ingest keys
  (`columns`, `column_groups`, `vocabulary`, `path_pattern`, `row_index`, `schema_mode`) harmonize schemas.
- **`object.write`** (DuckDBJob outputs and SparkJob outputs) writes a step's rows under a path inside the roots as
  `csv`, `tsv`, `json`, `jsonl` or `parquet`; `mode` is `error` (the default: never write over an object),
  `overwrite` or - on Spark - `append`. `split_by` writes one object per distinct value combination, named by
  `layout` and `file_name`.
- **`options`** are the reader's format options, with one vocabulary on every engine (see the `files` guide).

## Engines

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`) | `engines/source.py` | DuckDB httpfs on a locked connection of its own; one temporary secret scoped to the roots |
| DuckDBJob | `engines/duckdb.py` | one secret per root, scoped to it, so two connections never share credentials |
| SparkJob | `engines/spark.py` | GCS's S3-compatible endpoint through S3A (`s3a://`), per-bucket settings; `pip install "streamwright-gcs[spark]"` |
