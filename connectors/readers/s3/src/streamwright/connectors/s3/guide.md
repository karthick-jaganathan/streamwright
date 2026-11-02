# Amazon S3 (`s3`)

csv, tsv, json, jsonl and parquet objects in Amazon S3 or an S3-compatible store (MinIO, Ceph, LocalStack). Every
engine reads and writes **only inside the connection's `roots`**: `s3://bucket/prefix/` URL prefixes. A path is an
`s3://` URL inside a root, or a key relative to the first root.

## Connection

```yaml
# source.yaml (auth:) or a job's connections.<name>:
provider: s3
roots: ["s3://acme-data/exports/"]     # references allowed, e.g. ["{{ config.bucket_root }}"]
key_id: "{{ secrets.s3_key_id }}"      # credentials: secret references only
secret: "{{ secrets.s3_secret }}"
region: us-east-1
# endpoint: minio.internal:9000        # S3-compatible stores: host[:port], no scheme
# url_style: path
# use_ssl: false
```

- Credentials are optional: without `key_id` and `secret` the source and DuckDB engines read anonymously (public
  buckets), and Spark uses the platform's credential chain (instance profile, environment). Spark never receives
  credentials from a connection: a Spark connection that names them is refused.
- `key_id`, `secret` and `session_token` must each be exactly one `{{ secrets.NAME }}` reference; their values are
  `***` in every log line and error, and reach DuckDB only as bound parameters of a secret scoped to the roots.
- `roots`, `region`, `endpoint`, `url_style` and `use_ssl` may be literals or `{{ config.* }}` references; they are
  checked again once resolved.

## Operations

s3 offers the shared file operations (`streamwright.core.sdk.operations.files`) and adds no argument of its own; the
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

## Engines

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`) | `engines/source.py` | DuckDB httpfs on a locked connection of its own; one temporary secret scoped to the roots |
| DuckDBJob | `engines/duckdb.py` | one secret per root, scoped to it, so two connections never share credentials |
| SparkJob | `engines/spark.py` | the S3A filesystem (`s3a://`), per-bucket endpoint/region settings; `pip install "streamwright-s3[spark]"` |
