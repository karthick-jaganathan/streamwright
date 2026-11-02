# Local files (`files`)

csv, tsv, json, jsonl and parquet files in local folders and POSIX mounts. Every engine reads and writes **only inside
the connection's `roots`**: local folders (no URLs: object storage has the `s3`, `gcs` and `azure_blob` connectors). A
path is absolute inside a root, or relative to the first root.

## Connection

```yaml
# source.yaml (auth:) or a job's connections.<name>:
provider: files
roots: ["{{ config.data_root }}"]      # the only folders files can be read from
```

- `roots` is the allow-list: deployments decide it (through `config` or the source), and nothing outside it can be
  read. It takes no secrets and no URLs. Relative folders are relative to the working directory (SourceRunner and
  SparkJob) or to the job's folder (DuckDBJob).
- The source engine needs each root to exist; DuckDBJob and Spark outputs create the folders they write to.
- DuckDBJob's `files` provider is this connector: install `streamwright-files` to read or write local files from a
  DuckDBJob (`pip install streamwright-files`).

## Operations

files offers the shared file operations (`streamwright.core.sdk.operations.files`) and adds no argument of its own; the
Engines column of each table says which engine takes an argument (each engine declares it once). A request names the
service `object`: `sdk: files`, `service: object`, `method: read`.

- **`object.read`** reads a path, a glob (`*`, `?`, `[ab]`, `**` for any folders) or a list of them, in name order.
  `format: auto` takes each file's format from its extension (`.csv`, `.tsv`/`.tab`, `.json`, `.jsonl`/`.ndjson`,
  `.parquet`/`.pq`, also followed by `.gz`/`.zst`). On the source engine `match` (a Python regex that must fully match
  each file's path below the folder `path`) and `recursive` select files; on DuckDB and Spark the ingest keys
  (`columns`, `column_groups`, `vocabulary`, `path_pattern`, `row_index`, `schema_mode`) harmonize schemas.
- **`object.write`** (DuckDBJob outputs and SparkJob outputs) writes a step's rows under a path inside the roots as
  `csv`, `tsv`, `json`, `jsonl` or `parquet`; `mode` is `error` (the default: never write over a file), `overwrite` or
  - on Spark - `append`. `split_by` writes one file per distinct value combination, named by `layout` and `file_name`.
- **`options`** are the reader's format options. One vocabulary on every connector and engine; an engine that lacks
  one refuses it by name when the job or source is validated:

| Format | Options | Notes |
|---|---|---|
| csv, tsv | `header`, `delimiter`, `quote`, `escape`, `compression`, `nullstr`, `dateformat`, `timestampformat` | every engine (`nullstr` is a text on Spark) |
| csv, tsv | `columns` (name: DuckDB type), `null_padding`, `ignore_errors`, `skip`, `all_varchar`, `filename` | source and DuckDB |
| csv, tsv | `encoding`, `multiLine` | Spark only |
| json | `format` (auto, array, newline_delimited), `compression`, `ignore_errors`, `filename` | source and DuckDB |
| jsonl | `compression`, `ignore_errors`, `filename` | source and DuckDB |
| parquet | `filename` | source and DuckDB |

`filename: true` adds each file's path, relative to its root, to its records (source and DuckDB engines).

**Records.** On the source engine each row of a file is one record in pages of at most 1,000 (memory stays bounded however large
the file is): csv/tsv the file's text (empty cells are null) unless `columns` types them; json/jsonl each object as
written; parquet its values with decimals exact.

**Security.**

- Every file is checked BEFORE any file is read: no `..`, no URL, and every file inside a root once symbolic links
  are followed (for a glob or `match`, every file it selects) and with no glob character in its real path, otherwise
  the request fails with `path ... is outside the roots` (code `ACCESS_DENIED`).
- The source engine's DuckDB connection can only read inside the roots too (DuckDB's `allowed_directories`, which also
  refuses symbolic links that leave them), with external access off and its configuration locked. It loads no
  extension and sets no secret.

## Engines

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`) | `engines/source.py` | reads only; a locked DuckDB connection of its own |
| DuckDBJob | `engines/duckdb.py` | reads and writes; roots relative to the job's folder; writes create the folders |
| SparkJob | `engines/spark.py` | Spark's native `file://` filesystem (no jar); `pip install "streamwright-files[spark]"` |
