# Azure Blob Storage (`azure_blob`)

csv, tsv, json, jsonl and parquet blobs in Azure Blob Storage (Data Lake Storage Gen2 included). Every engine reads
**only inside the connection's `roots`**, and writes there too except DuckDBJob (DuckDB's azure extension cannot COPY
to Azure storage): `azure://container/prefix/` or `abfss://container/prefix/` URL prefixes. A path is a URL inside a
root, or a key relative to the first root.

## Connection

```yaml
# source.yaml (auth:) or a job's connections.<name>:
provider: azure_blob
roots: ["azure://acme-data/exports/"]           # references allowed, e.g. ["{{ config.container_root }}"]
connection_string: "{{ secrets.azure_connection_string }}"
# or an account with its key:
# account_name: acmedata
# account_key: "{{ secrets.azure_account_key }}"
# or a service principal:
# account_name: acmedata
# tenant_id: 00000000-0000-0000-0000-000000000000
# client_id: 11111111-1111-1111-1111-111111111111
# client_secret: "{{ secrets.azure_client_secret }}"
# or the account alone: the platform's credential chain (managed identity, az login)
# endpoint: 127.0.0.1:10000                     # emulators (Azurite): host[:port] or an http(s) URL
# use_ssl: false
```

- One way to sign in: a `connection_string`; or `account_name` with `account_key`, with a service principal
  (`client_secret`, `tenant_id`, `client_id`), or alone. `account_key` and `client_secret` need `account_name`.
- `connection_string`, `account_key` and `client_secret` must each be exactly one `{{ secrets.NAME }}` reference;
  their values (and a connection string's AccountKey) are `***` in every log line and error, and reach DuckDB only as
  bound parameters of a secret scoped to the roots.
- An `endpoint` ends up in a connection string: it is `host[:port]` or an http(s) URL without credentials, query,
  fragment, whitespace or `;`, so it cannot add parts to the string.
- Spark never receives credentials from a connection: it needs `account_name` and the platform's credentials, and a
  connection that names a key, a connection string or a service principal is refused.

## Operations

azure_blob offers the shared file operations (`streamwright.core.sdk.operations.files`) and adds no argument of its
own; the Engines column of each table says which engine takes an argument (each engine declares it once). `object.write`
runs on Spark only.

- **`object.read`** reads a path, a glob (`*`, `[ab]`, `**` for any folders) or a list of them. `?` is never a glob
  character, and any `?`, `%`, `#` or backslash in a root, a path or a listed key is refused before anything is read.
  On the source engine `match` (a regex over the keys below the folder `path`) and `recursive` select blobs; on DuckDB
  and Spark the ingest keys (`columns`, `column_groups`, `vocabulary`, `path_pattern`, `row_index`, `schema_mode`)
  harmonize schemas.
- **`object.write`** (SparkJob outputs) writes a step's rows under a path inside the roots as `csv`, `tsv`, `json`,
  `jsonl` or `parquet`; `mode` is `error`, `overwrite` or `append`. `split_by` writes one blob per distinct value
  combination, named by `layout` and `file_name`.
- **`options`** are the reader's format options, with one vocabulary on every engine (see the `files` guide).

## Engines

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`) | `engines/source.py` | DuckDB's azure extension on a locked connection of its own; one temporary secret scoped to the roots |
| DuckDBJob | `engines/duckdb.py` | reads only; one secret per root, scoped to it |
| SparkJob | `engines/spark.py` | the ABFS filesystem (`abfss://container@account.dfs.core.windows.net/`); `pip install "streamwright-azure_blob[spark]"` |
