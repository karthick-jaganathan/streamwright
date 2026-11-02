# StreamWright files connector

The `files` connector for [streamwright](../../../core/README.md): csv, tsv, json, jsonl and parquet files in local
folders and POSIX mounts, on every engine: `streamwright run` sources (read with [DuckDB](https://duckdb.org/)),
DuckDBJob (read and write) and SparkJob. It follows [connector contract v1](../../../docs/design/connector-contract.md)
like [`s3`](../s3/README.md). Example: the source folder
[examples/sources/readers/files_demo/](../../../examples/sources/readers/files_demo/), which reads the small files
committed in its `data/`. Object storage: the `s3`, `gcs` and `azure_blob` connectors (this one reads local files only).

The user guide (connection, operations and their arguments, the format options, engines) is
[src/streamwright/connectors/files/guide.md](src/streamwright/connectors/files/guide.md); its tables are generated from
the spec and the engines' capabilities.

**DuckDBJob's local files are this connector** (there is no built-in `files` provider any more): a DuckDBJob with
`provider: files` needs `streamwright-files` installed (`pip install streamwright-files`).

## Install

```bash
make install-files           # from the repository root; or: pip install ./connectors/readers/files
pip install "./connectors/readers/files[spark]"   # with the SparkJob adapter's dependencies
streamwright connectors                           # lists files [spark]
streamwright run examples/sources/readers/files_demo --set data_root=examples/sources/readers/files_demo/data \
    --allow-connector files --output jsonl:out
```

## Layout

| File | Holds |
|---|---|
| `__init__.py` | `CONNECTOR`, the one entry point (`streamwright.connectors`: `files = "streamwright.connectors.files:CONNECTOR"`) |
| `spec.py` | the definition: `roots`, the catalog's `files.READ` and `files.WRITE`, adapters (pure data) |
| `client.py` | what every engine shares: the rules for roots (local folders, never URLs) and Spark's `file://` URL |
| `engines/source.py`, `engines/duckdb.py`, `engines/spark.py` | one small adapter per engine over `streamwright.core.io.files` (`LocalLocation`) / `streamwright.spark.io.files` |
| `guide.md` | the user guide (package data) |

## Security

- Every file is checked BEFORE any file is read: no `..`, no URL, and every file inside a root once symbolic links are
  followed (for a glob or `match`, every file it selects); otherwise the request fails with
  `path ... is outside the roots` (code `ACCESS_DENIED`).
- The source engine's DuckDB connection can only read inside the roots too (DuckDB's `allowed_directories`), with
  external access off and its configuration locked. It never turns external access on, loads no extension (no `httpfs`:
  it reads no URL) and sets no secret.
- Allow the connector with `streamwright run --allow-connector files`.

## Logs

Each file read on the source engine is a call (counted in the run summary's `requests`) and logs an INFO line on
`streamwright.network`, with the file, its rows, bytes and time:

```text
INFO streamwright.network: stream 'orders', request 'raw_orders': files read /data/orders/east.csv: 3 row(s), 126 bytes, 0.00 s
```

## Tests

```bash
make -C connectors/readers/files test      # offline: local files only
```
