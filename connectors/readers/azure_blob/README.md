# StreamWright azure_blob connector

The `azure_blob` connector for [streamwright](../../../core/README.md): csv, tsv, json, jsonl and parquet blobs in Azure
Blob Storage (read through DuckDB's azure extension, or through Hadoop's ABFS filesystem on Spark), on every engine:
`streamwright run` sources, DuckDBJob (read-only) and SparkJob. It follows
[connector contract v1](../../../docs/design/connector-contract.md) like [`s3`](../s3/README.md). Example: the source
folder [examples/sources/readers/azure_blob_demo/](../../../examples/sources/readers/azure_blob_demo/).

The user guide (connection keys, operations and their arguments, engines) is
[src/streamwright/connectors/azure_blob/guide.md](src/streamwright/connectors/azure_blob/guide.md); its tables are
generated from the spec and the engines' capabilities. DuckDBJob's read-only limit is declared once in the spec
(`engine_limits=(EngineLimit(engine="duckdb", operation="object.write"),)`): every check, listing and the guide follow it.

## Install

```bash
make install-azure_blob      # from the repository root; or: pip install ./connectors/readers/azure_blob
pip install "./connectors/readers/azure_blob[spark]"     # with the SparkJob adapter's dependencies
streamwright connectors                                  # lists azure_blob [spark]
streamwright validate examples/sources/readers/azure_blob_demo     # static checks: no network, no container
```

DuckDB installs its azure extension on the first connect (offline hosts: run `INSTALL azure` ahead of time).

## Layout

| File | Holds |
|---|---|
| `__init__.py` | `CONNECTOR`, the one entry point (`streamwright.connectors`: `azure_blob = "streamwright.connectors.azure_blob:CONNECTOR"`) |
| `spec.py` | the definition: connection fields, the catalog's `files.READ` and `files.WRITE`, adapters, `engine_limits` (pure data) |
| `client.py` | what every engine shares about Azure: the store's facts, the connection string's parts, the endpoint and credential rules, the ABFS URL |
| `engines/source.py`, `engines/duckdb.py`, `engines/spark.py` | one small adapter per engine over `streamwright.core.io.files` / `streamwright.spark.io.files` |
| `guide.md` | the user guide (package data) |

## Security

- **No `?` anywhere.** Any `?`, `%`, `#` or backslash in a root, a path or a listed key is refused before anything is
  read; `?` is not a glob character here.
- **Containment.** A URL is read only inside a root: the same scheme and container (exactly as written) and a key under
  the root's prefix; `..`, `.`, empty folders, `user@`, ports and other schemes are refused.
- **`endpoint` cannot inject connection-string parts** (`host;AccountKey=other`): only `host[:port]` or an http(s) URL
  without credentials, query, fragment, whitespace or `;` is accepted, and the account name and key cannot hold `;`.
- **Credentials** come from secrets only, are redacted everywhere (a connection string's AccountKey and the connection
  string derived from an account and its key too) and reach DuckDB as bound parameters of a secret scoped to the roots.

## Tests

```bash
make -C connectors/readers/azure_blob test     # offline: a recording DuckDB stand-in serves azure:// URLs from local files
```
