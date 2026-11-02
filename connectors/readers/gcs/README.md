# StreamWright gcs connector

The `gcs` connector for [streamwright](../../../core/README.md): csv, tsv, json, jsonl and parquet objects in Google
Cloud Storage (read through DuckDB's httpfs extension with an HMAC key, or through Hadoop's S3A on Spark), on every
engine: `streamwright run` sources, DuckDBJob and SparkJob. It follows
[connector contract v1](../../../docs/design/connector-contract.md) like [`s3`](../s3/README.md). Example: the source
folder [examples/sources/readers/gcs_demo/](../../../examples/sources/readers/gcs_demo/).

The user guide (connection keys, operations and their arguments, engines) is
[src/streamwright/connectors/gcs/guide.md](src/streamwright/connectors/gcs/guide.md); its tables are generated from
the spec and the engines' capabilities.

## Install

```bash
make install-gcs             # from the repository root; or: pip install ./connectors/readers/gcs
pip install "./connectors/readers/gcs[spark]"     # with the SparkJob adapter's dependencies
streamwright connectors                           # lists gcs [spark]
streamwright validate examples/sources/readers/gcs_demo     # static checks: no network, no bucket
STREAMWRIGHT_SECRET_GCS_KEY_ID=GOOG1E... STREAMWRIGHT_SECRET_GCS_SECRET=... \
  streamwright run examples/sources/readers/gcs_demo --set bucket_root=gs://my-bucket/exports/ --allow-connector gcs --output jsonl:out
```

DuckDB installs its httpfs extension on the first connect (offline hosts: run `INSTALL httpfs` ahead of time).

## Layout

| File | Holds |
|---|---|
| `__init__.py` | `CONNECTOR`, the one entry point (`streamwright.connectors`: `gcs = "streamwright.connectors.gcs:CONNECTOR"`) |
| `spec.py` | the definition: connection fields, the catalog's `files.READ` and `files.WRITE`, adapters (pure data) |
| `client.py` | what every engine shares about GCS: the store's facts (`ObjectStore`), resolved-connection rules, the S3A endpoint |
| `engines/source.py`, `engines/duckdb.py`, `engines/spark.py` | one small adapter per engine over `streamwright.core.io.files` / `streamwright.spark.io.files` |
| `guide.md` | the user guide (package data) |

## Security

- **No `?` anywhere.** DuckDB's httpfs reads a URL's query parameters as connection settings (`?s3_endpoint=evil...`
  would send signed requests elsewhere): any `?`, `%`, `#` or backslash in a root, a path or a listed key is refused
  before anything is read.
- **Containment.** A URL is read or written only inside a root: the same bucket (exactly as written) and a key under
  the root's prefix; `..`, `.`, empty folders, `user@`, ports and other schemes (`s3://`, `gcs://`) are refused.
- **Credentials** come from secrets only, are redacted everywhere and reach DuckDB as bound parameters of a secret
  scoped to the roots. Spark uses the platform's credentials only (no key reaches Spark's configuration).

## Tests

```bash
make -C connectors/readers/gcs test      # offline: a recording DuckDB stand-in serves gs:// URLs from local files
```
