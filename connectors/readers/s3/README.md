# StreamWright s3 connector

The `s3` connector for [streamwright](../../../core/README.md): csv, tsv, json, jsonl and parquet objects in Amazon S3
or an S3-compatible store (MinIO, Ceph, LocalStack), on every engine: `streamwright run` sources, DuckDBJob and
SparkJob. It is the reference implementation of
[connector contract v1](../../../docs/design/connector-contract.md). Example: the source folder
[examples/sources/readers/s3_demo/](../../../examples/sources/readers/s3_demo/).

The user guide (connection keys, operations and their arguments, engines) is
[src/streamwright/connectors/s3/guide.md](src/streamwright/connectors/s3/guide.md); its tables are generated from the
spec and the engines' capabilities (`streamwright.core.sdk.contract.guide_text`): which engine takes which argument is
declared once by each engine, not by the connector.

## Install

```bash
make install-s3              # from the repository root; or: pip install ./connectors/readers/s3
pip install "./connectors/readers/s3[spark]"      # with the SparkJob adapter's dependencies
streamwright connectors                           # lists s3 [spark]
streamwright validate examples/sources/readers/s3_demo      # static checks: no network, no bucket
STREAMWRIGHT_SECRET_S3_KEY_ID=AKIA... STREAMWRIGHT_SECRET_S3_SECRET=... \
  streamwright run examples/sources/readers/s3_demo --set bucket_root=s3://my-bucket/exports/ --allow-connector s3 --output jsonl:out
```

DuckDB installs its httpfs extension on the first connect (offline hosts: run `INSTALL httpfs` ahead of time).

## Layout

| File | Holds |
|---|---|
| `__init__.py` | `CONNECTOR`, the one entry point (`streamwright.connectors`: `s3 = "streamwright.connectors.s3:CONNECTOR"`) |
| `spec.py` | the definition: connection fields, the catalog's `files.READ` and `files.WRITE`, adapters (pure data) |
| `client.py` | what every engine shares about S3: the store's facts (`ObjectStore`), resolved-connection rules, the S3A scheme |
| `engines/source.py`, `engines/duckdb.py`, `engines/spark.py` | one small adapter per engine over `streamwright.core.io.files` / `streamwright.spark.io.files`, imported only when a job uses it |
| `guide.md` | the user guide (package data) |

## Security

- **No `?` anywhere.** DuckDB's httpfs reads an S3 URL's query parameters as connection settings
  (`?s3_endpoint=evil.example.com` would send signed requests elsewhere): any `?`, `%`, `#` or backslash in a root, a
  path or a listed key is refused before anything is read; `?` is not a glob character here.
- **Containment.** A URL is read or written only inside a root: the same bucket (exactly as written) and a key under
  the root's prefix. `..`, `.` and empty folders, `user@` and ports in the bucket and other schemes are refused.
  On the source engine DuckDB's `allowed_directories` are the roots and its configuration is locked.
- **Credentials** come from secrets only, are redacted everywhere and reach DuckDB as bound parameters of a secret
  scoped to the roots (one per root on DuckDBJob). Spark uses the platform credential chain only.

## Tests

```bash
make -C connectors/readers/s3 test      # offline: a recording DuckDB stand-in serves s3:// URLs from local files
```

`tests/test_spec.py` certifies the spec against the source and DuckDB engines (`streamwright.core.sdk.testing.certify`),
`tests/engines/test_spark.py` against Spark's capabilities; `tests/engines/` runs the `SourceAdapterContract` and
`DuckDBAdapterContract` suites and the engine-specific tests.
