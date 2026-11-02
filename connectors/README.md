# StreamWright connectors

Connectors add APIs that need a vendor SDK to [streamwright](../core/README.md): an auth provider that builds
the SDK client, and the read-only calls that a stream's `requests` items (`{name, sdk: <connector>, service, method,
arguments}`) can make. Each folder is its own distribution, installed only where it is needed, since each one pulls in
a vendor SDK. Four reader connectors read files, object storage and databases the same way, with DuckDB instead of a
vendor SDK: `files` (local files), `s3` and `gcs` (object storage) and `postgres` (PostgreSQL).

| Folder | Package | Connector | Libraries | Loggers (for `--log`) |
|---|---|---|---|---|
| [google_ads](ads/google_ads/README.md) | `streamwright-google-ads` | `google_ads` | google-ads | `google.ads.googleads.client` |
| [microsoft_ads](ads/microsoft_ads/README.md) | `streamwright-microsoft-ads` | `microsoft_ads` | bingads | `suds.client`, `suds.transport` |
| [meta_ads](ads/meta_ads/README.md) | `streamwright-meta-ads` | `meta_ads` | facebook_business | `urllib3` |
| [openai_ads](ads/openai_ads/README.md) | `streamwright-openai-ads` | `openai_ads` | requests | `streamwright.network`, `urllib3` |
| [linkedin_ads](ads/linkedin_ads/README.md) | `streamwright-linkedin-ads` | `linkedin_ads` | requests | `streamwright.network`, `urllib3` |
| [amazon_ads](ads/amazon_ads/README.md) | `streamwright-amazon-ads` | `amazon_ads` | requests | `streamwright.network`, `urllib3` |
| [apple_ads](ads/apple_ads/README.md) | `streamwright-apple-ads` | `apple_ads` | requests | `streamwright.network`, `urllib3` |
| [files](readers/files/README.md) | `streamwright-files` | `files` | duckdb | - |
| [s3](readers/s3/README.md) | `streamwright-s3` | `s3` | duckdb (httpfs) | - |
| [gcs](readers/gcs/README.md) | `streamwright-gcs` | `gcs` | duckdb (httpfs) | - |
| [azure_blob](readers/azure_blob/README.md) | `streamwright-azure-blob` | `azure_blob` | duckdb (azure) | - |
| [postgres](readers/postgres/README.md) | `streamwright-postgres` | `postgres` | duckdb (postgres) | - |
| [mysql](readers/mysql/README.md) | `streamwright-mysql` | `mysql` | duckdb (mysql) | - |
| [mongodb](readers/mongodb/README.md) | `streamwright-mongodb` | `mongodb` | pymongo | `pymongo` |
| [deltalake](readers/deltalake/README.md) | `streamwright-deltalake` | `deltalake` | duckdb (delta, httpfs, azure) | - |
| [google_sheets](readers/google_sheets/README.md) | `streamwright-google-sheets` | `google_sheets` | google-auth, requests | `streamwright.network`, `urllib3`, `google.auth` |
| [kafka](readers/kafka/README.md) | `streamwright-kafka` | `kafka` | confluent-kafka | - |
| [sqs](readers/sqs/README.md) | `streamwright-sqs` | `sqs` | boto3 | `botocore` |
| [restapi](http/restapi/README.md) | `streamwright-restapi` | `restapi` | requests | `streamwright.network`, `urllib3` |

```bash
make install-connectors         # all of them, from the repository root; or: pip install ./connectors/ads/google_ads
make install-files              # one of them: install-files, install-s3, install-gcs, install-postgres, ...
streamwright connectors         # the installed connectors, as pip show shows packages
```

```text
$ streamwright connectors google_ads
Name: google_ads
Title: Google Ads
Family: advertising
Summary: Google Ads API campaigns, ad groups, keywords and metrics through GAQL
Package: streamwright-google-ads
Version: 0.0.1
Libraries: google-ads==33.0.0
DuckDB-Extensions:
Loggers: google.ads.googleads.client
```

Connectors are installed from the [hub](https://github.com/karthick-jaganathan/streamwright-hub), which pins each one
to a tag or commit of this repository; their version stays `0.0.1` until they are published to PyPI.

An SDK's loggers show its own request and response logs, redacted like streamwright's lines; streamwright never turns them on. Name
them with `--log` (each connector's README has an example):

```bash
streamwright run examples/sources/ads/google_ads --set customer_ids=1112223333 --log google.ads.googleads.client=DEBUG
```

## Files, object storage and databases

- `files` reads local csv, tsv, json, jsonl and parquet files only (no URLs, no `httpfs`, no credentials, external
  access always off): `auth: {provider: files, roots: [...]}` lists the local folders that can be read, and requests
  are `sdk: files`, `service: object`, `method: read`, `arguments: {path, format, options, on_missing, match, recursive}` (DuckDBJob's `provider: files` is this connector too). `path` is a
  file, a glob or a list; or, with `match` (a Python regex fully matched against each file's path relative to it), a
  folder, searched in its sub-folders too with `recursive: true`. Example:
  [examples/sources/readers/files_demo](../examples/sources/readers/files_demo/), which runs offline.
- `s3`, `gcs`, and `azure_blob` read the same formats from object storage through DuckDB (httpfs for S3/GCS, azure for Azure Blob), with the same arguments on
  `service: object`: `auth: {provider: s3, roots: ["s3://bucket/prefix/"], key_id, secret, ...}`,
  `{provider: gcs, roots: ["gs://bucket/prefix/"], key_id, secret}`, or `{provider: azure_blob, roots: ["azure://container/prefix/"], connection_string, ...}`.
  Credentials are `{{ secrets.* }}` references only and redacted; any `?` or `%` in a
  URL is refused, and the scheme, bucket/container and prefix of a root must match exactly. Examples:
  [examples/sources/readers/s3_demo](../examples/sources/readers/s3_demo/), [examples/sources/readers/gcs_demo](../examples/sources/readers/gcs_demo/),
  [examples/sources/readers/azure_blob_demo](../examples/sources/readers/azure_blob_demo/).
- `postgres` runs read-only SELECT queries on a PostgreSQL database, attached `READ_ONLY` through DuckDB's postgres
  extension and a temporary DuckDB secret holding the DSN: `auth: {provider: postgres, dsn: "{{ secrets.pg_dsn }}"}`,
  and requests are `sdk: postgres` with `method: query` (`{query, params}`: DuckDB SQL over the attached database,
  with bound `$name` values; DuckDB's system and catalog views are refused) or `method: table`. Example:
  [examples/sources/readers/postgres_demo](../examples/sources/readers/postgres_demo/).

There is no `https` reader. Allow them like the others: `streamwright run --allow-connector files` (or `s3`, `gcs`, `azure_blob`,
`postgres`). They log one line per file, object or query on `streamwright.network`, and have no SDK loggers.

## Layout

```text
connectors/<family>/<name>/
├── pyproject.toml              distribution streamwright-<name>; one entry point: <name> = "streamwright.connectors.<name>:CONNECTOR"
├── README.md                   auth keys, allowed calls, records, errors and logs
├── Makefile, LICENSE
├── src/streamwright/connectors/<name>/   import streamwright.connectors.<name> (streamwright and streamwright.connectors are namespace packages)
│   ├── __init__.py             exports CONNECTOR only
│   ├── spec.py                 the ConnectorDefinition (pure data; query builders and engine limits included)
│   ├── client.py               vendor knowledge, shared by every engine
│   ├── guide.md                user documentation (package data)
│   └── engines/                source.py, duckdb.py, spark.py: one adapter per engine (none for HTTP connectors)
└── tests/                      test_spec.py, test_client.py, engines/test_<engine>.py
```

Example sources are the folders in [examples/sources/](../examples/sources/), where CI validates them; the
[design doc](../docs/design/source-format.md) shows the same files.

## Adding a connector

1. Copy a connector folder, then rename the folder, the package (`src/streamwright/connectors/<name>`), the distribution
   (`streamwright-<name>`) and the entry point:

   ```toml
   [project.entry-points."streamwright.connectors"]
   <name> = "streamwright.connectors.<name>:CONNECTOR"
   ```

2. Implement the connector contract ([docs/design/connector-contract.md](../docs/design/connector-contract.md)):
   `spec.py` declares the connection fields, the operations (from the catalog `streamwright.core.sdk.operations`,
   read-only for sources) and the engine adapters; each session reads with the request's context (`context.call(...)`
   for rate limits and retries), raises `ConnectorError` for the vendor's errors, and passes tokens the connector
   obtains at run time to `context.secret(value)` (they are `***` in every log line and error).
3. Test offline with `streamwright.core.runtime.testing` (`FakeApi`, `MemoryOutput`, `FakeClock`, and `page_stream` for a stream
   with one request, step and export), starting each test module with `pytest.importorskip("<sdk module>")` so the
   core CI job, which has no SDKs, skips it.
4. Add the connector to `CONNECTORS` in the root Makefile and to the `connectors` job in `.github/workflows/ci.yml`, and add an
   example source folder to `examples/sources/` and the design doc.

Tests: `make test` runs everything; `python -m pytest connectors/<name>/tests` runs one connector's tests.
