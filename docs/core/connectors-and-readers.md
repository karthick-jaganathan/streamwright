---
layout: default
title: Connectors & Readers
parent: streamwright
nav_order: 6
permalink: /core/connectors-and-readers/
---

# Connectors and readers

SDK-backed connectors and query builders, async jobs, writing a connector, and reading local files, object storage (S3, GCS) and PostgreSQL.

## Connectors

APIs that need a vendor SDK (gRPC, SOAP, paged SDK objects) run through connectors.

- `auth: {provider: <connector>, ...}` builds the SDK client.
- A request `{name, sdk: <connector>, service, method, arguments}` in a stream's `requests` calls it.
- Files, objects and databases are read through connectors the same way ([below](#reading-from-files-and-databases)).
- Query builders, the other kind of component, write query languages.

### Installed connectors

`streamwright connectors` lists installed connectors using entry-point metadata without importing their
code; connectors registered in this process can also show their declared SDK loggers
([SDK logs]({{ site.baseurl }}/core/logging/#sdk-logs)). It does not list query builders, which `streamwright run` and
`streamwright validate` still use and check:

```text
$ streamwright connectors
advertising:
  meta_ads — Meta Ads (facebook-business SDK) (SDK loggers: urllib3.connectionpool)
  google_ads — Google Ads (GAQL via the google-ads SDK) (SDK loggers: google.ads.googleads.client)
  microsoft_ads — Microsoft Advertising (Bing Ads SDK) (SDK loggers: suds.client, suds.transport)
databases:
  postgres — PostgreSQL tables, read-only
files:
  files — Local files: CSV, JSON, JSONL, Parquet, TSV
object storage:
  gcs — Google Cloud Storage objects
  s3 — Amazon S3 objects (DuckDB httpfs)
```

| Connector | Package | Calls |
|---|---|---|
| `google_ads` | [streamwright-google-ads](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/ads/google_ads/README.md) | `GoogleAdsService.search_stream` / `search` (GAQL), `CustomerService.list_accessible_customers` |
| `microsoft_ads` | [streamwright-microsoft-ads](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/ads/microsoft_ads/README.md) | read operations of the v13 SOAP services; reports as async jobs |
| `meta_ads` | [streamwright-meta-ads](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/ads/meta_ads/README.md) | `api_get` and the declared `get_<edge>` edges of Marketing API objects |
| `files` | [streamwright-files](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/readers/files/README.md) | `object.read` / `object.write`: local csv, tsv, json, jsonl and parquet files (DuckDBJob and SparkJob too) |
| `s3` | [streamwright-s3](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/readers/s3/README.md) | `object.read`: the same formats from `s3://` buckets (or an S3-compatible store), through DuckDB's httpfs |
| `gcs` | [streamwright-gcs](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/readers/gcs/README.md) | `object.read` / `object.write`: the same formats from `gs://` buckets (an HMAC key), through DuckDB's httpfs |
| `azure_blob` | [streamwright-azure-blob](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/readers/azure_blob/README.md) | `object.read` / `object.write`: the same formats from `azure://` and `abfss://` containers, through DuckDB's azure extension (reads only on DuckDBJob) |
| `postgres` | [streamwright-postgres](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/readers/postgres/README.md) | `database.query` (one read-only SELECT) and `database.table` on a PostgreSQL database; `database.write` (append, overwrite, merge, delete) in DuckDBJob and SparkJob outputs |
| `mysql` | [streamwright-mysql](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/readers/mysql/README.md) | the same database operations on MySQL |
| `deltalake` | [streamwright-deltalake](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/readers/deltalake/README.md) | `table.read` and `table.query` (source) on Delta Lake tables; `table.write` (append, overwrite, merge; replay-safe) in DuckDBJob and SparkJob outputs |
| `mongodb` | [streamwright-mongodb](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/readers/mongodb/README.md) | `collection.find` and `collection.aggregate`; `collection.write` (append, merge) in SparkJob outputs |

### Read-only calls

A connector allows only read-only calls; anything else stops the run before it starts (exit 2).

### Rate limits and retries

- Connector calls use the stream's `rate_limit` and `retry` (or the source's `http` ones).
- Which provider error codes are retried is configuration: each connector declares its defaults in its spec
  (`retry_codes`, e.g. Google Ads `RESOURCE_EXHAUSTED`, Meta `17`/`613`, Microsoft `117`), and a stream's
  `retry.codes` replace them (e.g. `retry: {codes: [RESOURCE_EXHAUSTED]}` retries only that). Every code an error
  carries is matched; codeless failures (a dropped connection, a timeout) are always retried.
- A streamed response that fails after records were read is not retried.
- This is one execution-policy vocabulary (connector contract v1.2 §6a): the same `retry`, `rate_limit`,
  `paginator`/`page_size`, `timeout` and `on_error: fail|skip|warn` keys, with identical defaults, on a source
  stream, a DuckDBJob/SparkJob step and an output.

### Engine reach (which engines can use a connector)

A connector supports an engine **natively** (an adapter for it) or **through a call step** (its source adapter, run
by a job engine). `sw connectors`, the MCP views and the generated guides show both, in the fixed order
`source, duckdb, spark`, each marked native or `(call)`:

```
google_ads   [source, duckdb (call), spark (call)]   # source-only: reached via call steps
kafka        [source, duckdb (call), spark]           # native spark, duckdb via a call step
s3           [source, duckdb, spark]                   # native everywhere
```

So the ads connectors (source-only) are usable in a DuckDBJob and a SparkJob through call steps.

### Auth and headers

- Sources with a connector `auth.provider` cannot have `http` requests: their auth is the connector's.
- An sdk request takes no `headers`: per-request values (an account, a customer) are the operation's `arguments`.
  Connector contract v1 connectors take them as arguments, e.g.
  {% raw %}`arguments: {account_id: "{{ partition.account_id }}", request: {...}}`{% endraw %} for Microsoft Ads.
- An async job's `poll` uses its `submit` headers.
- Headers a connector does not take stop the run before it starts.

### Vendor arguments

A connector accepts everything its vendor accepts. Declared fields (the connection keys and operation arguments in
a connector's contract) are the common, *validated* vocabulary — they are typed, checked, and mean the same thing on
every engine (`max_messages`, `max_wait`, `format`, `mode`, …). They are never the limit of what you can pass:

- **Aliases.** A declared field may also be reached by its vendor-native names. Kafka's `security_protocol` accepts
  `security.protocol`; SQS's `max_messages`/`max_wait`/`visibility_timeout` accept `MaxNumberOfMessages`,
  `WaitTimeSeconds` and `VisibilityTimeout`. A field and its alias together is an error.
- **Vendor arguments.** Any key that is not a declared field or alias is forwarded verbatim to the vendor call that
  implements the operation, or (at connection level) to the vendor client's configuration:
  - Kafka consumer/producer conf (`fetch.wait.max.ms`, `session.timeout.ms`, `socket.timeout.ms`, …);
  - SQS boto3 receive/send parameters (`AttributeNames`, `MessageAttributeNames`, …) and client options;
  - object stores (`s3`/`gcs`/`azure_blob`/files): reader options pass through to DuckDB `read_csv`/`read_json`/
    `read_parquet` and to Spark `.option()`; connection keys become DuckDB secret parameters / httpfs settings;
  - PostgreSQL/MySQL: libpq / mysql connection options (`connect_timeout`, `application_name`, …) and Spark JDBC
    properties;
  - MongoDB: `find`/`aggregate` kwargs (`hint`, `max_time_ms`, `allowDiskUse`, …) and client URI options;
  - Google Ads request fields, Graph API parameters, Google Sheets `values.get` parameters, HTTP query parameters.
- **`options`.** The explicit `options` mapping is still the channel for a vendor key that collides with a declared
  field's name.
- **Validation.** A vendor argument must be a literal or a non-secret reference. A misspelled common field (within a
  small edit distance of a declared field or alias) is a warning (`did you mean max_messages?`). Format reader/writer
  options are no longer checked against a closed list: unknown names pass through to the engine, which is the
  authority; only the options whose meaning we translate (`delimiter`, `header`, …) stay typed.
- **Secrets.** A vendor argument may not hold a secret. Vendor *connection* configuration may hold a secret reference
  only under a key the connector declares as secret-bearing (e.g. Kafka `*.password`,
  `sasl.oauthbearer.client.secret`); such values are redacted.

### Query builders

A query builder writes query text from a mapping, so inputs are never pasted into queries.

- In an `http` or `sdk` request, a mapping with a single key that is the name of a query builder of a usable connector
  (installed, and allowed when there is an allow-list) is replaced by
  the query text the builder writes from it, e.g. `query: {gaql: {select, from, where, order_by, limit}}` with
  [streamwright-google-ads](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/ads/google_ads/README.md#gaql-queries).
- References are rendered with their types first.
- The builder checks, quotes and escapes every value.
- Only calls written in the source are built: values from inputs or responses stay data.
- A connector's query builders come with it: `--allow-connector google_ads` allows `gaql`; the builders of a connector
  that is not allowed are never imported.

### Checks

- `streamwright run`, and `streamwright validate`, run the checks of the installed connectors and query builders before anything is
  fetched (e.g. an unknown GAQL operator, with its file and line).
- Plain `streamwright validate` checks the YAML and every reference without them.

### Async jobs

An `async_job` request submits a job, polls it until it is done, then downloads the result (or reads it with `results`).

{% raw %}
```yaml
requests:
  - name: raw_campaign_performance
    async_job:
      submit: {sdk: microsoft_ads, service: ReportingService, method: SubmitGenerateReport, arguments: {...}}
      poll:
        method: PollGenerateReport                   # same service as submit
        arguments: {ReportRequestId: "{{ submit.result }}"}
        every: 15s
        timeout: 30m
        done_when: {path: Status, equals: Success}
        fail_when: {path: Status, equals: Error}
      download: {url: "{{ poll.ReportDownloadUrl }}", format: csv, compression: zip}   # or results: {sdk request}
```
{% endraw %}

**`submit` and `poll`**

- Their responses are scopes for the job's later parts.
- A response that is not a mapping (e.g. a job ID) is `submit.result` / `poll.result`.
- `poll` runs every `every` until `done_when` matches.
- `fail_when` or `timeout` fails the request like an API error.

**`download`**

- Fetches the file without the API's credentials (report URLs are pre-signed, and their signatures are masked in logs
  and errors).
- Streams it to a temporary file.
- `csv` (empty cells are null) and `jsonl` files give one record per row.
- A `json` file is a response, read with the request's `records.path`.
- An empty or missing URL means no data.

**`results`**

- An sdk request instead of a download, read like any response.

### Writing a connector

New connectors follow **[connector contract v1](../design/connector-contract.md)**: one package, one entry point to
a frozen `CONNECTOR` definition, vendor logic once, and one small adapter per engine. The `s3` connector
([connectors/readers/s3](https://github.com/karthick-jaganathan/streamwright/tree/master/connectors/readers/s3)) is the
reference implementation:

```toml
[project.entry-points."streamwright.connectors"]
my_store = "streamwright.connectors.my_store:CONNECTOR"
```

| File | Holds |
|---|---|
| `spec.py` | `CONNECTOR = ConnectorDefinition(...)` from `streamwright.core.sdk.contract`: `Field`s of the connection (`secret=True` for credentials), the operations from the shared catalog `streamwright.core.sdk.operations` (e.g. `operations=(files.READ, files.WRITE)`; `with_arguments` adds an argument of the connector's own), `adapters` (`{"source": "module:Class", ...}`: the engines it supports) and the vendor `validator`. Pure data. |
| `client.py` | vendor knowledge every engine shares: URL and endpoint rules, credential mapping, `validate_connection(connection) -> [problem]` on resolved values. |
| `engines/source.py` | `open(connection, context) -> session` with `session.read(operation, arguments)` yielding pages, and `close()` (`streamwright.core.sdk.engines.source`). |
| `engines/duckdb.py` | `configure(session, name, connection)`, `read_sql(context, operation, arguments)`, `write(context, operation, arguments)` (`streamwright.core.sdk.engines.duckdb`). |
| `engines/spark.py` | the `streamwright.spark.sdk.SparkAdapter` protocol (ships with streamwright-spark). |
| `guide.md` | the user guide (package data), sections `## Connection`, `## Operations`, `## Engines`; `guide_text()` appends the tables generated from the spec. |

Core validates connections and arguments from the spec (types, required and unknown keys, choices, patterns, secret
references), resolves `{% raw %}{{ config.* }}{% endraw %}` and `{% raw %}{{ secrets.* }}{% endraw %}`, runs the
vendor validator and hands each adapter a read-only `ResolvedConnection`. Each engine declares once what it supports
of the catalog operations (`EngineCapabilities`: the operation kinds it runs, the arguments and choices it takes):
an argument or a choice an engine does not take is refused by name ("`row_errors` is not supported by the duckdb
engine", "`mode: append` is not supported by the duckdb engine"). Behaviour shared between connectors is composed
from the engines' io helpers - `streamwright.core.io.files` (`ObjectStore`, `FileLocation`, `open_file_session`,
`configure_store_secrets`, `read_sql`, `write_files`) and `streamwright.spark.io.files` - not inherited. Test with
`streamwright.core.sdk.testing`:

```python
from streamwright.connectors.my_store import CONNECTOR
from streamwright.core.sdk.testing import certify

TestCertification = certify(CONNECTOR)    # spec, JSON Schema, secrets, each engine's arguments and adapter
```

and subclass `SourceAdapterContract` / `DuckDBAdapterContract` for the adapters.

A plug-in in `streamwright.connectors` whose target is not a `ConnectorDefinition` (a class written for the
pre-contract `Connector` API) is refused: "<name> (<distribution>) targets the pre-contract connector API; it needs an
update to connector contract v1 (docs/design/connector-contract.md)".

#### A source session

`engines/source.py` holds the adapter: `open(connection, context)` returns a session whose
`read(operation, arguments, context)` yields pages (lists of records) of one call, and `close()`. `context` is the
`ConnectorContext` of the request that reads: its stream's rate limit, retry policy, metrics and log fields. Raise
`ConnectorError(message, code, retryable, retry_after)` for the vendor's errors (`streamwright.core.io.api_calls`
translates them: `make_caller(context, translate)`, `translate_pages`).

#### A query builder

A query builder is listed by the connector that owns it, in its spec, and is usable wherever that connector is
(allowed): `ConnectorDefinition(..., query_builders={"gaql": "streamwright.connectors.google_ads.gaql:GaqlBuilder"})`.
The class subclasses `streamwright.core.sdk.QueryBuilder` and implements:

| Method | Returns |
|---|---|
| `check(spec)` | problems with the unrendered mapping, as `(path inside it, message)` pairs |
| `build(spec)` | the query text for the rendered mapping; raise `streamwright.core.sdk.QueryError` for a value it cannot write safely |

Before a run, an operation's argument checks see a builder's call as a `streamwright.core.sdk.BuiltQuery`.

#### Calls, tokens and logs

- Wrap every API call in `context.call(...)` for rate limits and retries. streamwright counts the calls in the run's metrics
  and logs a line per response on `streamwright.network`.
- Pass the tokens the connector obtains at run time (access tokens, refreshed tokens, signatures) to
  `context.secret(value)`: from then on they are `***` in every log line and error.
- An SDK's own loggers (e.g. `google.ads.googleads.client`) are turned on with `streamwright run --log NAME=DEBUG`;
  name them in the connector's guide. streamwright never turns them on by itself.
- An SDK that sends its requests with the requests library can also pass each response to
  `streamwright.core.sdk.network.http_details(response, context.redact)` (a response hook, as meta_ads does) for the
  headers and bodies on `streamwright.network` at `DEBUG`.

#### Shared bases

A connector imports only `streamwright.core.sdk*` and `streamwright.core.io*` (checked by
`scripts/check_plugin_contract.py`); its Spark adapter module also `streamwright.spark.sdk*` and
`streamwright.spark.io*`.

| Module | For |
|---|---|
| `streamwright.core.io.files` | files for contract v1 adapters: `ObjectStore` (a store's facts), `FileLocation` (roots, containment, relative keys) and `LocalLocation` (local folders), the source session, DuckDB secrets, reads and COPY writes, and the engines' capabilities (`SOURCE_CAPABILITIES`, `DUCKDB_CAPABILITIES`). |
| `streamwright.core.io.databases` | attached databases for contract v1 adapters: a frozen `DatabaseDialect` (extension, secret options, URL schemes, system schemas, server-side timeout; `database_connections`), the source session (a READ_ONLY ATTACH behind a temporary secret on a locked sandbox, `QueryReader`, `read_pages`) and the DuckDB `configure_database`, `read_sql` and `write_table`; `queries` holds the parse-tree query checks (`QueryPolicy`, `check_query`), `sandbox` the `DuckDBSandbox`, `database_rules` the engines' capabilities of the database and table operations. Spark's counterparts: `streamwright.spark.io.jdbc` (`JdbcDialect`, staged merges and deletes) and `streamwright.spark.io.delta`. |
| `streamwright.core.sdk.network` | `HttpClient`, `HttpError` and `http_details` for connectors that call a REST API themselves (`google_sheets`): credentials travel in headers, calls go through `context.call`. |
| `streamwright.core.sdk.messaging` | `BrokerClient`, `Message`: the poll/ack/publish client of a broker (`kafka`, `sqs`). |
| `streamwright.core.sdk.engines.duckdb` | the DuckDBJob adapter protocols (`DuckDBAdapter`, `DuckDBWriteAdapter`, `DuckDBStateAdapter`), `ReadContext` / `WriteContext` and the shared table-format write rules (`evolves_schema`, `merge_condition`, `render_target_predicate`). |

#### Examples and tests

- The connectors in [connectors/](https://github.com/karthick-jaganathan/streamwright/blob/master/connectors/README.md)
  are examples, one self-contained folder each.
- Test yours offline with `streamwright.core.sdk.testing` (`FakeApi`, `MemoryOutput`, `FakeClock`, `SourceRunner`, ...).
- Certify it with `certify(CONNECTOR)` and the `SourceAdapterContract` / `DuckDBAdapterContract` suites there
  (subclass one and set its attributes; the suite supplies the tests).

## Reading from files and databases

Local files, object storage and PostgreSQL databases are read through connectors, like any SDK-backed API.

- The `auth` block names the connector and what it may read.
- Each request is an `sdk` request. There is no other request kind and no other option for them.
- Allow the connector like any other (`--allow-connector files`, `--allow-connector s3`, `--allow-connector gcs`,
  `--allow-connector postgres`).
- There is no `https` reader.
- All read with DuckDB, on a connection of the connector's own (never the transform sandbox).
- Each row is one record, a JSON object, in pages of at most 1,000 records.
- Steps read them with `record->>'column'` and cast.

### Files

The `files` connector reads and writes local files only (DuckDBJob's `provider: files` is this connector too:
`pip install streamwright-files`):

- no URLs, no `httpfs`, no credentials;
- the source engine's DuckDB connection never has external access.

```bash
make install-files           # from the repository root; or: pip install ./connectors/readers/files
streamwright run examples/sources/readers/files_demo --set data_root=examples/sources/readers/files_demo/data --allow-connector files \
    --output jsonl:out
```

{% raw %}
```yaml
# source.yaml
auth:
  provider: files
  roots: ["{{ config.data_root }}"]    # the local folders files can be read from; no URLs, no secrets

# streams/orders.yaml
requests:
  - name: raw_orders
    sdk: files
    service: object                    # the default and only service
    method: read                       # or write, on DuckDBJob and SparkJob
    arguments:
      path: "orders/*.csv"             # a file, a glob or a list of them; references allowed
      format: csv                      # csv | tsv | json | jsonl | parquet | auto (the default: by extension)
      options: {header: true, filename: true}
      on_missing: skip                 # skip (the default: a warning) | error, when nothing matches the path
```
{% endraw %}

**Roots**

- `roots` is the boundary.
- Every path, relative to the first root or absolute inside any root, is checked to be inside a root before any file
  is read (once symbolic links are followed).
- The connector's DuckDB connection can read nothing else.
- Deployments set the roots, through `config` or the source.

**Paths**

- `path` can use {% raw %}`{{ config.* }}`{% endraw %}, {% raw %}`{{ partition.* }}`{% endraw %} and {% raw %}`{{ window.start }}`{% endraw %} / {% raw %}`{{ window.end }}`{% endraw %} (e.g. one file
  per day: {% raw %}`customers/{{ window.start }}.jsonl`{% endraw %}).
- A whole reference to a list (a `batch_size` partition of file names) reads each file of the list.
- Partitions, windows, `batch_size` and `records.explode` work as for any request.

**Picking files by regex: `match`**

- `match` picks files by a regex instead of a glob. `path` is then one folder inside a root.
- The files whose path relative to it fully matches `match` (a literal Python regex, `re.fullmatch`) are read, sorted
  by that relative path.
- `recursive: true` looks in sub-folders too (default `false`), whose names the relative path then has, separated by
  `/`.
- Each file it selects is checked to be inside a root before any file is read:

  ```yaml
      arguments:
        path: orders                                   # a folder, not a glob
        match: '(.*/)?orders_\d{4}-\d{2}-\d{2}\.csv'   # daily files at any depth
        recursive: true
  ```

**Values**

- CSV and TSV values are text unless `options.columns` types them.
- JSON values are as written.
- Parquet values keep their types (decimals exact, dates and timestamps as text).
- `filename: true` adds each file's path, relative to its root, as `filename`.

### Object storage: S3, Google Cloud Storage and Azure Blob

The connectors share one reader (`streamwright.core.io.files`), so they take the same options (one vocabulary on
every engine: `header`, `delimiter`, `nullstr`, `dateformat`, ... - see the `files` guide) and list objects in the same
order (sorted by path, each once), and read through DuckDB's `httpfs` (`azure`) extension:

- `s3` reads Amazon S3 (or an S3-compatible store such as MinIO);
- `gcs` reads Google Cloud Storage (its S3-compatible access, with an HMAC key);
- `azure_blob` reads Azure Blob Storage (`azure://`, `abfss://`; a connection string, an account name and key, or a service principal). DuckDBJob only reads it.

Their requests are those of `files`, on `service: object`, `match` and `recursive` included:

```bash
make install-s3              # or install-gcs; from the repository root, or: pip install ./connectors/readers/s3
STREAMWRIGHT_SECRET_S3_KEY_ID=... STREAMWRIGHT_SECRET_S3_SECRET=... streamwright run examples/sources/readers/s3_demo \
    --set bucket_root=s3://my-bucket/exports/ --allow-connector s3 --output jsonl:out
```

{% raw %}
```yaml
# source.yaml
auth:
  provider: s3                         # or gcs, with gs:// roots, key_id and secret only
  roots: ["{{ config.bucket_root }}"]  # the URL prefixes objects can be read from: s3://bucket/prefix/
  key_id: "{{ secrets.s3_key_id }}"    # credentials: one secret reference each
  secret: "{{ secrets.s3_secret }}"
  region: "{{ config.region }}"        # optional settings: region, endpoint, url_style, use_ssl

# streams/events.yaml
requests:
  - name: raw_events
    sdk: s3
    service: object                    # the default and only service
    method: read
    arguments:
      path: events/                    # with match: the folder to look in
      match: 'day=\d{4}-\d{2}-\d{2}/part-\d+\.parquet'
      recursive: true
      format: parquet
```
{% endraw %}

**`auth`**

| Connector | Takes |
|---|---|
| `s3` | `roots`, optional credentials `key_id` and `secret` (together; none: anonymous access) and `session_token` (each one {% raw %}`{{ secrets.* }}`{% endraw %} reference), and the settings `region`, `endpoint` (`host[:port]`), `url_style` (`vhost` or `path`) and `use_ssl` (literal values or references); see the connector's `guide.md` |
| `gcs` | `roots` (`gs://` only), `key_id` and `secret` (together; none: anonymous access) |
| `azure_blob` | `roots` (`azure://` or `abfss://`), and one way to sign in: `connection_string`, or `account_name` with `account_key`, a service principal (`tenant_id`, `client_id`, `client_secret`) or alone; `endpoint`, `use_ssl` |

**Credentials**

- A literal credential is refused (by `streamwright validate`, and by `streamwright run` for a value that is not a secret of the
  run).
- The credentials become one temporary DuckDB secret scoped to the roots, set with bound parameters.
- They are `***` in every log line, error and `--summary`.

**No `?` and no `%` in a URL**

- httpfs reads a URL's query parameters as connection settings, so `...x.csv?s3_endpoint=` would send a signed request
  to another host.
- A root, a path or a listed key with either is refused.
- So `?` is not a glob character here (only `*`, `[ab]` and `**`).

**Containment is exact**

- The root's scheme, exactly its bucket and a key under its prefix (`s3://acme/` does not hold `s3://acme-evil/...`).
- Checked before anything is read, and by DuckDB's `allowed_directories` too.

### PostgreSQL

```bash
make install-postgres        # from the repository root; or: pip install ./connectors/readers/postgres
STREAMWRIGHT_SECRET_PG_PASSWORD='...' streamwright run examples/sources/readers/postgres_demo \
  --set pg_host=db.example.com --allow-connector postgres --output jsonl:out
```

Two forms, one or the other (not both):

- **Connection keys** — when the connection is per-environment configuration (host, database, role) and only the
  password is secret. This is what the demo uses.
- **A `dsn` secret** — when the whole connection string is one managed secret (e.g. a URL from a vault), or needs
  libpq features the keys don't cover.

{% raw %}
```yaml
# source.yaml — connection keys (the password is the only secret)
spec:
  config:
    pg_host: {type: string, default: localhost}
  secrets:
    pg_password: {type: string}
auth:
  provider: postgres
  host: "{{ config.pg_host }}"         # connection keys are config: literal text or references
  port: 5432                           # optional (default 5432)
  database: shop
  user: reader
  password: "{{ secrets.pg_password }}"  # the only credential; a secret only, redacted in logs
  sslmode: require                     # optional: disable | allow | prefer | require | verify-ca | verify-full
  options: {connect_timeout: "10", application_name: streamwright}  # optional: extra libpq parameters
  statement_timeout: 5min              # optional: Postgres' statement_timeout

# source.yaml — or one DSN secret, in their place (not alongside the keys)
auth:
  provider: postgres
  dsn: "{{ secrets.pg_dsn }}"          # a libpq URL or keyword DSN, from secrets only; redacted with its password
  statement_timeout: 5min              # optional: Postgres' statement_timeout

# streams/orders.yaml
requests:
  - name: raw_orders
    sdk: postgres
    service: database
    method: query
    arguments:
      query: |
        SELECT id, customer_id, total::VARCHAR AS total, updated_at
        FROM public.orders
        WHERE updated_at >= $since AND updated_at < $until + INTERVAL 1 DAY
      params:
        since: "{{ window.start }}"
        until: "{{ window.end }}"
  - name: raw_customers
    sdk: postgres
    service: database
    method: table                      # the connector writes the SELECT: names quoted, values bound
    arguments: {schema: public, table: customers, columns: [id, name, status],
                where: [{column: status, op: "=", value: active}]}
```
{% endraw %}

**Queries**

- The query is DuckDB SQL over the attached database, not text sent to Postgres as written.
- Name tables `schema.table`; DuckDB pushes filters and columns down to Postgres.
- Its values are named parameters (`$since`) bound from `params`, never pasted into the query.
- The query text itself cannot hold references or secrets.

**Windows and batches**

- Incremental streams bind the window (`window.end` is the window's last day).
- A `batch_size` partition binds a list: {% raw %}`params: {ids: "{{ partition.ids }}"}`{% endraw %} with
  `WHERE order_id = ANY($ids::BIGINT[])` reads a batch in one query.

**Read-only**

- One SELECT per request (`WITH`, `FROM`-first and `VALUES` too).
- Write keywords, a second statement and functions that run text of their own are refused before the query runs.
- The database is attached `READ_ONLY`, and the connection reaches nothing else.
- A write fails even when the role could write.

**Isolation**

- The database is attached through a temporary DuckDB secret that holds the DSN, so DuckDB's views do not show it.
- Queries read the database's own tables only: DuckDB's system and catalog views (`duckdb_*`, `pragma_*`, `system.*`,
  `pg_catalog`, `information_schema`) and `SHOW`, `DESCRIBE` and `SUMMARIZE` are refused.

**Numbers**

- A `numeric` without a precision comes through as exact text (DuckDB's `pg_numeric_as_varchar`).
- Steps read JSON numbers as doubles (about 17 digits).
- Select big integers and exact decimals as text (`total::VARCHAR AS total`) and cast them in the step
  (`(record->>'total')::DECIMAL(18,2)`).

### Examples

- [examples/sources/readers/files_demo](https://github.com/karthick-jaganathan/streamwright/tree/master/examples/sources/readers/files_demo) (runs offline, on the files in its `data/`)
- [examples/sources/readers/s3_demo](https://github.com/karthick-jaganathan/streamwright/tree/master/examples/sources/readers/s3_demo)
- [examples/sources/readers/gcs_demo](https://github.com/karthick-jaganathan/streamwright/tree/master/examples/sources/readers/gcs_demo)
- [examples/sources/readers/postgres_demo](https://github.com/karthick-jaganathan/streamwright/tree/master/examples/sources/readers/postgres_demo)
- [examples/sources/readers/mysql_demo](https://github.com/karthick-jaganathan/streamwright/tree/master/examples/sources/readers/mysql_demo), [deltalake_demo](https://github.com/karthick-jaganathan/streamwright/tree/master/examples/sources/readers/deltalake_demo), [mongodb_demo](https://github.com/karthick-jaganathan/streamwright/tree/master/examples/sources/readers/mongodb_demo)

The connectors' READMEs have every option, the error codes and the logs.
