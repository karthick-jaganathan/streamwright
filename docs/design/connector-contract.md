---
layout: default
title: Connector contract
nav_exclude: true
permalink: /design/connector-contract/
---
{% raw %}
<!-- raw: the {{ }} references below are part of the format, not Jekyll's Liquid -->

# Connector contract (v1)
{: .no_toc }

**Status:** implemented 2026-11-01 (v1.1: the shared operation catalog, engine capabilities and the
`streamwright.core.io` helpers). All 19 connectors are on this contract and the pre-contract API is deleted
(§8, complete).
**Supersedes:** the three per-engine plug-in contracts: core `Connector` (`connector.py`, plus a `reader.py`), the
`streamwright.duckdb.connectors` `DuckDBAdapter` (`duckdb_adapter.py`), and the `streamwright.spark.connectors`
`Provider` hierarchy (`spark_adapter.py`).

## 1. Why

**The problem.** A connector is defined three times today, once per engine. Each definition has its own entry-point
group, its own base class and its own class-attribute tables (`auth_required`, `auth_optional`, `secret_keys`,
`credentials`, `settings`, `Method`/`arg` tables). The same facts are therefore restated and drift: the s3 credential
map and its `url_style` / `endpoint` rules exist three times.

**The goal.** A connector author should answer one question per file: *what the connector accepts* (spec), *how the
vendor works* (client), and *how each engine uses it* (one adapter per engine). What an operation *is* (its arguments
and their vocabulary) is declared once for every connector of a family, and what an *engine* supports of it is
declared once by the engine - never per connector, never per field.

## 2. Rules

1. **One connector = one package, one distribution, one entry point.**
   - The package is `streamwright.connectors.<name>`, distributed as `streamwright-<name>`.
   - It registers exactly one entry point:
     ```toml
     [project.entry-points."streamwright.connectors"]
     s3 = "streamwright.connectors.s3:CONNECTOR"
     ```
   - The engine-specific groups `streamwright.duckdb.connectors` and `streamwright.spark.connectors` are removed.
2. **`CONNECTOR` is the single source of truth.** It is a frozen `streamwright.core.sdk.ConnectorDefinition`: identity,
   the connection fields, the operations, and where each engine adapter lives. Everything engines and tools need
   (required keys, secret keys, argument schemas, JSON Schema, MCP descriptions, documentation tables) is **derived**
   from it and from the engines' capabilities (§5), never restated.
3. **The spec is pure data.** `spec.py` imports only `streamwright.core.sdk.contract` and the operation catalog
   `streamwright.core.sdk.operations`. Importing the package `__init__` (which imports only `spec`) must not import
   vendor SDKs, DuckDB or pyspark. This is what makes listing connectors cheap (guarantee G3).
4. **Operations come from the shared catalog.** A connector lists catalog operations (`operations=(files.READ,
   files.WRITE)`); it declares an operation of its own, or adds an argument of its own to a catalog operation
   (`files.READ.with_arguments(...)`), only for what is truly vendor-specific. One vocabulary across engines: a write
   `mode` is `error`, `overwrite` or `append` wherever it runs.
5. **Validation is generic.** Core validates the declared connection and the operation arguments from the spec: types,
   required fields, unknown keys, choices, patterns, and secret references for `secret=True` fields; then, for the
   engine that runs the operation, its capabilities (§5): an argument or a choice that engine does not support is an
   error, and the engine's semantic checks run. A connector adds only vendor rules on resolved values, in
   `client.validate_connection`.
6. **Vendor knowledge lives once, in `client.py`.** It holds URL and endpoint rules, credential mapping, client
   construction and error translation. Every engine adapter uses it.
7. **Engine adapters are optional and small.**
   - Each engine has its own module: `engines/source.py`, `engines/duckdb.py`, `engines/spark.py`.
   - Each implements one small protocol (§6).
   - A connector supports exactly the engines listed in `CONNECTOR.adapters`; on each, it offers the operations whose
     kind that engine runs (its capabilities). There are no per-operation or per-field engine lists.
   - Engines import an adapter lazily, by its `"module:Class"` path, only when a job uses that engine. The adapter
     module itself therefore imports its engine's libraries (pyspark, duckdb) **at module top**.
8. **Shared behaviour is composed, not inherited through class-attribute tables.** Object-store files, attached-database
   queries, JDBC, Delta and messaging are provided by the engines' io packages - `streamwright.core.io` (source and
   DuckDB engines) and `streamwright.spark.io` (Spark), one module per family with the same name in both - as
   functions and frozen configuration objects (`ObjectStore`, `FileLocation`, a `JdbcDialect` dataclass) that adapters
   call.
9. **User documentation is a guide file, not a docstring.** Each connector ships `guide.md` as package data, with
   sections `## Connection`, `## Operations` and `## Engines`. Tables of fields and arguments are generated from the
   spec.

## 3. Package layout

```
connectors/readers/s3/
├── pyproject.toml               one entry point (rule 1); extras: spark = [...], duckdb = [...]
├── Makefile                     include ../../connector.mk
└── src/streamwright/connectors/s3/
    ├── __init__.py              from streamwright.connectors.s3.spec import CONNECTOR; __all__ = ["CONNECTOR"]
    ├── spec.py                  CONNECTOR = ConnectorDefinition(..., operations=(files.READ, files.WRITE))
    ├── client.py                vendor logic shared by every engine            (no engine imports)
    ├── guide.md                 user documentation (package data)
    └── engines/
        ├── __init__.py          (empty)
        ├── source.py            S3SourceAdapter    — `streamwright run` sources
        ├── duckdb.py            S3DuckDBAdapter    — kind: DuckDBJob
        └── spark.py             S3SparkAdapter     — kind: SparkJob (imports pyspark at top)
```

Tests mirror it: `tests/test_spec.py`, `tests/test_client.py`, `tests/engines/test_{source,duckdb,spark}.py`, plus the
SDK contract suites (§7). There is no `reader.py` and no `connector.py`: what they held is the io helpers' behaviour
(composed in by the adapters) plus vendor facts (now in `client.py`).

The s3 spec in full is connection fields plus `operations=(files.READ, files.WRITE)`: s3 has no argument of its own.

## 4. The declarative contract (`streamwright.core.sdk.contract`)

All types are frozen dataclasses, and collections in them are tuples or frozen mappings.

```python
Field(name, type, *, required=False, secret=False, default=None, choices=(), pattern=None, item_type=None,
      description="")
# type: "string" | "integer" | "number" | "boolean" | "duration" | "list" | "mapping", or a tuple of them (a union,
#   e.g. ("string", "list") for a path or a list of paths); pattern applies to a text and to each text item
# secret=True: the declared value must be exactly one {{ secrets.NAME }} reference; never logged; redacted.

class OperationKind(str, Enum): READ = "read"; WRITE = "write"; CONSUME = "consume"; PUBLISH = "publish"

Operation(*, service, method, kind=OperationKind.READ, arguments=(), idempotent=True, description="", extensions=())
# keyword-only; derived: .name ("object.read"), .argument(name), .arguments_for(capabilities) (the arguments an engine
#   takes, choices narrowed to what it supports), .json_schema(capabilities=None), .describe()
# .with_arguments(*fields) -> Operation: a new operation with `fields` added, or replacing the arguments of the same
#   name; the added names are recorded in `extensions` (a connector's own arguments, which its adapters take)

ConnectorDefinition(name, title, family, *, connection, operations, adapters, sdk_version=SDK_VERSION,
                    guide="guide.md", summary="", validator=None, engine_limits=(), query_builders={})
# adapters: {"source": "streamwright.connectors.s3.engines.source:S3SourceAdapter", "duckdb": ..., "spark": ...}
#   - the engines the connector supports are the keys of `adapters`
# validator: "streamwright.connectors.s3.client:validate_connection" - the spec stays pure data, so core needs the
#   path of the vendor check (rule 5) to run it on resolved values; imported only when a connection is resolved
# engine_limits: what the connector cannot do on one of its engines although that engine's capabilities allow it,
#   declared once, as data (never per-field engine flags):
#   EngineLimit(*, engine, operation, choices={})  - without `choices` the engine does not run `operation` for this
#     connector; with {argument: (values...)} it runs it with only those values. Examples: azure_blob
#     EngineLimit(engine="duckdb", operation="object.write") (DuckDB's azure extension cannot COPY to Azure storage);
#     deltalake EngineLimit(engine="duckdb"|"spark", operation="table.query") (its own operation, source only);
#     mysql EngineLimit(engine="duckdb", operation="database.write", choices={"mode": ("append", "overwrite")})
#     (DuckDB's mysql extension cannot merge). operations_for, check_arguments, describe and the guide follow them.
# query_builders: {name: "module:Class"} - the connector's query builders (a `{name: {...}}` call in a request), e.g.
#   google_ads {"gaql": "streamwright.connectors.google_ads.gaql:GaqlBuilder"}; usable wherever the connector is
#   (its allow-list entry is theirs); imported only when a request calls one (.load_query_builder(name))
# derived, read-only: .engines (frozenset of the adapters' keys), .secret_fields, .required_fields, .field(name),
# .operation(service, method), .operations_for(capabilities) (the operations whose kind the engine runs, less those
# an engine limit excludes; none when the connector has no adapter for it), .capabilities_for(capabilities) (the
# engine's capabilities with the limits on choices applied), .json_schema(engine=None), .describe(*capabilities) ->
# dict (the operations, query builders, and per engine of `capabilities`: {operation: {argument: [choices] or None}}),
# .load_adapter(engine), .load_query_builder(name)
```

Core functions (engine-neutral; used by every engine, the validators and MCP):

```python
check_connection(definition, declared) -> list[Issue]          # declared form: keys, types, required, secret refs
check_arguments(definition, operation, arguments, capabilities=None) -> list[Issue]
#   declared checks; with an engine's capabilities also: unsupported-operation, unsupported-argument
#   ("`row_errors` is not supported by the duckdb engine"), unsupported-choice ("`mode: append` is not supported by
#   the duckdb engine (supported: error, overwrite)"), then the engine's semantic checks (code bad-value)
resolve_connection(definition, declared, *, config, secrets) -> ResolvedConnection   # read-only mapping + redactor
resolve_values(definition, rendered) -> ResolvedConnection     # the second half, for engines that render themselves
guide_text(definition, package, capabilities=()) -> str        # guide.md with the generated tables (rule 9)
```

`ResolvedConnection` is a read-only `Mapping` of the resolved values (defaults applied, rendered text booleans and
numbers converted) with `.secret_values`, `.redactor` and `.redact(text)`. Resolution raises
`InvalidConnectionError(connector, issues)`; its messages never quote secret values.

## 5. Operation catalog and engine capabilities

**The catalog** (`streamwright.core.sdk.operations`, one module per family) declares each shared operation once, with
the UNION of the arguments any engine knows and one vocabulary. The first family is `files`:

| Operation | Arguments (one vocabulary) |
|---|---|
| `files.READ` = `object.read` | `path` (required), `format` (`auto` \| `csv` \| `tsv` \| `json` \| `jsonl` \| `parquet`), `options`, `on_missing`, `match`, `recursive`, `schema_mode`, `columns`, `column_groups`, `vocabulary`, `on_unmapped_column`, `path_pattern`, `row_index`, `on_mismatch`, `row_errors`, `inject_path_vars` |
| `files.WRITE` = `object.write` | `path`, `format` (`csv` \| `tsv` \| `json` \| `jsonl` \| `parquet`; required), `mode` (`error` default \| `overwrite` \| `append`), `options`, `split_by`, `layout`, `file_name`, `partition_by`, `single_file`, `max_splits`, `on_exists` |

Descriptions are generic ("a URL of the connection's scheme or a key relative to the first root"). Later families:
`databases`, `messages`.

**Engine capabilities** are declared ONCE per engine, in its io module, as a frozen value:

```python
EngineCapabilities(engine, operation_kinds, arguments, checks={})
# engine: "source" | "duckdb" | "spark"; operation_kinds: frozenset of OperationKind the engine runs
# arguments: {operation name: {argument: None (every declared value) | tuple of the choices it supports}}
#   - an argument of a listed operation that is missing is NOT supported by the engine
#   - an operation that is not listed (a connector's own) passes all its arguments to the connector's adapter, as do a
#     catalog operation's `extensions`
# checks: {operation name: check(arguments) -> list[str]}: the engine's semantic checks (what declarations cannot say)
# derived: .runs(operation), .supports(operation, argument), .choices(operation, argument), .describe(operations)
```

| Engine | Declared in | Kinds | `object.read` | `object.write` |
|---|---|---|---|---|
| source | `streamwright.core.io.file_rules.SOURCE_CAPABILITIES` | read | `path`, `format`, `options`, `on_missing`, `match`, `recursive` | - |
| duckdb | `streamwright.core.io.file_rules.DUCKDB_CAPABILITIES` | read, write | `path`, `format`, `options`, the ingest keys (`schema_mode`, `columns`, `column_groups`, `vocabulary`, `on_unmapped_column`, `path_pattern`, `row_index`) | `path`, `format`, `mode` (`error`, `overwrite`), `options`, `split_by`, `layout`, `file_name`, `partition_by` |
| spark | `streamwright.spark.io.file_rules.CAPABILITIES` | read, write | `path`, `format`, `options`, `on_missing`, the ingest keys, `on_mismatch`, `row_errors`, `inject_path_vars` | `path`, `format`, `mode`, `options`, `split_by`, `layout`, `file_name`, `single_file`, `max_splits`, `on_exists` |

`io.files` re-exports them (`streamwright.core.io.files.SOURCE_CAPABILITIES` / `DUCKDB_CAPABILITIES`,
`streamwright.spark.io.files.CAPABILITIES`); `file_rules` is the module that does not import DuckDB or pyspark, so
validation and listing stay cheap. Everything derived from a spec follows the capabilities: the JSON Schema of an
operation for an engine (only its arguments and choices), `describe()`, the guide's Engines column, certification and
the MCP connector contract (each engine describes the connector with its own capabilities).

## 6. Engine adapter protocols

Each protocol is minimal. An adapter receives the `ResolvedConnection`, which has already passed
`check_connection` and the connector's `client.validate_connection`, and arguments that passed `check_arguments` with
the engine's capabilities.

```python
# streamwright.core.sdk.engines.source  (engine: streamwright run; context: the existing ConnectorContext)
class SourceAdapter(Protocol):
    def open(self, connection: ResolvedConnection, context: ConnectorContext) -> "SourceSession": ...
class SourceSession(Protocol):
    def read(self, operation: Operation, arguments: Mapping, context: ConnectorContext) -> Iterator[list[dict]]: ...
    def close(self) -> None: ...
# open() gets the context of the request that connects (calls made while connecting); read() the context of the
# request that reads - its stream's rate limit, retry policy, metrics and log fields (`context.where`)

# streamwright.core.sdk.engines.duckdb  (engine: DuckDBJob)
class DuckDBAdapter(Protocol):
    def configure(self, session: DuckDBSession, name: str, connection: ResolvedConnection) -> None: ...
    def read_sql(self, context: ReadContext, operation: Operation, arguments: Mapping) -> str: ...
class DuckDBWriteAdapter(DuckDBAdapter, Protocol):         # a connector that writes on this engine
    def write(self, context: WriteContext, operation: Operation, arguments: Mapping) -> WriteReceipt: ...

# streamwright.spark.sdk  (engine: SparkJob; ships with streamwright-spark)
class SparkAdapter(Protocol):
    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> Mapping[str, str]: ...  # never secret values
    def read(self, context: SparkContext, operation: Operation, arguments: Mapping) -> "DataFrame": ...
    def write(self, context: SparkContext, frame, operation: Operation, arguments: Mapping) -> WriteReceipt: ...
class SparkMessagingAdapter(Protocol):                     # consume / publish operations
    def poll(...); def ack(...); def publish(...); def destination(arguments) -> str
```

**Reusable behaviour is offered as io helpers, not base-class hierarchies.** Both engine packages have the same
layout, one module per family:

```
streamwright.core.io                       (source and duckdb engines)
├── files.py          ObjectStore, FileLocation; source: open_file_session -> FileSession (read_files);
│                     duckdb: configure_store_secrets, read_sql(context, arguments, locate=),
│                     write_files(context, arguments, locate=) (COPY: csv, tsv, json, jsonl, parquet);
│                     re-exports SOURCE_CAPABILITIES, DUCKDB_CAPABILITIES, check_read, check_write
├── file_rules.py     SOURCE_CAPABILITIES, DUCKDB_CAPABILITIES, check_read, check_write   (no DuckDB import)
├── locations.py      ObjectStore, FileLocation, the URL rules (no `?`, `%`, `#`, `..`; root containment)
├── local.py          LocalLocation: the roots of local files (real paths), glob / match resolution
└── file_formats.py   formats, reader options, the DuckDB query of one file, AccessError/ReadError/MissingError

streamwright.spark.io                      (spark engine)
├── files.py          FileLocation, read_files(context, arguments, location=), write_files(context, frame, ...);
│                     re-exports CAPABILITIES, check_read, check_write
└── file_rules.py     CAPABILITIES, check_read, check_write, READ_OPTIONS, WRITE_OPTIONS  (no pyspark import)
(later: jdbc.py with JdbcDialect, delta.py, messaging.py; core: databases)
```

`FileLocation` has the same meaning in both: the connection's roots; `resolve_path(path)` takes a URL of the
connection's scheme or a key relative to the first root and refuses anything outside the roots. An s3 adapter is a few
lines that call these helpers with s3's facts from `client.py` (`client.STORE`, a frozen `ObjectStore`: schemes,
DuckDB secret type, credential and setting parameters):

```python
class S3DuckDBAdapter:
    def configure(self, session, name, connection):
        files.configure_store_secrets(client.STORE, session, name, connection)
    def read_sql(self, context, operation, arguments):
        return files.read_sql(context, arguments, locate=_location(context.connection).resolve_path)
    def write(self, context, operation, arguments):
        return files.write_files(context, arguments, locate=_location(context.connection).resolve_path)
```

The object-store connectors (`s3`, `gcs`, `azure_blob`) and `files` are all adapters over these helpers; the
class-based bases they replaced (`sdk.objectstore`, DuckDB's `ObjectStoreAdapter` and built-in `files` adapter) are
deleted. DuckDBJob's local files are the `files` connector's (`engines/duckdb.py`).

**How engines load connectors.** An engine reads the `streamwright.connectors` entry points whose target is a
package's `CONNECTOR` (`streamwright.core.runtime.definitions`), checks the allow-list and SDK compatibility before
the import, and uses the definition with its capabilities: the source engine directly
(`runtime.source_connectors`: the declared and resolved checks of the `auth` block, the checks of each call, then
`SourceAdapter.open` once per run and `SourceSession.read` per request), DuckDBJob through
`jobs.duckdb.definition_adapter` (a step's `read: <format>` selects the read operation whose `format` takes it;
outputs use the write operation, whose `mode` choices the engine supports are the modes), SparkJob through
`streamwright.spark.runtime.definition`. `definition.load_adapter(engine)` imports the adapter only when a job uses
the engine. An entry point whose target is not a `ConnectorDefinition` was written for the pre-contract API and is
refused (`PluginError`: "<name> (<distribution>) targets the pre-contract connector API; it needs an update to
connector contract v1 (docs/design/connector-contract.md)").

**Which connectors an engine can use** is derived from the definitions, never from a group of the engine's own: a
job engine (`streamwright.core.jobs.engine.JobEngine`, registered in `streamwright.job_kinds`) runs on the engine
`engine_name(kind)` (`DuckDBJob` -> `duckdb`, `SparkJob` -> `spark`), and its connectors are the installed definitions
whose `adapters` name it: `streamwright.core.runtime.definitions.engine_connectors(engine)` (only their specs are
imported). `streamwright connectors` and the MCP server list connectors and their engines this way.

## 6a. Vendor arguments, execution policy and engine reach (contract v1.2, decided 2026-11-01)

**Vendor arguments: a connector accepts everything its vendor accepts.** Declared fields are the *common,
validated* vocabulary. They are never the limit of what a user can pass. Two layers:

1. **Declared fields** (connection fields and operation arguments in the spec and catalog) are typed and validated,
   and they mean the same thing on every engine (`max_messages`, `max_wait`, `format`, `mode`, …). A declared field
   may list `aliases`: vendor-native names with the same meaning, accepted interchangeably (e.g. kafka
   `max_messages` ← `max.poll.records`, `max_wait` ← `fetch.max.wait.ms`, with the unit conversion done by the
   connector client). Passing a field and its alias together is an error.
2. **Vendor arguments**: any key that is not a declared field or alias is forwarded verbatim to the vendor call that
   implements the operation (or, at connection level, to the vendor client's configuration).
   - **Where they go:**
     - Kafka consumer/producer conf, boto3 call parameters;
     - DuckDB `read_csv`/`read_json`/`read_parquet` parameters and `COPY` options;
     - Spark `DataFrameReader/Writer.option`, JDBC properties, delta-rs storage options;
     - Google Ads request fields, Graph API parameters, HTTP query parameters.
   - **Validation:** each value must be a literal or a reference that is not a secret. A misspelled common field is
     still caught: a vendor key within a small edit distance of a declared field or alias is a warning ("did you mean
     `max_messages`?").
   - **`options`:** the explicit `options` mapping is kept as the same channel, for vendor keys that collide with a
     declared field's name.
   - **Format-specific reader/writer options:** not checked against a closed allow-list any more. Unknown option
     names pass through to the engine, which is the authority. Only options whose meaning we translate stay declared
     (`delimiter`, `header`, …).
   - **Secrets:** a vendor argument may not hold a secret. Vendor *connection* configuration may hold a secret
     reference only under a key the connector declares as secret-bearing (`ConnectorDefinition.secret_key_patterns`,
     e.g. `*.password`, `sasl.oauthbearer.client.secret`); such values are registered for redaction.

**Execution policy: one vocabulary, every engine.** Paging, retries, rate limits and error handling are declared with
the same keys wherever they apply. An engine states once (in its capabilities) which of them it honours:

| Key | Meaning | Where |
|---|---|---|
| `retry` `{max_attempts, backoff, max_delay, codes}` | transient-error retries of calls/reads/writes | stream, step, output |
| `rate_limit` `{requests, per}` | client-side throttling of calls | stream, step |
| `paginator` / `page_size` | HTTP paging; vendor page or batch size | request, step |
| `on_error: fail \| skip \| warn` | a failed partition/file/record batch | stream (`on_partition_error`), step, output |
| `timeout` | per-call timeout | stream, step, output |

Defaults are identical across engines. An unsupported key is an `unsupported-argument` error naming the engine, never
silently ignored. Which provider error codes are worth retrying is data, not code: a connector declares its defaults as
`ConnectorDefinition.retry_codes`, every `ConnectorError` carries all of its provider `codes`, and a `retry.codes` list
replaces the defaults (one rule for streams, steps and outputs: `net.policies.retryable_error`).

**Engine reach: every engine can use every connector.** A connector supports an engine in one of two ways:

- **natively**, through an adapter for that engine (`adapters["duckdb"]`, `adapters["spark"]`): bulk or distributed
  I/O;
- **through its source adapter**, as a *call step*: the engine runs the source connector via the source engine
  (`SourceRunner.collect`) and loads the records. DuckDBJob does this today. SparkJob gets the same driver-side call
  steps (records streamed into a DataFrame in bounded batches).

`sw connectors`, MCP and the generated docs show both kinds of support for each connector. Engines are listed in a
fixed order (source, duckdb, spark), each marked native or call, e.g. `[source, duckdb (call), spark (call)]` for
google_ads and `[source, duckdb, spark]` for s3.

## 6b. The vendor's surface is never enumerated (contract v1.3, decided 2026-11-01)

**Rule: a connector declares its *policy*, the installed vendor library is the authority on its *surface*.** No
module may hardcode a vendor's catalog: its services, objects, edges, endpoints, methods, request fields, option or
parameter names, enum values, or API versions. Such lists go stale with every vendor release (a deprecated service,
a removed argument, a new edge) and force a code change for something that is configuration.

- **Operations are patterns.** An `Operation`'s `service` and `method` may be glob patterns (`*`, `?`, `[…]`); such
  an *operation family* matches every concrete `service.method` of the vendor that fits it. The patterns encode the
  connector's safety policy (e.g. read-only) and the argument shape, never a list of names:
  - meta_ads: `*.api_get` and `*.get_*` (any Marketing API object; reads only);
  - microsoft_ads: `*Service.Get*`, `*Service.Search*`, `*Service.Find*`, `ReportingService.SubmitGenerateReport`,
    `ReportingService.PollGenerateReport`;
  - google_ads: `GoogleAdsService.search` / `search_stream` (typed: `customer_id`, `query`) and the read methods
    (`get_*`, `list_*`) of any `*Service`, whose request fields are vendor arguments.
  `ConnectorDefinition.operation(service, method)` returns the exact operation first, else the first matching family
  bound to the concrete names (a concrete `Operation`), so engines, validation and logs never see a pattern.
- **Existence is the vendor's answer.** Whether `AdAccount.get_insights` or `CampaignManagementService.GetAdsByIds`
  exists is decided by the installed SDK (attribute lookup, WSDL, discovery document) at run time. An unknown name is
  a clear, non-retryable error naming the SDK and version, with "did you mean" suggestions drawn from the SDK by
  introspection. A connector may declare `surface="module:function"` (a pure introspection of the installed library,
  no network); `validate` calls it when the library is importable, so typos are still caught before a run.
- **Allowed constants** are the connector's own vocabulary, not the vendor's: its declared fields and aliases, its
  query-builder grammar (e.g. the GAQL builder's operators), the keys it consumes itself (e.g. kafka's
  `destination`, `group_id`), and transport mechanics (pagination cursor keys the client must own). Examples in
  guides are documentation, never allow-lists.
- **Certification:** `check_connectors` fails a spec that declares more than 3 concrete operations on one service with
  identical arguments (a family would do), or any module-level mapping or tuple of vendor names in a connector
  package; the standards review rejects vendor catalogs in clients.

## 7. Coding standards and certification

### Coding standards (enforced in CI with ruff, mypy on the SDK)

| Rule | Enforcement |
|---|---|
| Imports at module top. The lazy adapter boundary (rule 7) needs no in-function imports; an optional library is imported at the top of a module that only the code path needing it loads. A rare remaining exception says why on its line: `# noqa: PLC0415  <reason>` (honoured by both checks). | ruff `PLC0415`; `scripts/check_code_standards.py` |
| No global state: module level holds only immutable constants (`UPPER_CASE`, tuples / frozensets / `MappingProxyType`, `Final`). No `global` statements. Registries are injectable objects. | ruff `PLW0603`, `RUF012`; `scripts/check_code_standards.py` (mutable module-level containers) |
| No mutable class attributes. Declarative data belongs in the spec, not in class tables. | ruff `RUF012` |
| Functions are verbs and modules are named by feature. PEP 8 names. No cryptic abbreviations. | ruff `N`, review |
| Complexity ≤ 15 per function; modules ≤ 400 lines (tests exempt). | ruff `C901` (`max-complexity = 15`); `scripts/check_code_standards.py` `module-too-long` (blocking via `make lint`) |
| Lint baseline: `E`, `W`, `F`, `I`, `B`, `UP`, `SIM`, `C4`, `PIE`, `RET`, `PL`, `RUF`, `N`, `C90` | `make lint` (ruff + `scripts/check_code_standards.py`, vendored identically in streamwright-spark and streamwright-mcp), blocking in `make test` and CI |
| Typed public SDK | `mypy --strict streamwright.core.sdk streamwright.spark.sdk` |

### Certification

`streamwright.core.sdk.testing.certify(CONNECTOR, capabilities=ENGINE_CAPABILITIES)` generates contract tests from
the spec and the engines' capabilities (the core engines' by default; a Spark test adds
`streamwright.spark.io.file_rules.CAPABILITIES`):
- the spec is well formed;
- the JSON Schema is derived from it;
- secret fields are enforced and redacted;
- every engine adapter runs at least one operation, and its module exists;
- each engine takes exactly the arguments its capabilities list (schema and argument checks agree).

Per-engine suites (`SourceAdapterContract`, `DuckDBAdapterContract`, `SparkAdapterContract`) then exercise the
adapters. Third-party connectors run the same suites.

## 8. Migration (complete)

1. **Reference (done):**
   - the `sdk.contract` types, the `sdk.operations` catalog, engine capabilities and generic validation;
   - engines load adapters from `CONNECTOR.adapters`, with a temporary fallback to the legacy groups, deleted in
     step 3;
   - s3 fully migrated, including the Spark `SparkAdapter` protocol and the `core.io.files` / `spark.io.files` helpers;
   - the ruff config plus a baseline report.
2. **Families (done):** the other 18 connectors migrated, one family at a time (object stores, databases,
   messaging, ads/http).
3. **Cleanup (done):**
   - deleted: the `streamwright.duckdb.connectors`, `streamwright.spark.connectors` and `streamwright.query_builders`
     groups (query builders are the definition's `query_builders`), the legacy discovery fallback, the `Connector` /
     `ConnectorSpec` class-table bases (the source engine uses `SourceAdapter` / `SourceSession` directly), the public
     DuckDBJob `DuckDBAdapter` base (the built-in `duckdb` catalog is engine-internal), the Spark `Provider` /
     `Method` / `arg` / `Kind` / `Role` tables, and `JobEngine.connector_group` (engines derive their connectors from
     the definitions, §6);
   - `read_only_engines` generalized into `engine_limits` (§4); `SourceSession.read` takes the request's context (§6);
   - the ruff baseline is brought to zero and made blocking in CI per repository (streamwright-spark first).
{% endraw %}
