---
layout: default
title: Packages
nav_order: 10
description: "The StreamWright packages: streamwright and its connectors"
permalink: /packages/
has_children: true
has_toc: false
---

# StreamWright Packages

StreamWright is one core package, **streamwright**, and optional connectors. Every connector depends on streamwright and registers
itself under the `streamwright.connectors` entry point group, so `streamwright connectors` lists it once it is installed.

## streamwright

[`core/`](https://github.com/karthick-jaganathan/streamwright/tree/master/streamwright) ([documentation]({{ site.baseurl }}/core/)) - the `streamwright`
command (`streamwright run`, `streamwright validate`, `streamwright connectors`) and the runtime: built-in auth,
partitions, paginators, async jobs, incremental state, retries and rate limits, SQL transform steps on an embedded
DuckDB, and the outputs (Singer, JSONL, CSV, TSV, Parquet, DuckDB, DuckLake and, with the `dlt` extra, dlt).

Its packages (`streamwright.core.*`):

| Package | Purpose |
|---|---|
| `sdk` | the public connector SDK, the only core surface connectors import: the contract (`ConnectorDefinition`, `Field`, `Operation`), the operation catalog (`sdk.operations.files`, `databases`, `tables`, `messages`, `http`), engine adapter protocols (`sdk.engines`) and contract test suites (`sdk.testing`) |
| `io` | reusable connector behaviour as functions and frozen configs: files and object stores, attached databases, HTTP/SDK APIs, and each engine's `EngineCapabilities` |
| `runtime` | the connector contract internals, plug-in discovery (`runtime.plugins`), connector definitions, logging (`runtime.logs`), redaction and templates |
| `engine` | `SourceRunner` (`engine.runner`) and its collaborators: request readers, partitions, bookmarks, the SQL transform (`engine.sql`) |
| `validation` | `streamwright validate`: the source checks (`validation.sections`), YAML locations, job manifests and their overlays, the JSON Schemas |
| `jobs` | the engine-neutral `kind: *Job` framework (manifest, inputs, checks, plan, state store, batch runner, ingest planning, write rules) and the DuckDBJob engine (`jobs.duckdb`) |
| `outputs` | Singer, JSONL/CSV/TSV/Parquet files, DuckDB, DuckLake and dlt outputs |
| `config`, `net`, `sql`, `cli` | source loading and inputs; the HTTP client and retry policies; SQL quoting, conditions and analysis; the `streamwright` command |

## Connectors

| Connector | Package | Reads |
|---|---|---|
| `google_ads` | streamwright-google-ads | Google Ads (SDK, GAQL query builder) |
| `microsoft_ads` | streamwright-microsoft-ads | Microsoft Advertising (SDK, async reports) |
| `meta_ads` | streamwright-meta-ads | Meta Marketing API (SDK) |
| `files` | streamwright-files | local CSV, TSV, JSON, JSONL and Parquet files |
| `s3` | streamwright-s3 | Amazon S3 and S3-compatible object storage |
| `gcs` | streamwright-gcs | Google Cloud Storage |
| `azure_blob` | streamwright-azure-blob | Azure Blob Storage |
| `postgres` | streamwright-postgres | PostgreSQL (read-only queries; `[state]` extra for the jobs state store) |
| `mysql` | streamwright-mysql | MySQL (read-only queries) |
| `deltalake` | streamwright-deltalake | Delta Lake tables (`[duckdb]` extra for DuckDBJob writes) |
| `mongodb`, `google_sheets` | streamwright-mongodb, streamwright-google-sheets | MongoDB; Google Sheets |
| `kafka`, `sqs` | streamwright-kafka, streamwright-sqs | message brokers |
| `amazon_ads`, `apple_ads`, `linkedin_ads`, `openai_ads`, `restapi` | streamwright-<name> | declarative HTTP APIs |

Each connector's README (`connectors/<group>/<name>/README.md`) has its options, error codes, logs and adapters (DuckDBJob
and SparkJob, installed through the `[duckdb]` and `[spark]` extras); every package has a `pyproject.toml`, `README.md`, `LICENSE`
and a `Makefile` that includes `connectors/connector.mk` (`scripts/check_connectors.py` checks them).

## Installation

```bash
make install                  # streamwright
make install-connectors       # every connector; or e.g. make install-files
make install MODE=dev         # editable
```

See [Installation]({{ site.baseurl }}/installation) for details.
