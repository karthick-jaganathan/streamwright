# StreamWright postgres connector

The `postgres` connector for [streamwright](../../../core/README.md): read-only SELECT queries and table reads on PostgreSQL (`streamwright run` sources, DuckDBJob and SparkJob steps) and table writes (DuckDBJob and SparkJob outputs), built on
[connector contract v1](../../../docs/design/connector-contract.md). Example: the source folder
[examples/sources/readers/postgres_demo/](../../../examples/sources/readers/postgres_demo/).

The user guide (connection keys, operations and their arguments, engines) is
[src/streamwright/connectors/postgres/guide.md](src/streamwright/connectors/postgres/guide.md); its tables are generated
from the spec and the engines' capabilities (`streamwright.core.sdk.contract.guide_text`).

## Install

```bash
make install-postgres        # from the repository root; or: pip install ./connectors/readers/postgres
pip install "./connectors/readers/postgres[state]"    # DuckDBJob state in PostgreSQL (psycopg)
pip install "./connectors/readers/postgres[spark]"    # the SparkJob adapter's dependencies (psycopg)
streamwright connectors
streamwright validate examples/sources/readers/postgres_demo      # static checks: no network
```

## Layout

| File | Holds |
|---|---|
| `__init__.py` | `CONNECTOR`, the one entry point (`streamwright.connectors`: `postgres = "streamwright.connectors.postgres:CONNECTOR"`) |
| `spec.py` | the definition: connection fields, the catalog's `databases.QUERY`, `databases.TABLE` and `databases.WRITE`, adapters (pure data) |
| `client.py` | what every engine shares: the dialect (`DatabaseDialect`: the percent-encoded `postgresql://` URL secret, the server-side statement timeout), resolved-connection rules, the JDBC facts and the staged-write statements |
| `engines/source.py`, `engines/duckdb.py`, `engines/spark.py` | one small adapter per engine over `streamwright.core.io` / `streamwright.spark.io`, imported only when a job uses it |
| `guide.md` | the user guide (package data) |

## Security

- **Credentials** (`dsn`, `password`) come from secrets only and are redacted everywhere (the password inside a DSN too, as written and URL-encoded). DuckDB gets the connection only as ONE percent-encoded `postgresql://` URL in a temporary secret, never in the ATTACH text.
- **Read-only sources.** A query is one SELECT, checked by its text and DuckDB's parse tree before it runs (no system view or schema, no file, no function that runs text of its own); values are bound. The database is attached `READ_ONLY` and the sandbox locked (external access off).
- **Spark** checks the resolved host, port and database before building the JDBC URL; credentials travel only in JDBC properties, never in Spark's configuration.

## Tests

```bash
make -C connectors/readers/postgres test
```

`tests/test_spec.py` certifies the spec against the source and DuckDB engines (`streamwright.core.sdk.testing.certify`),
`tests/engines/test_spark.py` against Spark's capabilities; `tests/engines/` runs the adapter contract suites and the
engine-specific tests. It needs `STREAMWRIGHT_TEST_PG_DSN` (a throwaway PostgreSQL) for the engine tests; the Spark JDBC test also needs `org.postgresql:postgresql` in the local ivy cache.
