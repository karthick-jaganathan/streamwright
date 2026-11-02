# StreamWright mysql connector

The `mysql` connector for [streamwright](../../../core/README.md): read-only SELECT queries and table reads on MySQL (`streamwright run` sources, DuckDBJob and SparkJob steps) and table writes (DuckDBJob and SparkJob outputs), built on
[connector contract v1](../../../docs/design/connector-contract.md). Example: the source folder
[examples/sources/readers/mysql_demo/](../../../examples/sources/readers/mysql_demo/).

The user guide (connection keys, operations and their arguments, engines) is
[src/streamwright/connectors/mysql/guide.md](src/streamwright/connectors/mysql/guide.md); its tables are generated
from the spec and the engines' capabilities (`streamwright.core.sdk.contract.guide_text`).

## Install

```bash
make install-mysql        # from the repository root; or: pip install ./connectors/readers/mysql
pip install "./connectors/readers/mysql[spark]"       # the SparkJob adapter's dependencies (pymysql)
streamwright connectors
streamwright validate examples/sources/readers/mysql_demo      # static checks: no network
```

## Layout

| File | Holds |
|---|---|
| `__init__.py` | `CONNECTOR`, the one entry point (`streamwright.connectors`: `mysql = "streamwright.connectors.mysql:CONNECTOR"`) |
| `spec.py` | the definition: connection fields, the catalog's `databases.QUERY`, `databases.TABLE` and `databases.WRITE`, adapters, and the `engine_limits` (DuckDB writes append or overwrite: its mysql extension cannot merge) (pure data) |
| `client.py` | what every engine shares: the dialect (`DatabaseDialect`: the `TYPE mysql` secret parameters, MySQL's system schemas), resolved-connection rules, the JDBC facts and the staged-write statements |
| `engines/source.py`, `engines/duckdb.py`, `engines/spark.py` | one small adapter per engine over `streamwright.core.io` / `streamwright.spark.io`, imported only when a job uses it |
| `guide.md` | the user guide (package data) |

## Security

- **Credentials** (`dsn`, `password`) come from secrets only and are redacted everywhere; DuckDB gets them only as parameters of a temporary `TYPE mysql` secret (a parameter it cannot take is refused by name).
- **Read-only sources.** A query is one SELECT, checked by its text and DuckDB's parse tree (no `mysql`, `performance_schema`, `sys` or `information_schema`); values are bound. The database is attached `READ_ONLY` and the sandbox locked.
- **Spark** checks the resolved host, port and database before building the JDBC URL; credentials travel only in JDBC properties.

## Tests

```bash
make -C connectors/readers/mysql test
```

`tests/test_spec.py` certifies the spec against the source and DuckDB engines (`streamwright.core.sdk.testing.certify`),
`tests/engines/test_spark.py` against Spark's capabilities; `tests/engines/` runs the adapter contract suites and the
engine-specific tests. It needs `STREAMWRIGHT_TEST_MYSQL_DSN` (a throwaway MySQL, e.g. `mysql://user:pw@127.0.0.1:3306/db`) for the engine tests.
