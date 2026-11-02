# StreamWright deltalake connector

The `deltalake` connector for [streamwright](../../../core/README.md): Delta Lake tables in local folders or object storage: reads and queries (`streamwright run` sources), reads in DuckDBJob and SparkJob steps, and replay-safe appends, overwrites and merges in their outputs, built on
[connector contract v1](../../../docs/design/connector-contract.md). Example: the source folder
[examples/sources/readers/deltalake_demo/](../../../examples/sources/readers/deltalake_demo/).

The user guide (connection keys, operations and their arguments, engines) is
[src/streamwright/connectors/deltalake/guide.md](src/streamwright/connectors/deltalake/guide.md); its tables are generated
from the spec and the engines' capabilities (`streamwright.core.sdk.contract.guide_text`).

## Install

```bash
make install-deltalake        # from the repository root; or: pip install ./connectors/readers/deltalake
pip install "./connectors/readers/deltalake[duckdb]"  # DuckDBJob writes (delta-rs)
pip install "./connectors/readers/deltalake[spark]"   # the SparkJob adapter's dependencies (delta-spark)
streamwright connectors
streamwright validate examples/sources/readers/deltalake_demo      # static checks: no network
```

## Layout

| File | Holds |
|---|---|
| `__init__.py` | `CONNECTOR`, the one entry point (`streamwright.connectors`: `deltalake = "streamwright.connectors.deltalake:CONNECTOR"`) |
| `spec.py` | the definition: connection fields, the catalog's `tables.READ` and `tables.WRITE`, and its own `table.query` (source engine only: `engine_limits`), adapters (pure data) |
| `client.py` | what every engine shares: the roots and path containment, the storage secrets and delta-rs options, the `table.query` policy (delta_scan inside the roots), resolved-connection rules |
| `engines/source.py`, `engines/duckdb.py`, `engines/spark.py` | one small adapter per engine over `streamwright.core.io` / `streamwright.spark.io`, imported only when a job uses it |
| `guide.md` | the user guide (package data) |

## Security

- **Roots.** Every table path is checked against the connection's `roots` first; the source engine's DuckDB sandbox reads only inside them (allowed_directories, external access off, configuration locked) and `delta_scan` in a query takes only a literal path inside them.
- **Credentials** come from secrets only, are redacted everywhere (the account key inside an Azure connection string too) and reach DuckDB as bound parameters of secrets scoped to the roots. Spark takes credentials from the `storage` connection, never Spark's configuration.
- **Replay safety.** Every write carries an application transaction (`<job>:<output>`, the batch sequence): replaying a committed batch writes nothing twice.

## Tests

```bash
make -C connectors/readers/deltalake test
```

`tests/test_spec.py` certifies the spec against the source and DuckDB engines (`streamwright.core.sdk.testing.certify`),
`tests/engines/test_spark.py` against Spark's capabilities; `tests/engines/` runs the adapter contract suites and the
engine-specific tests. It runs offline on local Delta tables; the Spark test needs the delta-spark jars in the local ivy cache.
