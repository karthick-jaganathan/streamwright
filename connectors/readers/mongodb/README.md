# StreamWright mongodb connector

The `mongodb` connector for [streamwright](../../../core/README.md): read-only `find` and `aggregate` reads of MongoDB collections (`streamwright run` sources and SparkJob steps) and collection writes (SparkJob outputs), built on
[connector contract v1](../../../docs/design/connector-contract.md). Example: the source folder
[examples/sources/readers/mongodb_demo/](../../../examples/sources/readers/mongodb_demo/).

The user guide (connection keys, operations and their arguments, engines) is
[src/streamwright/connectors/mongodb/guide.md](src/streamwright/connectors/mongodb/guide.md); its tables are generated
from the spec and the engines' capabilities (`streamwright.core.sdk.contract.guide_text`).

## Install

```bash
make install-mongodb        # from the repository root; or: pip install ./connectors/readers/mongodb
pip install "./connectors/readers/mongodb[spark]"     # the SparkJob adapter's dependencies
streamwright connectors
streamwright validate examples/sources/readers/mongodb_demo      # static checks: no network
```

## Layout

| File | Holds |
|---|---|
| `__init__.py` | `CONNECTOR`, the one entry point (`streamwright.connectors`: `mongodb = "streamwright.connectors.mongodb:CONNECTOR"`) |
| `spec.py` | the definition: connection fields, its own `collection.find`, `collection.aggregate` and `collection.write`, adapters (pure data) |
| `client.py` | what every engine shares: the connection URI, the passwords to mask, resolved-connection rules, a find as a pipeline, BSON as JSON |
| `engines/source.py`, `engines/spark.py` | one small adapter per engine over `streamwright.core.io` / `streamwright.spark.io`, imported only when a job uses it |
| `guide.md` | the user guide (package data) |

## Security

- **Credentials** (`uri`, `password`) come from secrets only and are redacted everywhere (the password inside a URI too, as written and decoded).
- **Read-only sources**: only `find` and `aggregate`. Spark hands the URI to the MongoDB Spark connector as a read or write option, never Spark's configuration.

## Tests

```bash
make -C connectors/readers/mongodb test
```

`tests/test_spec.py` certifies the spec against the source and DuckDB engines (`streamwright.core.sdk.testing.certify`),
`tests/engines/test_spark.py` against Spark's capabilities; `tests/engines/` runs the adapter contract suites and the
engine-specific tests. It needs `STREAMWRIGHT_TEST_MONGODB_URI` (a throwaway MongoDB) for the engine tests.
