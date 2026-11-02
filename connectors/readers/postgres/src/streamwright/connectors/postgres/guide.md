# PostgreSQL (`postgres`)

Read-only SELECT queries and table reads on a PostgreSQL database (`streamwright run` sources, DuckDBJob and SparkJob
steps), and table writes (DuckDBJob and SparkJob outputs). Each row is one record; decimals stay exact, dates and
timestamps are text (timestamps with time zone in UTC).

## Connection

```yaml
# source.yaml (auth:) or a job's connections.<name>: the connection keys...
provider: postgres
host: "{{ config.pg_host }}"           # a host name, an IP address or a unix socket directory
port: 5432
database: shop
user: reader
password: "{{ secrets.pg_password }}"  # the only credential: a secret reference
sslmode: require
options: {connect_timeout: "10", application_name: streamwright}
statement_timeout: 5min                # the server's statement_timeout (500ms, 30s, 5min, 1h, or seconds)
# ...or one DSN secret instead of them (a postgresql:// URL or libpq key=value pairs):
# dsn: "{{ secrets.pg_dsn }}"
```

- Either `dsn` or the connection keys (`host`, `database` and `user` at least), not both. `dsn` and `password` must
  each be exactly one `{{ secrets.NAME }}` reference; their values (and the password inside a DSN, as written and
  URL-encoded) are `***` in every log line and error.
- DuckDB takes the connection only in a temporary secret, as ONE percent-encoded `postgresql://` URL (DuckDB 2.0 drops
  the password of a key=value DSN there): never in the ATTACH text, which DuckDB's system views show. A unix socket
  directory, a name with spaces or any password stays one value.
- `options` may set any libpq connection parameter that is not a key of its own and not a credential (e.g.
  `connect_timeout`, `application_name`, `sslrootcert`, `target_session_attrs`); the installed driver is the
  authority on which exist, so a new libpq parameter needs no change here (contract v1.3 §6b). An undeclared
  top-level connection key is forwarded the same way.
- Spark builds its JDBC URL from the resolved host, port and database - each checked first (a host name or IP
  address; letters, digits, `_`, `.`, `-` in a database name; a port 1-65535) - and passes user and password only as
  JDBC properties, never in Spark's configuration. Undeclared connection keys and undeclared `database.*` operation
  arguments become JDBC driver properties (e.g. `fetchsize`, `queryTimeout`, `sessionInitStatement`).

## Operations

postgres offers the shared database operations (`streamwright.core.sdk.operations.databases`) and adds no argument of
its own; the Engines column says which engine takes an argument.

- **`database.query`** runs ONE read-only SELECT. On the source engine it is checked twice before it runs: its text
  (one statement, no write keyword, named `$name` parameters only) and DuckDB's parse tree (no DuckDB system view or
  schema, no `pg_catalog` or `information_schema`, no file as a table, no table function but `unnest`, `range` and
  `generate_series`, no function that runs text of its own); the values of `params` are bound, never pasted into the
  query. On Spark, `$name` parameters become escaped literals and the database runs the query; on DuckDB
  (`read: query`) the database runs it through `postgres_query`.
- **`database.table`** reads a table (`schema` default `public`), its `columns` and the rows every `where` condition
  holds for (`{column, op, value, type}`; values bound or escaped, never pasted).
- **`database.write`** (DuckDBJob and SparkJob outputs) writes a step's rows into `table`: `append`; `overwrite`
  (DuckDB: replace the table); `merge` (update the rows matching on `primary_key`, insert the rest: `INSERT ... ON
  CONFLICT`, so the key needs a unique constraint); `delete` (Spark: delete the rows matching on `primary_key`).
  On Spark, `merge` and `delete` load an UNLOGGED staging table from the executors and run one statement in one
  driver transaction; without `staging_table` the run creates and drops a temporary one (CREATE and DROP privileges
  in the target schema), with it the table is truncated after use (`{{ output.name }}`, `{{ uuid }}`,
  `{{ batch.id_safe }}` and `{{ batch.seq }}` may name it).

## Engines

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`) | `engines/source.py` | DuckDB's postgres extension on a locked connection of its own, attached READ_ONLY; pages of 1,000 records; the server applies `statement_timeout` |
| DuckDBJob | `engines/duckdb.py` | attached under the connection's name, read-only unless an output or the state uses it; `runtime.state.connection` keeps the job's state in a schema (`pip install "streamwright-postgres[state]"`) |
| SparkJob | `engines/spark.py` | JDBC (`org.postgresql:postgresql`); merges, deletes and state through psycopg on the driver; `pip install "streamwright-postgres[spark]"` |
