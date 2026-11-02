# MySQL (`mysql`)

Read-only SELECT queries and table reads on a MySQL database (`streamwright run` sources, DuckDBJob and SparkJob
steps), and table writes (DuckDBJob and SparkJob outputs). Each row is one record.

## Connection

```yaml
# source.yaml (auth:) or a job's connections.<name>: the connection keys...
provider: mysql
host: "{{ config.host }}"
port: 3306
database: shop                          # also the schema of a table named without one
user: reader
password: "{{ secrets.mysql_password }}"  # the only credential: a secret reference
sslmode: required
options: {ssl_ca: /etc/ssl/certs/ca.pem}
statement_timeout: 5min                 # interrupts a source statement running longer
# ...or one DSN secret instead of them (a mysql:// URL or key=value pairs):
# dsn: "{{ secrets.mysql_dsn }}"
```

- Either `dsn` or the connection keys (`host`, `database` and `user` at least), not both. `dsn` and `password` must
  each be exactly one `{{ secrets.NAME }}` reference; their values are `***` in every log line and error.
- DuckDB takes the connection only as the parameters of a temporary `TYPE mysql` secret (never in the ATTACH text); a
  parameter that secret cannot take is refused by name, never silently dropped. `options` may set `socket` and the
  `ssl_*` files.
- Spark builds its JDBC URL from the resolved host, port and database - each checked first - and passes user and
  password only as JDBC properties, never in Spark's configuration. Any undeclared connection key, and any undeclared
  `database.*` operation argument, is a JDBC driver property forwarded verbatim (e.g. `fetchsize`, `connectTimeout`,
  `sessionInitStatement`) - the installed driver is the authority (contract v1.3 §6b).

## Operations

mysql offers the shared database operations (`streamwright.core.sdk.operations.databases`) and adds no argument of its
own; the Engines column says which engine takes an argument.

- **`database.query`** runs ONE read-only SELECT, checked as on postgres (one statement, no write keyword, named
  `$name` parameters, DuckDB's parse tree: no system view or schema - `mysql`, `performance_schema`, `sys`,
  `information_schema` included - no file, no function that runs text of its own); `params` values are bound on the
  source engine, escaped literals on Spark (backslashes doubled: MySQL reads them as escapes).
- **`database.table`** reads a table (`schema` default: the connection's database), its `columns` and the rows every
  `where` condition holds for.
- **`database.write`** writes a step's rows into `table`: `append`; `overwrite` (DuckDB: replace the table);
  `merge` (update the rows matching on `primary_key`, insert the rest; Spark: `INSERT ... ON DUPLICATE KEY UPDATE`);
  `delete` (Spark: the rows matching on `primary_key`). On Spark, `merge` and `delete` load a staging table
  (`CREATE TABLE ... LIKE` the target, or the output's `staging_table`) and run one statement on the driver.

## Engines

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`) | `engines/source.py` | DuckDB's mysql extension on a locked connection of its own, attached READ_ONLY; pages of 1,000 records; the connector interrupts a statement past `statement_timeout` |
| DuckDBJob | `engines/duckdb.py` | attached under the connection's name, read-only unless an output uses it |
| SparkJob | `engines/spark.py` | JDBC (`com.mysql:mysql-connector-j`); merges and deletes through pymysql on the driver; `pip install "streamwright-mysql[spark]"` |
