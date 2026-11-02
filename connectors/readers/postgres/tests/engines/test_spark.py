"""The postgres Spark adapter: its capabilities, JDBC targets, and JDBC reads and writes on a real PostgreSQL."""

import os

import psycopg
import pytest
from pyspark.sql import SparkSession

from streamwright.connectors.postgres import CONNECTOR
from streamwright.connectors.postgres.engines import spark as spark_module
from streamwright.core.jobs import BatchContext
from streamwright.core.sdk.contract import check_arguments, resolve_values
from streamwright.core.sdk.testing import certify
from streamwright.spark.engine.runtime import StepRuntime
from streamwright.spark.io.capabilities import CAPABILITIES
from streamwright.spark.sdk import SparkContext

DSN = os.environ.get("STREAMWRIGHT_TEST_PG_DSN")

TestSparkCertification = certify(CONNECTOR, (CAPABILITIES,))
JAR = os.path.expanduser("~/.ivy2/jars/org.postgresql_postgresql-42.7.4.jar")


def test_spark_takes_the_staged_writes_and_their_names():
    write = CONNECTOR.operation("database", "write")
    assert check_arguments(CONNECTOR, write, {"table": "s.t", "mode": "delete", "primary_key": ["id"],
                                              "staging_table": "s.stg_{{ output.name }}_{{ uuid }}"},
                           CAPABILITIES) == []
    issues = check_arguments(CONNECTOR, write, {"table": "a.b.c", "mode": "merge", "primary_key": ["id"],
                                                "staging_table": "{{ config.x }}"}, CAPABILITIES)
    assert [issue.message.split(":")[0] for issue in issues] == ["`table` 'a.b.c'", "`staging_table` '{{ config.x }}'"]
    assert [issue.code for issue in check_arguments(CONNECTOR, write, {"table": "t", "mode": "overwrite"},
                                                    CAPABILITIES)] == ["unsupported-choice"]


@pytest.mark.parametrize("host", ["db internal", "/var/run/postgresql", "999.1.1.1", "[zz::1]", "a..b"])
def test_jdbc_urls_take_only_hosts_and_databases_they_can_hold(host):
    connection = resolve_values(CONNECTOR, {"host": host, "database": "shop", "user": "u"}) if not host.startswith(
        "db ") else {"host": host, "database": "shop", "user": "u"}
    with pytest.raises(ValueError, match="invalid JDBC host"):
        spark_module._target(connection)
    with pytest.raises(ValueError, match="invalid JDBC database"):
        spark_module._target({"host": "db", "database": "shop;x=1", "user": "u"})


def test_credentials_stay_out_of_the_url_and_spark_settings():
    connection = resolve_values(CONNECTOR, {"host": "db", "database": "shop", "user": "u", "password": "pw-SECRET",
                                            "sslmode": "require"})
    target = spark_module._target(connection)
    assert target.url == "jdbc:postgresql://db:5432/shop" and target.properties["password"] == "pw-SECRET"
    assert target.properties["sslmode"] == "require" and "pw-SECRET" not in repr(target)
    assert spark_module.PostgresSparkAdapter().hadoop_settings("pg", connection) == {}


@pytest.fixture(scope="module")
def spark():
    if not os.path.exists(JAR):
        pytest.skip("the PostgreSQL JDBC driver is not in the local ivy cache")
    session = (SparkSession.builder.master("local[1]").appName("mg-db-postgres").config("spark.jars", JAR)
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def test_jdbc_reads_appends_merges_and_deletes(spark, postgres):
    schema, values = postgres
    connection = resolve_values(CONNECTOR, dict(values, sslmode="disable"))
    adapter = spark_module.PostgresSparkAdapter()
    runtime = StepRuntime(spark, BatchContext("b-1", 1, "2026-10-05T00:00:00Z", "job", 1, "store-a", {}),
                          {}, {}, {}, limits={})
    write = CONNECTOR.operation("database", "write")

    def context(name):
        return SparkContext(runtime, name, connection)
    frame = adapter.read(context("q"), CONNECTOR.operation("database", "query"),
                         {"query": "SELECT id FROM %s.orders WHERE status = $s" % schema, "params": {"s": "paid"}})
    assert [row.id for row in frame.collect()] == [2]
    changes = spark.createDataFrame([(2, "refunded", 5.0, None), (3, "new", 7.0, None)],
                                    "id INT, status STRING, total DOUBLE, updated_at TIMESTAMP")
    receipt = adapter.write(context("merge_out"), changes, write, {"table": "%s.orders" % schema, "mode": "merge",
                                                                    "primary_key": ["id"], "update": ["status"]})
    assert receipt.rows == 2
    staging = "%s.stg_{{ output.name }}" % schema
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute('CREATE TABLE "%s".stg_delete_out (LIKE "%s".orders)' % (schema, schema))
    adapter.write(context("delete_out"), changes.where("id = 3"), write,
                  {"table": "%s.orders" % schema, "mode": "delete", "primary_key": ["id"], "staging_table": staging})
    adapter.write(context("append_out"), changes.where("id = 3"), write, {"table": "%s.orders" % schema})
    table = adapter.read(context("t"), CONNECTOR.operation("database", "table"),
                         {"schema": schema, "table": "orders", "columns": ["id", "status"]})
    assert sorted((row.id, row.status) for row in table.collect()) == [(1, "'; DROP TABLE orders; --"),
                                                                        (2, "refunded"), (3, "new")]
    with psycopg.connect(DSN) as conn:
        names = [row[0] for row in conn.execute("SELECT table_name FROM information_schema.tables WHERE "
                                                "table_schema = %s", [schema]).fetchall()]
    assert sorted(names) == ["orders", "stg_delete_out"]  # the temporary staging table is dropped


def test_the_state_store_opens_on_the_driver(postgres):
    _, values = postgres
    connect, dialect = spark_module.PostgresSparkAdapter().state_connection(dict(values, sslmode="disable"))
    with connect() as conn:
        assert conn.execute("SELECT 1").fetchone() == (1,) and dialect == "postgres"
