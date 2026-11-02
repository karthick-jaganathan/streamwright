"""The deltalake Spark adapter: storage links, roots, and replay-safe Delta writes on a local Spark (cached jars)."""

import os
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession

from streamwright.connectors.deltalake import CONNECTOR
from streamwright.connectors.deltalake.engines import spark as spark_module
from streamwright.core.jobs import BatchContext
from streamwright.core.sdk.contract import check_arguments, resolve_values
from streamwright.core.sdk.testing import certify
from streamwright.spark.engine.runtime import StepRuntime
from streamwright.spark.io.capabilities import CAPABILITIES
from streamwright.spark.io.files import FileLocation
from streamwright.spark.sdk import SparkContext

TestSparkCertification = certify(CONNECTOR, (CAPABILITIES,))
JARS = tuple(os.path.expanduser("~/.ivy2/jars/io.delta_delta-%s-3.2.1.jar" % name)
             for name in ("spark_2.12", "storage"))


def test_spark_takes_every_write_argument_and_the_storage_must_be_an_object_store():
    write = CONNECTOR.operation("table", "write")
    assert check_arguments(CONNECTOR, write, {"path": "t", "mode": "merge", "condition": "c", "table_name": "x",
                                              "when_not_matched": [{"action": "insert"}]}, CAPABILITIES) == []
    adapter = spark_module.DeltaLakeSparkAdapter()
    assert adapter.check_links({"storage": "lake"}, {"lake": {"provider": "s3", "roots": ["s3://b/"]}}) == []
    assert adapter.check_links({"storage": "db"}, {"db": {"provider": "postgres"}}) == [
        "storage 'db' has no roots: it must be an object-store connection"]
    assert adapter.check_links({}, {})[0].startswith("storage must name")
    assert adapter.local_conf["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"


@pytest.fixture(scope="module")
def spark():
    if not all(os.path.exists(jar) for jar in JARS):
        pytest.skip("the delta-spark jars are not in the local ivy cache")
    builder = SparkSession.builder.master("local[1]").appName("mg-db-delta").config("spark.jars", ",".join(JARS))
    for key, value in spark_module.DeltaLakeSparkAdapter.local_conf.items():
        builder = builder.config(key, value)
    session = builder.config("spark.ui.enabled", "false").getOrCreate()
    yield session
    session.stop()


def _context(spark, tmp_path, name, sequence=1):
    lake = str(tmp_path / "lake")
    storage = SimpleNamespace(provider="files", auth={"roots": [str(tmp_path)]})
    adapter = SimpleNamespace(adapter=SimpleNamespace(location=lambda auth: FileLocation(
        tuple(auth["roots"]), "file", "file")))
    runtime = StepRuntime(spark, BatchContext("b-%d" % sequence, sequence, "2026-10-05T00:00:00Z", "job", 1,
                                              "store-a", {}),
                          {}, {"store": storage}, {"files": adapter}, limits={})
    return SparkContext(runtime, name, resolve_values(CONNECTOR, {"roots": [lake], "storage": "store"}))


def test_appends_are_replay_safe_and_merges_and_overwrites_follow_the_shared_rules(spark, tmp_path):
    adapter, write = spark_module.DeltaLakeSparkAdapter(), CONNECTOR.operation("table", "write")
    rows = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, v STRING")
    first = adapter.write(_context(spark, tmp_path, "out"), rows, write, {"path": "t"})
    replay = adapter.write(_context(spark, tmp_path, "out"), rows, write, {"path": "t"})
    assert (first.rows, first.version) == (2, 0) and replay.version == 0
    changes = spark.createDataFrame([(2, "B"), (3, "c")], "id INT, v STRING")
    merged = adapter.write(_context(spark, tmp_path, "out", 2), changes,
                           write, {"path": "t", "mode": "merge", "condition": "target.id = source.id",
                                   "when_matched": [{"action": "update", "set": {"v": "source.v"}}],
                                   "when_not_matched": [{"action": "insert", "predicate": "source.id > 2"}],
                                   "table_name": "mg_db_orders"})
    assert merged.rows == 2 and spark.catalog.tableExists("mg_db_orders")
    adapter.write(_context(spark, tmp_path, "out", 3), spark.createDataFrame([(1, "z")], "id INT, v STRING"), write,
                  {"path": "t", "mode": "overwrite", "replace_where": "id = 1"})
    frame = adapter.read(_context(spark, tmp_path, "scan"), CONNECTOR.operation("table", "read"),
                         {"path": "t", "where": [{"column": "id", "op": "<=", "value": 2}]})
    assert sorted((row.id, row.v) for row in frame.collect()) == [(1, "z"), (2, "B")]
    with pytest.raises(Exception, match="outside"):
        adapter.read(_context(spark, tmp_path, "scan"), CONNECTOR.operation("table", "read"), {"path": "/etc/t"})
