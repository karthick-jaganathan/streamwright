"""The mongodb Spark adapter: its operations on Spark, and calls the source engine owns refused."""

import pytest

from streamwright.connectors.mongodb import CONNECTOR
from streamwright.connectors.mongodb.engines import spark as spark_module
from streamwright.core.sdk.contract import check_arguments
from streamwright.core.sdk.testing import certify
from streamwright.spark.io.capabilities import CAPABILITIES
from streamwright.spark.sdk import SparkContext

TestSparkCertification = certify(CONNECTOR, (CAPABILITIES,))


def test_spark_runs_reads_and_writes():
    assert [item.name for item in CONNECTOR.operations_for(CAPABILITIES)] == [
        "collection.find", "collection.aggregate", "collection.write"]
    write = CONNECTOR.operation("collection", "write")
    assert check_arguments(CONNECTOR, write, {"collection": "c", "mode": "merge", "primary_key": ["sku"]},
                           CAPABILITIES) == []
    assert spark_module.MongoDBSparkAdapter().hadoop_settings("m", {"uri": "mongodb://u:p@h/d"}) == {}


def test_source_arguments_and_a_missing_database_are_refused_before_spark_runs():
    context = SparkContext(None, "s", {"host": "h"})
    with pytest.raises(ValueError, match="`batch_size` is the source engine's"):
        spark_module.MongoDBSparkAdapter().read(context, CONNECTOR.operation("collection", "find"),
                                                {"collection": "c", "batch_size": 5})
    with pytest.raises(ValueError, match="name the database"):
        spark_module.MongoDBSparkAdapter().read(context, CONNECTOR.operation("collection", "find"),
                                                {"collection": "c"})
