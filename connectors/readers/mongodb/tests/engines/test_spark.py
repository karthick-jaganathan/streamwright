#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

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
