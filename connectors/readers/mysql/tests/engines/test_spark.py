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

"""The mysql Spark adapter: its capabilities, JDBC target and dialect (no live Spark: no MySQL driver jar cached)."""

import pytest

from streamwright.connectors.mysql import CONNECTOR
from streamwright.connectors.mysql.engines import spark as spark_module
from streamwright.core.sdk.contract import check_arguments, resolve_values
from streamwright.core.sdk.testing import certify
from streamwright.spark.io import jdbc
from streamwright.spark.io.capabilities import CAPABILITIES

TestSparkCertification = certify(CONNECTOR, (CAPABILITIES,))


def test_the_jdbc_target_holds_credentials_only_in_its_properties():
    connection = resolve_values(CONNECTOR, {"dsn": "mysql://u:pw-SECRET@db:3307/shop?ssl_mode=required"})
    target = spark_module._target(connection)
    assert target.url == "jdbc:mysql://db:3307/shop" and "pw-SECRET" not in repr(target)
    assert target.properties["sslMode"] == "REQUIRED" and target.properties["driver"] == "com.mysql.cj.jdbc.Driver"
    with pytest.raises(ValueError, match="invalid JDBC port"):
        jdbc.jdbc_target(spark_module.DIALECT, host="db", database="d", user="u", password=None, port="x")


def test_tables_use_backticks_mysql_literals_and_the_connections_database():
    sql = jdbc.table_sql(spark_module.DIALECT, {"table": "orders", "columns": ["id"],
                                               "where": [{"column": "note", "op": "=", "value": "a\\b'c"}]})
    assert sql == "SELECT `id` FROM `orders` WHERE `note` = 'a\\\\b''c'"
    with pytest.raises(ValueError, match="exceeds the maximum length of 64"):
        spark_module.DIALECT.quote_name("x" * 65, "table")


def test_spark_runs_staged_merges_and_deletes():
    write = CONNECTOR.operation("database", "write")
    assert check_arguments(CONNECTOR, write, {"table": "orders", "mode": "delete", "primary_key": ["id"]},
                           CAPABILITIES) == []
    assert jdbc.bind_params("SELECT * FROM t WHERE a = $a AND b IN $b -- $c", {"a": "x\\'", "b": [1, 2]},
                            spark_module.DIALECT.literal) == "SELECT * FROM t WHERE a = 'x\\\\''' AND b IN (1, 2) -- $c"
