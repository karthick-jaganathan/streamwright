#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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

import os
import uuid

import pytest

from streamwright.connectors.mysql import client
from streamwright.core.sdk.testing import Session

DSN = os.environ.get("STREAMWRIGHT_TEST_MYSQL_DSN")


def run_sql(connection, *statements):
    """Runs statements on the server through a writable DuckDB attach (no MySQL driver needed)."""
    session = Session(writable=["my"])
    try:
        session.load_extension("mysql")
        secret = session.create_secret("my", "mysql", client.secret_options(client.parameters(connection)))
        session.attach("my", "", kind="mysql", secret=secret, read_only=False)
        return [session.con.execute("CALL mysql_execute('my', ?)", [statement]) for statement in statements]
    finally:
        session.close()


@pytest.fixture
def mysql():
    """(table, structured connection) of a throwaway `orders` table on STREAMWRIGHT_TEST_MYSQL_DSN."""
    if not DSN:
        pytest.skip("set STREAMWRIGHT_TEST_MYSQL_DSN to a throwaway MySQL")
    values = dict(client.parameters({"dsn": DSN}))
    connection = {"host": values["host"], "port": int(values["port"]), "database": values["database"],
                  "user": values["user"], "password": values["password"]}
    table = "mg_db_%s" % uuid.uuid4().hex[:10]
    run_sql(connection, "CREATE TABLE %s (id INT PRIMARY KEY, status VARCHAR(40), total DECIMAL(20,2))" % table,
            "INSERT INTO %s VALUES (1, 'it''s new', 1234567890123456.78), (2, 'paid', 5)" % table)
    yield table, connection
    run_sql(connection, *("DROP TABLE IF EXISTS %s%s" % (table, suffix) for suffix in ("", "_copy", "_count")))
