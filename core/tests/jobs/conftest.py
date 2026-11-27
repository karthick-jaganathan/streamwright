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

import os
import urllib.parse
import uuid

import pytest

PG_DSN = os.environ.get("STREAMWRIGHT_TEST_PG_DSN")
MYSQL_URL = os.environ.get("STREAMWRIGHT_TEST_MYSQL_URL")      # mysql://user:pass@host:3306/db


@pytest.fixture
def pg_schema():
    """A throw-away PostgreSQL schema name (dropped afterwards); skips without STREAMWRIGHT_TEST_PG_DSN."""
    if not PG_DSN:
        pytest.skip("set STREAMWRIGHT_TEST_PG_DSN to run PostgreSQL tests")
    psycopg = pytest.importorskip("psycopg")
    name = "ws_j_%s" % uuid.uuid4().hex[:12]
    yield name
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        conn.execute('DROP SCHEMA IF EXISTS "%s" CASCADE' % name)


@pytest.fixture
def mysql_database():
    """A throw-away MySQL database name (dropped afterwards); skips without STREAMWRIGHT_TEST_MYSQL_URL."""
    if not MYSQL_URL:
        pytest.skip("set STREAMWRIGHT_TEST_MYSQL_URL to run MySQL tests")
    pymysql = pytest.importorskip("pymysql")
    name = "ws_j_%s" % uuid.uuid4().hex[:12]
    yield name
    parsed = urllib.parse.urlparse(MYSQL_URL)
    conn = pymysql.connect(host=parsed.hostname, port=parsed.port or 3306, user=urllib.parse.unquote(parsed.username),
                           password=urllib.parse.unquote(parsed.password or ""), autocommit=True)
    try:
        conn.cursor().execute("DROP DATABASE IF EXISTS `%s`" % name)
    finally:
        conn.close()
