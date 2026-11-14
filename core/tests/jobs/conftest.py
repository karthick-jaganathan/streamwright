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
import uuid

import pytest

PG_DSN = os.environ.get("STREAMWRIGHT_TEST_PG_DSN")


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
