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
