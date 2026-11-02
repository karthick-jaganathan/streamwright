import os
import uuid

import pytest

from streamwright.connectors.postgres import client

DSN = os.environ.get("STREAMWRIGHT_TEST_PG_DSN")
INJECTION = "'; DROP TABLE orders; --"


@pytest.fixture
def postgres():
    """(schema, structured connection) of a throwaway schema on STREAMWRIGHT_TEST_PG_DSN with an `orders` table."""
    if not DSN:
        pytest.skip("set STREAMWRIGHT_TEST_PG_DSN to a throwaway PostgreSQL")
    psycopg = pytest.importorskip("psycopg")
    values = dict(client.parameters({"dsn": DSN}))
    schema = "mg_db_%s" % uuid.uuid4().hex[:10]
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute('CREATE SCHEMA "%s"' % schema)
        conn.execute('CREATE TABLE "%s".orders (id INTEGER PRIMARY KEY, status TEXT, total NUMERIC, '
                     'updated_at TIMESTAMPTZ)' % schema)
        conn.execute('INSERT INTO "%s".orders VALUES (1, %%s, 1234567890123456.78, \'2026-10-04 10:00+02\'), '
                     '(2, \'paid\', 5, \'2026-10-05 00:00+00\')' % schema, [INJECTION])
    connection = {"host": values["host"], "port": int(values["port"]), "database": values["database"],
                  "user": values["user"], "password": values["password"]}
    yield schema, connection
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute('DROP SCHEMA IF EXISTS "%s" CASCADE' % schema)
        conn.execute('DROP SCHEMA IF EXISTS "%s_state" CASCADE' % schema)
