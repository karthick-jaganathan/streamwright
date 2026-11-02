"""The postgres DuckDB adapter on a real PostgreSQL: scoped URL secret, read-only unless written, merges, state."""

import os
from types import MappingProxyType

import psycopg
import pytest

from streamwright.connectors.postgres import CONNECTOR
from streamwright.core.jobs.duckdb.definition_adapter import DefinitionDuckDBAdapter
from streamwright.core.sdk.testing import DuckDBAdapterContract, DuckDBRules, Session, check_job, run_job

DSN = os.environ.get("STREAMWRIGHT_TEST_PG_DSN")


class TestDuckDBAdapter(DuckDBAdapterContract):
    __test__ = True
    definition = CONNECTOR
    expected_records = ({"id": 2},)

    @pytest.fixture(autouse=True)
    def database(self, postgres):
        schema, connection = postgres
        self.connection = MappingProxyType(connection)
        self.read_arguments = MappingProxyType({"query": "SELECT id FROM %s.orders WHERE id = 2" % schema})


def _job(schema, connection, outputs, state=True):
    job = {"kind": "DuckDBJob", "name": "pg_job", "spec": {"secrets": {"pg_password": {"type": "string"}}},
           "connections": {"pg": dict(connection, provider="postgres", password="{{ secrets.pg_password }}")},
           "steps": [{"name": "existing", "connection": "pg", "read": "table", "schema": schema, "table": "orders"},
                     {"name": "counted", "connection": "pg", "read": "query",
                      "query": "SELECT count(*) AS n FROM %s.orders" % schema},
                     {"name": "changes", "sql": "SELECT 1 AS id, 'new' AS status UNION ALL SELECT 3, 'added'"}],
           "outputs": outputs}
    if state:
        job["runtime"] = {"state": {"connection": "pg", "schema": "%s_state" % schema}}
    return job


def _rows(schema):
    with psycopg.connect(DSN) as conn:
        return conn.execute('SELECT id, status FROM "%s".orders ORDER BY id' % schema).fetchall()


def test_merge_append_and_overwrite_through_the_attached_database_with_state_in_it(postgres):
    schema, connection = postgres
    merge = {"name": "upsert", "connection": "pg", "step": "changes", "table": "%s.orders" % schema,
             "mode": "merge", "primary_key": ["id"]}
    copy = {"name": "copy", "connection": "pg", "step": "existing", "table": "%s.copied" % schema,
            "mode": "overwrite"}
    secrets = {"pg_password": connection["password"]}
    result = run_job(_job(schema, connection, [merge, copy]), secrets=secrets)
    assert result.exit_code == 0, result.error
    assert _rows(schema) == [(1, "new"), (2, "paid"), (3, "added")]
    with psycopg.connect(DSN) as conn:
        assert conn.execute('SELECT count(*) FROM "%s".copied' % schema).fetchone() == (3,)
        assert conn.execute('SELECT count(*) FROM "%s_state".batches' % schema).fetchone() == (1,)
    append = dict(merge, name="again", mode="append", table="%s.copied" % schema, step="changes")
    del append["primary_key"]
    assert run_job(_job(schema, connection, [append]), secrets=secrets).exit_code == 0


def test_a_connection_no_output_writes_to_is_attached_read_only(postgres):
    schema, connection = postgres
    adapter = CONNECTOR.load_adapter("duckdb")
    session = Session()
    try:
        adapter.configure(session, "pg", DefinitionDuckDBAdapter(CONNECTOR).resolve(connection))
        with pytest.raises(Exception, match=r"read-only|read only"):
            session.con.execute('INSERT INTO pg."%s".orders VALUES (9, \'x\', 1, NULL)' % schema)
        assert connection["password"] not in str(session.con.execute("SELECT * FROM duckdb_secrets()").fetchall())
    finally:
        session.close()


def test_jobs_are_checked_from_the_spec(postgres):
    schema, connection = postgres
    rules = DuckDBRules({"postgres": DefinitionDuckDBAdapter(CONNECTOR)}, connectors=())
    out = {"name": "o", "connection": "pg", "step": "changes", "table": "t", "mode": "merge"}
    found = [(issue.code, issue.path) for issue in check_job(_job(schema, connection, [out], state=False), rules)]
    assert found == [("bad-value", "outputs[0]")]
    delete = dict(out, mode="delete", primary_key=["id"])
    found = [(issue.code, issue.path) for issue in check_job(_job(schema, connection, [delete], state=False), rules)]
    assert found == [("unsupported-mode", "outputs[0]")]
    assert DefinitionDuckDBAdapter(CONNECTOR).holds_state
