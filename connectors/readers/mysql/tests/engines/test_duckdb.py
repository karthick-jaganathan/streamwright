"""The mysql DuckDB adapter on a real MySQL: tables in the connection's database, merges, read-only unless written."""

from types import MappingProxyType

import pytest

from streamwright.connectors.mysql import CONNECTOR
from streamwright.core.sdk import ConnectorContext, Redactor
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.testing import DuckDBAdapterContract, run_job


def context():
    return ConnectorContext(CONNECTOR.name, Redactor(()))


class TestDuckDBAdapter(DuckDBAdapterContract):
    __test__ = True
    definition = CONNECTOR
    expected_records = ({"id": 2},)

    @pytest.fixture(autouse=True)
    def database(self, mysql):
        table, connection = mysql
        self.connection = MappingProxyType(connection)
        self.read_arguments = MappingProxyType({"query": "SELECT id FROM %s WHERE id = 2" % table})


def test_append_and_overwrite_into_the_connections_database(mysql):
    table, connection = mysql
    job = {"kind": "DuckDBJob", "name": "my_job", "spec": {"secrets": {"pw": {"type": "string"}}},
           "connections": {"my": dict(connection, provider="mysql", password="{{ secrets.pw }}")},
           "steps": [{"name": "changes", "sql": "SELECT 1 AS id, 'new' AS status, 1.5 AS total"},
                     {"name": "counted", "connection": "my", "read": "query",
                      "query": "SELECT count(*) AS n FROM %s" % table}],
           "outputs": [{"name": "add", "connection": "my", "step": "changes", "table": "%s_copy" % table,
                        "mode": "append"},
                       {"name": "copy", "connection": "my", "step": "counted", "table": "%s_count" % table,
                        "mode": "overwrite"}]}
    result = run_job(job, secrets={"pw": connection["password"]})
    assert result.exit_code == 0, result.error
    rows = CONNECTOR.load_adapter("source").open(resolve_values(CONNECTOR, connection), context())
    try:
        pages = rows.read(CONNECTOR.operation("database", "table"), {"table": "%s_count" % table}, context())
        found = [record for page in pages for record in page]
    finally:
        rows.close()
    assert found == [{"n": 2}]
