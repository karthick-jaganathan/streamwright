"""The mysql source adapter on a real MySQL (STREAMWRIGHT_TEST_MYSQL_DSN): reads, refusals, secrets, timeouts."""

from types import MappingProxyType

import pytest

from streamwright.connectors.mysql import CONNECTOR
from streamwright.core.sdk import ConnectorContext, ConnectorError, Redactor
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.testing import SourceAdapterContract


def read(connection, method, arguments):
    resolved = resolve_values(CONNECTOR, connection)
    context = ConnectorContext(CONNECTOR.name, Redactor(resolved.secret_values))
    session = CONNECTOR.load_adapter("source").open(resolved, context)
    try:
        return [record for page in session.read(CONNECTOR.operation("database", method), arguments, context)
                for record in page]
    finally:
        session.close()


class TestSourceAdapter(SourceAdapterContract):
    __test__ = True
    definition = CONNECTOR
    expected_records = ({"id": 2, "status": "paid"},)

    @pytest.fixture(autouse=True)
    def database(self, mysql):
        table, connection = mysql
        self.connection = MappingProxyType(connection)
        self.read_call = (("database", "query"), {"query": "SELECT id, status FROM %s WHERE id = $id" % table,
                                                  "params": {"id": 2}})


def test_values_are_bound_and_tables_default_to_the_database(mysql):
    table, connection = mysql
    assert read(connection, "query", {"query": "SELECT id FROM %s WHERE status = $s" % table,
                                      "params": {"s": "it's new"}}) == [{"id": 1}]
    assert read(dict(connection, statement_timeout=30), "table", {"table": table, "columns": ["id"], "where": [
        {"column": "total", "op": ">", "value": 10}]}) == [{"id": 1}]


@pytest.mark.parametrize("query", ["SELECT * FROM mysql.user", "SELECT * FROM performance_schema.threads",
                                   "SELECT * FROM duckdb_secrets()"])
def test_a_run_cannot_read_system_schemas_or_the_secret(mysql, query):
    _, connection = mysql
    with pytest.raises(ConnectorError) as caught:
        read(connection, "query", {"query": query})
    assert caught.value.code == "QUERY_REFUSED"


def test_a_dsn_connection_reads_and_a_wrong_password_never_shows(mysql):
    table, connection = mysql
    dsn = "host=%s port=%d database=%s user=%s password='%s'" % (
        connection["host"], connection["port"], connection["database"], connection["user"], connection["password"])
    assert read({"dsn": dsn}, "table", {"table": table, "columns": ["id"], "where": [
        {"column": "id", "op": "=", "value": 2}]}) == [{"id": 2}]
    with pytest.raises(ConnectorError) as caught:
        read(dict(connection, password="Wr0ng-Secret-pw"), "query", {"query": "SELECT 1"})
    assert caught.value.code == "CONNECT_ERROR" and "Wr0ng-Secret-pw" not in str(caught.value)
