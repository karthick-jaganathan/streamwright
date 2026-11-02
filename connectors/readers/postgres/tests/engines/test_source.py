"""The postgres source adapter on a real PostgreSQL (STREAMWRIGHT_TEST_PG_DSN): reads, refusals, secrets."""

from types import MappingProxyType

import pytest

from streamwright.connectors.postgres import CONNECTOR
from streamwright.core.sdk import ConnectorContext, ConnectorError, Redactor
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.testing import SourceAdapterContract

INJECTION = "'; DROP TABLE orders; --"  # the conftest's first order status


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
    def database(self, postgres):
        schema, connection = postgres
        self.connection = MappingProxyType(connection)
        self.read_call = (("database", "query"), {"query": "SELECT id, status FROM %s.orders WHERE id = $id" % schema,
                                                  "params": {"id": 2}})


def test_values_are_bound_never_pasted_and_numerics_stay_exact(postgres):
    schema, connection = postgres
    records = read(connection, "query", {"query": "SELECT id, total, updated_at FROM %s.orders WHERE status = $s"
                                                  % schema, "params": {"s": INJECTION}})
    assert records == [{"id": 1, "total": "1234567890123456.78",  # an unconstrained NUMERIC: exact text
                        "updated_at": "2026-10-04 08:00:00+00"}]


def test_table_reads_select_columns_and_conditions(postgres):
    schema, connection = postgres
    assert read(connection, "table", {"schema": schema, "table": "orders", "columns": ["id"],
                                      "where": [{"column": "id", "op": "IN", "value": [2, 3]}]}) == [{"id": 2}]


def test_a_dsn_connection_with_options_and_a_server_timeout_reads(postgres):
    schema, connection = postgres
    dsn = "host=%s port=%d dbname=%s user=%s password='%s'" % (
        connection["host"], connection["port"], connection["database"], connection["user"], connection["password"])
    records = read({"dsn": dsn, "statement_timeout": "30s"}, "table", {"schema": schema, "table": "orders",
                                                                      "columns": ["id"]})
    assert sorted(record["id"] for record in records) == [1, 2]


@pytest.mark.parametrize("query", ["SELECT * FROM duckdb_secrets()", "SELECT * FROM duckdb_databases()",
                                   "SELECT * FROM src.pg_catalog.pg_authid", "SELECT * FROM information_schema.tables",
                                   "SELECT * FROM duckdb_settings()"])
def test_a_run_cannot_read_the_connection_or_system_views(postgres, query):
    _, connection = postgres
    with pytest.raises(ConnectorError) as caught:
        read(connection, "query", {"query": query})
    assert caught.value.code == "QUERY_REFUSED" and connection["password"] not in str(caught.value)


def test_names_longer_than_postgres_takes_are_refused_before_reading(postgres):
    schema, connection = postgres
    with pytest.raises(ConnectorError, match="longer than 63 bytes"):
        read(connection, "table", {"schema": schema, "table": "x" * 64})


def test_a_failing_attach_never_shows_the_password(postgres):
    _, connection = postgres
    secret = "Wr0ng-Secret-pw"
    with pytest.raises(ConnectorError) as caught:
        read(dict(connection, password=secret), "query", {"query": "SELECT 1"})
    assert caught.value.code == "CONNECT_ERROR" and secret not in str(caught.value)


def test_an_unreachable_server_is_retryable():
    with pytest.raises(ConnectorError) as caught:
        read({"host": "127.0.0.1", "port": 1, "database": "d", "user": "u", "password": "Wr0ng-Secret-pw"},
             "query", {"query": "SELECT 1"})
    assert caught.value.code == "CONNECT_ERROR" and caught.value.retryable
