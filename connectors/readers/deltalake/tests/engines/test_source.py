"""The deltalake source adapter: scans and queries inside the roots, on a locked sandbox, with masked credentials."""

import os

import pytest

from streamwright.connectors.deltalake import CONNECTOR
from streamwright.core.sdk import ConnectorContext, ConnectorError, Redactor
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.testing import SourceAdapterContract


def read(connection, method, arguments):
    resolved = resolve_values(CONNECTOR, connection)
    context = ConnectorContext(CONNECTOR.name, Redactor(resolved.secret_values))
    session = CONNECTOR.load_adapter("source").open(resolved, context)
    try:
        return [record for page in session.read(CONNECTOR.operation("table", method), arguments, context)
                for record in page]
    finally:
        session.close()


class TestSourceAdapter(SourceAdapterContract):
    __test__ = True
    definition = CONNECTOR
    expected_records = ({"city": "Tokyo"},)

    @pytest.fixture(autouse=True)
    def tables(self, lake):
        self.connection = {"roots": [lake[0]]}
        self.read_call = (("table", "read"), {"path": "cities", "columns": ["city"],
                                              "where": [{"column": "id", "op": ">", "value": 2}]})


def test_a_query_binds_its_parameters_and_reads_the_calls_table_as_delta_table(lake):
    inside, _ = lake
    records = read({"roots": [inside]}, "query", {"query": "SELECT city FROM delta_table WHERE population > $n "
                                                           "ORDER BY id", "params": {"n": 8500000},
                                                  "path": os.path.join(inside, "cities")})
    assert records == [{"city": "London"}, {"city": "Tokyo"}]
    query = "SELECT count(*) AS n FROM delta_scan('%s')" % os.path.join(inside, "cities")
    assert read({"roots": [inside]}, "query", {"query": query}) == [{"n": 3}]


@pytest.mark.parametrize(("method", "arguments", "code"), [
    ("read", {"path": "ELSEWHERE"}, "ACCESS_DENIED"),
    ("read", {"path": "../elsewhere/cities"}, "ACCESS_DENIED"),
    ("query", {"query": "SELECT * FROM delta_scan('ELSEWHERE')"}, "QUERY_REFUSED"),
    ("query", {"query": "SELECT * FROM read_parquet('ELSEWHERE/*.parquet')"}, "QUERY_REFUSED"),
    ("query", {"query": "SELECT * FROM duckdb_settings()"}, "QUERY_REFUSED"),
])
def test_the_roots_confine_every_read(lake, method, arguments, code):
    inside, outside = lake
    arguments = {key: value.replace("ELSEWHERE", os.path.join(outside, "cities")) for key, value in arguments.items()}
    with pytest.raises(ConnectorError) as caught:
        read({"roots": [inside]}, method, arguments)
    assert caught.value.code == code


def test_the_sandbox_itself_refuses_files_outside_the_roots(lake):
    inside, outside = lake
    connection = resolve_values(CONNECTOR, {"roots": [inside]})
    session = CONNECTOR.load_adapter("source").open(connection, ConnectorContext(CONNECTOR.name, Redactor(())))
    try:
        with pytest.raises(Exception, match=r"(?i)permission|disabled|not allowed"):
            session.reader.connection.execute("SELECT * FROM delta_scan(?)", [os.path.join(outside, "cities")])
        with pytest.raises(Exception, match=r"(?i)lock"):
            session.reader.connection.execute("SET enable_external_access = true")
    finally:
        session.close()


def test_credentials_are_masked_including_the_account_key_of_a_connection_string(lake):
    inside, _ = lake
    connection = resolve_values(CONNECTOR, {"roots": [inside], "azure_connection_string":
                                            "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=S3cr3tKey==;"})
    context = ConnectorContext(CONNECTOR.name, Redactor(connection.secret_values))
    try:
        session = CONNECTOR.load_adapter("source").open(connection, context)
        session.close()
    except ConnectorError as exc:
        assert "S3cr3tKey" not in str(exc)
    assert "S3cr3tKey" not in context.redact("key S3cr3tKey== here")
