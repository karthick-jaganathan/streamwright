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


def test_a_gcs_key_file_without_an_hmac_key_is_named_when_the_session_opens(lake):
    with pytest.raises(ConnectorError, match="gcs_key_file: DuckDB reads Google Cloud Storage only with an HMAC key"):
        read({"roots": [lake[0]], "gcs_key_file": "/keys/sa.json"}, "read", {"path": "cities"})


def test_an_hmac_key_becomes_a_gcs_secret_scoped_to_the_gs_roots_and_is_masked(lake):
    connection = resolve_values(CONNECTOR, {"roots": [lake[0], "gs://bucket/lake"], "gcs_key_file": "/keys/sa.json",
                                            "gcs_hmac_key_id": "GOOG1EKEYID", "gcs_hmac_secret": "hmacS3cr3t"})
    context = ConnectorContext(CONNECTOR.name, Redactor(connection.secret_values))
    session = CONNECTOR.load_adapter("source").open(connection, context)
    try:
        secrets = session.reader.connection.execute(
            "SELECT name, type, scope, secret_string FROM duckdb_secrets()").fetchall()
        assert [row[:3] for row in secrets] == [("streamwright_gcs", "gcs", ["gs://bucket/lake"])]
        assert "hmacS3cr3t" not in secrets[0][3]
        records = [record for page in session.read(CONNECTOR.operation("table", "read"), {"path": "cities"}, context)
                   for record in page]
        assert len(records) == 3
        with pytest.raises(ConnectorError) as caught:
            list(session.read(CONNECTOR.operation("table", "read"), {"path": "gs://other/t"}, context))
    finally:
        session.close()
    assert "GOOG1EKEYID" not in str(caught.value) and "hmacS3cr3t" not in str(caught.value)
    assert "GOOG1EKEYID" not in context.redact("id GOOG1EKEYID") and "hmacS3cr3t" not in context.redact("hmacS3cr3t")
