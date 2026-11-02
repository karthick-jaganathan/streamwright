"""The azure_blob source adapter (streamwright run), against an offline stand-in for the store."""

import os
from types import MappingProxyType

import pytest

from streamwright.connectors.azure_blob import CONNECTOR, client
from streamwright.core.sdk import ConnectorContext, ConnectorError, Redactor
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.testing import SourceAdapterContract, patch_object_storage, run_cli

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))
EXAMPLE = os.path.join(REPO_ROOT, "examples", "sources", "readers", "azure_blob_demo")
KEY = "dGVzdGtleQ==0042"
CONNECTION_STRING = ("DefaultEndpointsProtocol=https;AccountName=testacc;AccountKey=%s;"
                     "EndpointSuffix=core.windows.net;" % KEY)
CONNECTION = MappingProxyType({"roots": ["azure://acme-data/in/", "azure://lake/raw/"],
                               "connection_string": CONNECTION_STRING})


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


@pytest.fixture
def storage(monkeypatch):
    state = patch_object_storage(monkeypatch, client.STORE, client.STORE.extension)
    yield state
    if state._plain is not None:
        state._plain.close()


@pytest.fixture
def lake(storage, tmp_path):
    write(tmp_path / "lake" / "a.csv", "id,name\n1,Ada\n2,Lin\n")
    write(tmp_path / "lake" / "sub" / "b.csv", "id,name\n3,Kay\n")
    storage.serve(tmp_path / "lake", "azure://acme-data/in/")
    return storage


def open_session(connection=None):
    resolved = resolve_values(CONNECTOR, connection or CONNECTION)
    context = ConnectorContext(CONNECTOR.name, Redactor(resolved.secret_values))
    return CONNECTOR.load_adapter("source").open(resolved, context), context


def read(session, arguments):
    context = ConnectorContext(CONNECTOR.name, Redactor(()))
    return [record for page in session.read(CONNECTOR.operation("object", "read"), arguments, context)
            for record in page]


class TestSourceAdapter(SourceAdapterContract):
    __test__ = True
    definition = CONNECTOR
    connection = CONNECTION
    read_call = (("object", "read"), MappingProxyType({"path": "*.csv", "format": "csv"}))
    expected_records = ({"id": "1", "name": "Ada"}, {"id": "2", "name": "Lin"})

    @pytest.fixture
    def backend(self, lake):
        return lake


def test_a_connection_string_is_the_secret_scoped_to_the_roots(storage):
    open_session()[0].close()
    secret = next(parameters for statement, parameters in storage.statements if "SECRET" in statement)
    assert secret == [CONNECTION_STRING, ["azure://acme-data/in/", "azure://lake/raw/"]]


def test_an_account_and_its_key_are_sent_as_a_connection_string_built_for_the_secret(storage):
    open_session({"roots": ["azure://c/"], "account_name": "acc", "account_key": "mykey-0042-secret"})[0].close()
    secret = next(parameters for statement, parameters in storage.statements if "SECRET" in statement)
    assert secret[0].startswith("DefaultEndpointsProtocol=https;AccountName=acc;AccountKey=mykey-0042-secret;")


def test_abfss_urls_are_read_like_azure_urls(storage, tmp_path):
    write(tmp_path / "a.csv", "id\n1\n")
    storage.objects = {"abfss://acme-data/in/a.csv": str(tmp_path / "a.csv")}
    session, _ = open_session({"roots": ["abfss://acme-data/in/"], "account_name": "acc"})
    try:
        assert read(session, {"path": "a.csv"}) == [{"id": "1"}]
        with pytest.raises(ConnectorError, match="outside the roots"):
            read(session, {"path": "azure://acme-data/in/a.csv"})
    finally:
        session.close()


def test_a_blob_that_is_missing_is_skipped_or_fails_as_on_missing_says(lake):
    session, _ = open_session()
    try:
        assert read(session, {"path": "none/*.csv"}) == []
        with pytest.raises(ConnectorError) as caught:
            read(session, {"path": "none/*.csv", "on_missing": "error"})
        assert caught.value.code == "NOT_FOUND"
    finally:
        session.close()


def test_the_demo_validates_offline(storage):
    assert run_cli(["validate", EXAMPLE]) == 0
    assert storage.statements == []  # validation never connects
