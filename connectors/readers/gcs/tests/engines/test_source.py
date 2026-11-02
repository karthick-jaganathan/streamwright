"""The gcs source adapter (streamwright run), against an offline stand-in for the store."""

import json
import os
from types import MappingProxyType

import pytest
import yaml

from streamwright.connectors.gcs import CONNECTOR, client
from streamwright.core.sdk import ConnectorContext, ConnectorError, Redactor
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.testing import Listener, SourceAdapterContract, components, patch_object_storage, run_cli

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))
EXAMPLE = os.path.join(REPO_ROOT, "examples", "sources", "readers", "gcs_demo")
KEY_ID = "GOOGHMACKEYID0042"
SECRET = "hmac-secret-value-0042"
CONNECTION = MappingProxyType({"roots": ["gs://acme-data/in/", "gs://lake/raw/"], "key_id": KEY_ID, "secret": SECRET})


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
    storage.serve(tmp_path / "lake", "gs://acme-data/in/")
    return storage


def open_session(connection=None):
    resolved = resolve_values(CONNECTOR, connection or CONNECTION)
    context = ConnectorContext(CONNECTOR.name, Redactor(resolved.secret_values))
    return CONNECTOR.load_adapter("source").open(resolved, context)


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


def test_the_secret_holds_the_hmac_key_as_bound_parameters_scoped_to_the_roots(storage):
    open_session().close()
    secrets = [(statement, parameters) for statement, parameters in storage.statements if "SECRET" in statement]
    assert secrets == [("CREATE OR REPLACE TEMPORARY SECRET streamwright_gcs (TYPE gcs, KEY_ID ?, SECRET ?, SCOPE ?)",
                        [KEY_ID, SECRET, ["gs://acme-data/in/", "gs://lake/raw/"]])]


def test_a_folder_and_a_match_read_the_matching_keys(lake):
    session = open_session()
    try:
        assert read(session, {"path": "gs://acme-data/in/", "match": r".*\.csv", "recursive": True,
                              "format": "csv"}) == [{"id": "1", "name": "Ada"}, {"id": "2", "name": "Lin"},
                                                    {"id": "3", "name": "Kay"}]
        assert read(session, {"path": "none/*.csv"}) == []
        with pytest.raises(ConnectorError) as caught:
            read(session, {"path": "none/*.csv", "on_missing": "error"})
        assert caught.value.code == "NOT_FOUND"
    finally:
        session.close()


@pytest.fixture
def listener():
    result = Listener()
    yield result
    result.close()


def test_paths_outside_the_roots_or_with_a_query_are_refused_before_anything_is_read(storage, listener):
    session = open_session()
    try:
        for path in ("gs://acme-data/in/x.csv?s3_endpoint=127.0.0.1:%d" % listener.port, "gs://other/x.csv",
                     "gcs://acme-data/in/x.csv", "../x.csv"):
            with pytest.raises(ConnectorError) as caught:
                read(session, {"path": path})
            assert caught.value.code == "ACCESS_DENIED", path
    finally:
        session.close()
    assert storage.reads() == [] and not listener.connected()


def test_the_gcs_demo_runs_offline_through_the_installed_definition(storage, tmp_path, monkeypatch):
    components.unregister("gcs")  # load it from the entry point, as streamwright run does
    write(tmp_path / "lake" / "orders" / "east.csv", "order_id,quantity,amount,ordered_on\n1001,2,19.98,2026-10-01\n")
    plain = storage.plain()
    target = tmp_path / "lake" / "events" / "day=2026-10-01"
    target.mkdir(parents=True)
    plain.execute("COPY (SELECT 1 AS event_id, 'view' AS kind, 1.25::DECIMAL(12,2) AS value) TO '%s' (FORMAT parquet)" %
                  (target / "part-0.parquet"))
    write(tmp_path / "lake" / "events" / "README.txt", "not an event")
    storage.serve(tmp_path / "lake", "gs://acme-data/exports/")
    monkeypatch.setenv("STREAMWRIGHT_SECRET_GCS_KEY_ID", KEY_ID)
    monkeypatch.setenv("STREAMWRIGHT_SECRET_GCS_SECRET", SECRET)
    out = tmp_path / "out"
    assert run_cli(["run", EXAMPLE, "--allow-connector", "gcs", "--output", "jsonl:%s" % out]) == 0
    written = {}
    for name in os.listdir(str(out)):
        if name != "state.json":
            with open(str(out / name)) as stream:
                written[name.split(".")[0]] = [json.loads(line) for line in stream]
    assert [(record["order_id"], record["region"]) for record in written["orders"]] == [(1001, "east")]
    assert written["events"] == [{"event_id": 1, "kind": "view", "value": 1.25, "day": "2026-10-01"}]


def test_streamwright_validate_refuses_a_key_written_in_the_source(storage, capsys, tmp_path):
    assert run_cli(["validate", EXAMPLE]) == 0
    assert storage.statements == []  # validation never connects
    with open(os.path.join(EXAMPLE, "source.yaml")) as stream:
        document = yaml.safe_load(stream)
    with open(os.path.join(EXAMPLE, "streams", "orders.yaml")) as stream:
        orders = yaml.safe_load(stream)
    document["streams"] = [dict(orders, name="orders")]
    document["auth"]["secret"] = "hmac-written-in-the-source"
    bad = tmp_path / "source.yaml"
    bad.write_text(yaml.safe_dump(document, sort_keys=False))
    assert run_cli(["validate", str(bad)]) == 1
    output = capsys.readouterr().out
    assert "auth.secret: must be one {{ secrets.NAME }} reference" in output
    assert "hmac-written-in-the-source" not in output
