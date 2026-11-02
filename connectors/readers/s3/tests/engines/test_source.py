"""The s3 source adapter (streamwright run), against an offline stand-in for the store."""

import json
import os
from types import MappingProxyType

import pytest
import yaml

from streamwright.connectors.s3 import CONNECTOR, client
from streamwright.core.sdk import ConnectorContext, ConnectorError, Redactor
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.testing import Listener, SourceAdapterContract, components, patch_object_storage, run_cli

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))
EXAMPLE = os.path.join(REPO_ROOT, "examples", "sources", "readers", "s3_demo")
KEY_ID = "AKIAEXAMPLEKEYID0042"
SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYsecret42"
TOKEN = "session-token-0042-secret"
CONNECTION = MappingProxyType({
    "roots": ["s3://acme-data/in/", "s3://lake/raw/"], "key_id": KEY_ID, "secret": SECRET, "session_token": TOKEN,
    "region": "us-east-1", "endpoint": "minio.local:9000", "url_style": "path", "use_ssl": False,
})


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
    storage.serve(tmp_path / "lake", "s3://acme-data/in/")
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


def test_the_secret_holds_credentials_and_settings_as_bound_parameters_scoped_to_the_roots(storage):
    open_session().close()
    secrets = [(statement, parameters) for statement, parameters in storage.statements if "SECRET" in statement]
    assert secrets == [(
        "CREATE OR REPLACE TEMPORARY SECRET streamwright_s3 (TYPE s3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, "
        "REGION ?, ENDPOINT ?, URL_STYLE ?, USE_SSL ?, SCOPE ?)",
        [KEY_ID, SECRET, TOKEN, "us-east-1", "minio.local:9000", "path", False,
         ["s3://acme-data/in/", "s3://lake/raw/"]])]


def test_a_folder_and_a_match_read_the_matching_keys(lake):
    session = open_session()
    try:
        assert read(session, {"path": "s3://acme-data/in/", "match": r".*\.csv", "recursive": True,
                              "format": "csv"}) == [{"id": "1", "name": "Ada"}, {"id": "2", "name": "Lin"},
                                                    {"id": "3", "name": "Kay"}]
    finally:
        session.close()


def test_missing_objects_are_skipped_or_fail_as_on_missing_says(lake):
    session = open_session()
    try:
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
    session = open_session(dict(CONNECTION, endpoint="127.0.0.1:%d" % listener.port))
    try:
        for path in ("s3://acme-data/in/x.csv?s3_endpoint=127.0.0.1:%d" % listener.port, "s3://other/x.csv",
                     "../x.csv", "x.csv?s3_endpoint=evil"):
            with pytest.raises(ConnectorError) as caught:
                read(session, {"path": path})
            assert caught.value.code == "ACCESS_DENIED", path
    finally:
        session.close()
    assert storage.reads() == [] and not listener.connected()


def serve_example(storage, folder):
    """Objects for the s3_demo example under its default root, s3://acme-data/exports/."""
    write(folder / "orders" / "east.csv", "order_id,quantity,amount,ordered_on\n1001,2,19.98,2026-10-01\n"
                                          "1002,1,99999999.99,2026-10-02\n")
    write(folder / "orders" / "west.csv", "order_id,quantity,amount,ordered_on\n2001,5,5.00,2026-10-01\n")
    write(folder / "customers" / "2026-10-01.jsonl", '{"id": "cu_1", "name": "Ada", "signed_up": "2026-10-01"}\n')
    write(folder / "customers" / "2026-10-02.jsonl", '{"id": "cu_2", "name": "Lin", "signed_up": "2026-10-02"}\n')
    plain = storage.plain()
    for day, start in (("2026-10-01", 1), ("2026-10-02", 3)):
        target = folder / "events" / ("day=" + day)
        target.mkdir(parents=True, exist_ok=True)
        plain.execute("COPY (SELECT * FROM (VALUES (%d, 'view', 1.25::DECIMAL(12,2)), "
                      "(%d, 'buy', 10.50::DECIMAL(12,2))) t(event_id, kind, value)) TO '%s' (FORMAT parquet)" % (
                          start, start + 1, target / "part-0.parquet"))
    write(folder / "events" / "day=2026-10-01" / "_SUCCESS", "")
    write(folder / "events" / "README.txt", "not an event")
    storage.serve(folder, "s3://acme-data/exports/")


def test_the_s3_demo_runs_offline_through_the_installed_definition(storage, tmp_path, monkeypatch):
    components.unregister("s3")  # load it from the entry point, as streamwright run does
    serve_example(storage, tmp_path / "lake")
    monkeypatch.setenv("STREAMWRIGHT_SECRET_S3_KEY_ID", KEY_ID)
    monkeypatch.setenv("STREAMWRIGHT_SECRET_S3_SECRET", SECRET)
    out = tmp_path / "out"
    assert run_cli(["run", EXAMPLE, "--allow-connector", "s3", "--set", "start_date=2026-10-01", "--output",
                    "jsonl:%s" % out]) == 0
    written = {}
    for name in os.listdir(str(out)):
        if name != "state.json":
            with open(str(out / name)) as stream:
                written[name.split(".")[0]] = [json.loads(line) for line in stream]
    assert sorted(written) == ["customers", "events", "orders"]
    assert [(record["order_id"], record["region"], record["amount"]) for record in written["orders"]] == [
        (1001, "east", 19.98), (1002, "east", 99999999.99), (2001, "west", 5)]
    assert written["events"][0] == {"event_id": 1, "kind": "view", "value": 1.25, "day": "2026-10-01"}
    secret = next(parameters for statement, parameters in storage.statements if "SECRET" in statement)
    assert secret == [KEY_ID, SECRET, "us-east-1", True, ["s3://acme-data/exports/"]]


def test_streamwright_validate_checks_the_declared_source(storage, capsys, tmp_path):
    assert run_cli(["validate", EXAMPLE]) == 0
    assert storage.statements == []  # validation never connects
    with open(os.path.join(EXAMPLE, "source.yaml")) as stream:
        document = yaml.safe_load(stream)
    with open(os.path.join(EXAMPLE, "streams", "orders.yaml")) as stream:
        orders = yaml.safe_load(stream)
    orders["requests"][0]["arguments"]["path"] = "orders/east.csv?s3_endpoint=evil.example.com"
    document["streams"] = [dict(orders, name="orders")]
    document["auth"]["secret"] = "wJalrXUtnFEMIwrittenInTheSource"
    bad = tmp_path / "source.yaml"
    bad.write_text(yaml.safe_dump(document, sort_keys=False))
    assert run_cli(["validate", str(bad)]) == 1
    output = capsys.readouterr().out
    assert "arguments.path: 'orders/east.csv?s3_endpoint=evil.example.com' does not match" in output
    assert "auth.secret: must be one {{ secrets.NAME }} reference" in output
    assert "wJalrXUtnFEMIwrittenInTheSource" not in output
