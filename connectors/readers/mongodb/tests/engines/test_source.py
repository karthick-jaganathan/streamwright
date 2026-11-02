"""The mongodb source adapter on a real MongoDB (STREAMWRIGHT_TEST_MONGODB_URI): find, aggregate, errors."""

from types import MappingProxyType

import pytest

from streamwright.connectors.mongodb import CONNECTOR
from streamwright.connectors.mongodb.engines.source import MongoDBSession
from streamwright.core.sdk import ConnectorContext, ConnectorError, Redactor
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.testing import SourceAdapterContract


def pages(connection, method, arguments, retry=None):
    resolved = resolve_values(CONNECTOR, connection)
    context = ConnectorContext(CONNECTOR.name, Redactor(resolved.secret_values), retry)
    session = CONNECTOR.load_adapter("source").open(resolved, context)
    try:
        return list(session.read(CONNECTOR.operation("collection", method), arguments, context))
    finally:
        session.close()


class TestSourceAdapter(SourceAdapterContract):
    __test__ = True
    definition = CONNECTOR
    read_call = (("collection", "find"), {"collection": "users", "filter": {"status": "active"},
                                          "projection": {"_id": 0, "name": 1}, "sort": [["name", 1]]})
    expected_records = ({"name": "Ada"}, {"name": "Cy"})

    @pytest.fixture(autouse=True)
    def database(self, mongo):
        self.connection = MappingProxyType({"uri": mongo[1]})


def test_documents_are_json_and_pages_follow_batch_size(mongo):
    _, uri = mongo
    found = pages({"uri": uri}, "find", {"collection": "users", "sort": [["_id", 1]], "batch_size": 2})
    assert [len(page) for page in found] == [2, 1]
    assert found[0][0] == {"_id": "65f000000000000000000001", "name": "Ada", "status": "active",
                           "since": "2026-10-05T08:00:00", "spend": 12.5}


def test_an_aggregate_reads_its_pipeline(mongo):
    database, uri = mongo
    host = uri.rsplit("/", 1)[0].split("://", 1)[1]
    connection = {"host": host.split(":")[0], "port": int(host.split(":")[1]), "database": database}
    found = pages(connection, "aggregate", {"collection": "users", "pipeline": [
        {"$group": {"_id": "$status", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]})
    assert found == [[{"_id": "active", "n": 2}, {"_id": "inactive", "n": 1}]]


def test_spark_options_are_refused_and_an_unreachable_server_is_retryable():
    with pytest.raises(ConnectorError, match="`options` are the Spark connector's"):
        pages({"uri": "mongodb://127.0.0.1:1/d"}, "find", {"collection": "c", "options": {"x": 1}})
    with pytest.raises(ConnectorError) as caught:
        pages({"uri": "mongodb://u:S3cret-pw@127.0.0.1:1/d?serverSelectionTimeoutMS=200"}, "find",
              {"collection": "c"}, retry={"max_attempts": 1, "backoff": "constant"})
    assert caught.value.retryable and "S3cret-pw" not in str(caught.value)


class _FakeCollection:
    def __init__(self, recorder):
        self.recorder = recorder

    def find(self, filter, projection=None, **kwargs):
        self.recorder.append(("find", kwargs))
        return iter([])

    def aggregate(self, pipeline, **kwargs):
        self.recorder.append(("aggregate", kwargs))
        return iter([])


class _FakeDatabase:
    def __init__(self, recorder):
        self.recorder = recorder

    def __getitem__(self, _collection):
        return _FakeCollection(self.recorder)


class _FakeMongo:
    def __init__(self, recorder):
        self.recorder = recorder

    def __getitem__(self, _database):
        return _FakeDatabase(self.recorder)

    def close(self):
        pass


def test_vendor_arguments_reach_pymongo_find_and_aggregate():
    recorder = []
    session = MongoDBSession(_FakeMongo(recorder), "db")
    find = CONNECTOR.operation("collection", "find")
    list(session._cursor(find, {"collection": "users", "filter": {}, "hint": "status_1", "max_time_ms": 500}))
    assert recorder[-1] == ("find", {"hint": "status_1", "max_time_ms": 500})
    aggregate = CONNECTOR.operation("collection", "aggregate")
    list(session._cursor(aggregate, {"collection": "users", "pipeline": [], "allowDiskUse": True}))
    assert recorder[-1] == ("aggregate", {"allowDiskUse": True})
