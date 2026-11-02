"""Kafka source session delegates vendor reads through its source context."""

from types import SimpleNamespace

from streamwright.connectors.kafka import CONNECTOR, client
from streamwright.core.runtime.source_connectors import check_call
from streamwright.core.sdk.engines.source import SourceAdapter, SourceSession


def test_source_adapter_opens_a_protocol_session(monkeypatch):
    monkeypatch.setattr(client, "peek", lambda connection, arguments, context: iter([[{"offset": 1}]]))
    session = CONNECTOR.load_adapter("source").open({"bootstrap_servers": "broker"}, SimpleNamespace())
    assert isinstance(CONNECTOR.load_adapter("source"), SourceAdapter)
    assert isinstance(session, SourceSession)
    pages = session.read(CONNECTOR.operation("message", "read"), {"destination": "arrivals"}, SimpleNamespace())
    assert list(pages) == [[{"offset": 1}]]
    session.close()


def test_source_engine_accepts_read_only_message_calls():
    connector = CONNECTOR
    assert check_call(connector, {"service": "message", "method": "read",
                                    "arguments": {"destination": "arrivals", "max_wait": "1s"}}) == []
    assert check_call(connector, {"service": "message", "method": "consume",
                                    "arguments": {"destination": "arrivals", "group_id": "workers"}})
