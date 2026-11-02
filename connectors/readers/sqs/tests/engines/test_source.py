"""The SQS source session follows the source adapter protocol."""

from types import SimpleNamespace

from streamwright.connectors.sqs import CONNECTOR, client
from streamwright.core.sdk.engines.source import SourceAdapter, SourceSession


def test_source_session_opens_and_reads(monkeypatch):
    monkeypatch.setattr(client, "peek", lambda connection, arguments, context: iter([[{"message_id": "one"}]]))
    session = CONNECTOR.load_adapter("source").open({"region": "us-east-1"}, SimpleNamespace())
    assert isinstance(CONNECTOR.load_adapter("source"), SourceAdapter)
    assert isinstance(session, SourceSession)
    assert list(session.read(CONNECTOR.operation("message", "read"), {"destination": "queue"}, SimpleNamespace())) == [
        [{"message_id": "one"}]]
    session.close()
