"""SQS Spark broker is loaded from its definition."""

from streamwright.connectors.sqs import CONNECTOR, client
from streamwright.connectors.sqs.client import SqsClient
from streamwright.spark.runtime.definition import bind


def test_spark_definition_opens_broker(monkeypatch):
    monkeypatch.setattr(client.boto3, "client", lambda service, **kwargs: object())
    adapter = bind(CONNECTOR, "sqs")
    assert adapter.operation("message", "publish")
    assert adapter.operation("message", "read") is None
    assert isinstance(adapter.adapter.open_broker({"region": "us-east-1"}), SqsClient)
