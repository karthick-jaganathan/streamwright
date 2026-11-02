"""Kafka Spark broker is loaded from the definition."""

from streamwright.connectors.kafka import CONNECTOR
from streamwright.connectors.kafka.client import KafkaClient
from streamwright.spark.runtime.definition import bind


def test_spark_definition_opens_broker():
    adapter = bind(CONNECTOR, "kafka")
    assert adapter.operation("message", "consume")
    assert adapter.operation("message", "read") is None
    broker = adapter.adapter.open_broker({"bootstrap_servers": "broker"})
    assert isinstance(broker, KafkaClient)
    assert broker.destination({"destination": "commits"}) == "commits"
