"""Kafka broker binding for the Spark batch runner."""

from streamwright.connectors.kafka import client

__all__ = ["KafkaSparkAdapter"]


class KafkaSparkAdapter:
    def hadoop_settings(self, name, connection):
        return {}

    def open_broker(self, connection):
        return client.KafkaClient(connection)
