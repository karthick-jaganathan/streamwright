"""Read-only Kafka topic inspection on the source engine."""

from streamwright.connectors.kafka import client

__all__ = ["KafkaSourceAdapter"]


class KafkaSourceSession:
    def __init__(self, connection):
        self.connection = connection

    def read(self, operation, arguments, context):
        yield from client.peek(self.connection, arguments, context)

    def close(self):
        pass


class KafkaSourceAdapter:
    def open(self, connection, context):
        return KafkaSourceSession(connection)
