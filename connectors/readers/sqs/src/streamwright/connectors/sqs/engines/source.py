"""Read-only SQS queue inspection on the source engine."""

from streamwright.connectors.sqs import client

__all__ = ["SqsSourceAdapter"]


class SqsSourceSession:
    def __init__(self, connection):
        self.connection = connection

    def read(self, operation, arguments, context):
        yield from client.peek(self.connection, arguments, context)

    def close(self):
        pass


class SqsSourceAdapter:
    def open(self, connection, context):
        return SqsSourceSession(connection)
