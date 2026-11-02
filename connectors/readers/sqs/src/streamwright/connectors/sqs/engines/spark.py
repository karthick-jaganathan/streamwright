"""SQS broker binding for the Spark batch runner."""

from streamwright.connectors.sqs import client

__all__ = ["SqsSparkAdapter"]


class SqsSparkAdapter:
    def hadoop_settings(self, name, connection):
        return {}

    def open_broker(self, connection):
        return client.SqsClient(connection)
