"""SQS's declared connection and messaging operations."""

from streamwright.core.sdk.contract import ConnectorDefinition, Field
from streamwright.core.sdk.operations import messages

__all__ = ["CONNECTOR"]

_DESTINATION = Field("destination", "string", required=True,
                     pattern=r"^https?://[^\s/]+/[0-9]{12}/[A-Za-z0-9_.-]{1,80}$",
                     description="the URL of an SQS queue")
_OPTIONS = Field("options", "mapping", description="additional SQS request parameters")

CONNECTOR = ConnectorDefinition(
    name="sqs", title="Amazon SQS", family="messaging",
    summary="Read and consume queue messages and publish events through Amazon SQS",
    connection=(
        Field("region", "string", required=True),
        Field("endpoint", "string", pattern=r"^https?://[^\s]+$"),
        Field("key_id", "string", secret=True), Field("secret", "string", secret=True),
        Field("session_token", "string", secret=True),
    ),
    operations=(
        messages.READ.with_arguments(
            _DESTINATION,
            Field("max_messages", "integer", aliases=("MaxNumberOfMessages",),
                  description="messages per receive call"),
            Field("max_wait", "duration", aliases=("WaitTimeSeconds",), description="long-poll wait"),
        ),
        messages.CONSUME.with_arguments(
            _DESTINATION,
            Field("visibility_timeout", "integer", aliases=("VisibilityTimeout",),
                  description="visibility timeout in seconds"),
            Field("max_messages", "integer", aliases=("MaxNumberOfMessages",),
                  description="messages per receive call"),
            Field("max_wait", "duration", aliases=("WaitTimeSeconds",), description="long-poll wait"),
            _OPTIONS,
        ),
        messages.PUBLISH.with_arguments(
            _DESTINATION, Field("group_id", "string", description="FIFO message group"), _OPTIONS,
        ),
    ),
    adapters={
        "source": "streamwright.connectors.sqs.engines.source:SqsSourceAdapter",
        "spark": "streamwright.connectors.sqs.engines.spark:SqsSparkAdapter",
    },
    validator="streamwright.connectors.sqs.client:validate_connection",
)
