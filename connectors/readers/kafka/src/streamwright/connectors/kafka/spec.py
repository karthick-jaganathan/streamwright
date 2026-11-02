"""Kafka's declared connection and messaging operations."""

from streamwright.core.sdk.contract import ConnectorDefinition, Field
from streamwright.core.sdk.operations import messages

__all__ = ["CONNECTOR"]

_DESTINATION = Field("destination", "string", required=True, pattern=r"^[A-Za-z0-9._-]{1,249}$")
_OPTIONS = Field("options", "mapping", description="additional Kafka client configuration")
_GROUP = Field("group_id", "string", required=True, description="Kafka consumer group")

CONNECTOR = ConnectorDefinition(
    name="kafka", title="Apache Kafka", family="messaging",
    summary="Read and consume topic messages and publish events through Kafka or Amazon MSK",
    connection=(
        Field("bootstrap_servers", "string", required=True, description="comma-separated Kafka broker addresses"),
        Field("security_protocol", "string", default="PLAINTEXT",
              choices=("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"), aliases=("security.protocol",)),
        Field("sasl_mechanism", "string", choices=("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "AWS_MSK_IAM"),
              aliases=("sasl.mechanism",)),
        Field("username", "string", secret=True), Field("password", "string", secret=True),
        Field("region", "string"), Field("ca_location", "string", aliases=("ssl.ca.location",)),
    ),
    operations=(
        messages.READ.with_arguments(
            _DESTINATION,
            Field("from", "string", default="latest", choices=("latest", "earliest")),
        ),
        messages.CONSUME.with_arguments(
            _DESTINATION,
            _GROUP, Field("from", "string", default="earliest", choices=("earliest", "latest", "error")), _OPTIONS,
        ),
        messages.PUBLISH.with_arguments(
            _DESTINATION,
            Field("key", "string", description="event field used as partition key"), _OPTIONS,
        ),
    ),
    adapters={
        "source": "streamwright.connectors.kafka.engines.source:KafkaSourceAdapter",
        "spark": "streamwright.connectors.kafka.engines.spark:KafkaSparkAdapter",
    },
    validator="streamwright.connectors.kafka.client:validate_connection",
    # vendor librdkafka conf keys that may carry a secret reference (redacted); e.g. ssl.key.password, sasl secrets
    secret_key_patterns=("*.password", "sasl.password", "ssl.key.password", "ssl.keystore.password",
                         "sasl.oauthbearer.client.secret"),
)
