"""Kafka client construction, peek reads and durable broker calls shared across engines."""

import json
import time
from importlib import import_module

import confluent_kafka

from streamwright.core.sdk import ConnectorError
from streamwright.core.sdk.messaging import BrokerClient, Message, poll_limit, poll_wait_seconds

__all__ = ["KafkaClient", "call_conf", "client_conf", "validate_connection"]

# arguments the connector handles itself; any other key in a call is a librdkafka conf (a vendor argument)
_CALL_KEYS = frozenset({"destination", "group_id", "from", "key", "options", "max_messages", "max_wait"})
_CONFIGURATION_ERRORS = frozenset({confluent_kafka.KafkaError._INVALID_ARG, confluent_kafka.KafkaError._INVALID_TYPE})


def validate_connection(connection):
    problems = []
    if not connection["bootstrap_servers"].strip():
        problems.append("bootstrap_servers must not be empty")
    mechanism = connection.get("sasl_mechanism")
    if mechanism in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512") and not (
        connection.get("username") and connection.get("password")
    ):
        problems.append("sasl_mechanism needs username and password")
    if mechanism == "AWS_MSK_IAM" and not connection.get("region"):
        problems.append("AWS_MSK_IAM needs region")
    if mechanism and not connection.get("security_protocol", "PLAINTEXT").startswith("SASL"):
        problems.append("sasl_mechanism needs a SASL security_protocol")
    return problems


def client_conf(connection):
    conf = {"bootstrap.servers": connection["bootstrap_servers"],
            "security.protocol": connection.get("security_protocol", "PLAINTEXT")}
    if connection.get("sasl_mechanism"):
        conf["sasl.mechanism"] = connection["sasl_mechanism"]
    if connection.get("username"):
        conf["sasl.username"] = connection["username"]
        conf["sasl.password"] = connection["password"]
    if connection.get("ca_location"):
        conf["ssl.ca.location"] = connection["ca_location"]
    if connection.get("sasl_mechanism") == "AWS_MSK_IAM":
        signer = import_module("aws_msk_iam_sasl_signer").MSKAuthTokenProvider
        region = connection["region"]

        def token(_config):
            value, expiry_ms = signer.generate_auth_token(region)
            return value, expiry_ms / 1000.0

        conf.update({"sasl.mechanism": "OAUTHBEARER", "oauth_cb": token})
    # undeclared connection keys are librdkafka conf (vendor arguments): e.g. metadata.broker.list, socket.timeout.ms
    conf.update(dict(getattr(connection, "vendor", {}) or {}))
    return conf


def call_conf(arguments):
    """The librdkafka conf a call adds: its `options` plus any undeclared key (a vendor argument)."""
    extras = dict(arguments.get("options") or {})
    extras.update({key: value for key, value in arguments.items() if key not in _CALL_KEYS})
    return extras


def _text(value):
    return value.decode("utf-8", "replace") if isinstance(value, bytes) else value


def _record(message):
    return {"topic": message.topic(), "partition": message.partition(), "offset": message.offset(),
            "key": _text(message.key()), "value": _text(message.value()),
            "headers": {key: _text(value) for key, value in (message.headers() or ())},
            "timestamp": message.timestamp()[1]}


def _describe_error(error, destination):
    if error.name() == "UNKNOWN_TOPIC_OR_PART":
        return "kafka topic %r does not exist: create it before consuming" % destination
    return "kafka error: %s" % error.name()


def _retryable(exc):
    """A rejected configuration (e.g. an invalid vendor argument) or a fatal error never succeeds on retry."""
    error = exc.args[0] if exc.args and isinstance(exc.args[0], confluent_kafka.KafkaError) else None
    if error is None:
        return True
    return not (error.fatal() or error.code() in _CONFIGURATION_ERRORS)


def _call(context, function, *args, **kwargs):
    def invoke():
        try:
            return function(*args, **kwargs)
        except confluent_kafka.KafkaException as exc:
            raise ConnectorError(
                context.redact(f"kafka: {exc}"), code="CONNECT_ERROR", retryable=_retryable(exc)
            ) from None

    return context.call(invoke)


class KafkaClient(BrokerClient):
    """A Kafka broker with commit disabled until the job's outbox is durable."""

    supports_watermarks = True

    def __init__(self, connection):
        self._conf = client_conf(connection)
        self._consumer = None

    def destination(self, arguments):
        return arguments["destination"]

    def poll_limit(self, arguments, default):
        return poll_limit(arguments, default)

    def poll_wait_seconds(self, arguments, default):
        return poll_wait_seconds(arguments, default)

    def _ensure_consumer(self, arguments):
        if self._consumer is None:
            config = dict(self._conf, **call_conf(arguments))
            config.update({"group.id": arguments["group_id"], "enable.auto.commit": False,
                           "enable.auto.offset.store": False,
                           "auto.offset.reset": arguments.get("from", "earliest")})
            self._consumer = confluent_kafka.Consumer(config)
            self._consumer.subscribe([arguments["destination"]])
        return self._consumer

    def watermarks(self, arguments):
        consumer = self._ensure_consumer(arguments)
        destination = arguments["destination"]
        metadata = consumer.list_topics(destination, timeout=10)
        if destination not in metadata.topics:
            return {}
        result = {}
        for partition in metadata.topics[destination].partitions:
            _, high = consumer.get_watermark_offsets(
                confluent_kafka.TopicPartition(destination, partition), timeout=10,
            )
            result[(destination, partition)] = high
        return result

    def poll(self, arguments, limit, max_wait_seconds, watermarks=None):
        consumer = self._ensure_consumer(arguments)
        messages, deadline = [], time.monotonic() + max_wait_seconds
        while len(messages) < limit and time.monotonic() < deadline:
            batch = consumer.consume(num_messages=min(limit - len(messages), 500),
                                     timeout=min(1.0, max(0.0, deadline - time.monotonic())))
            for item in batch:
                if item.error():
                    raise ConnectorError(_describe_error(item.error(), arguments["destination"]), code="READ_ERROR")
                partition_key = (item.topic(), item.partition())
                if watermarks is not None and item.offset() >= watermarks.get(partition_key, float("inf")):
                    high = watermarks[partition_key]
                    target = confluent_kafka.TopicPartition(item.topic(), item.partition(), high)
                    consumer.seek(target)
                    consumer.pause([confluent_kafka.TopicPartition(item.topic(), item.partition())])
                    continue
                messages.append(Message(
                    message_id="%s:%d:%d" % (item.topic(), item.partition(), item.offset()),
                    key=_text(item.key()), body=_text(item.value()) or "",
                    headers={key: _text(value) or "" for key, value in (item.headers() or ())},
                    timestamp=str(item.timestamp()[1]),
                    receipt={"topic": item.topic(), "partition": item.partition(), "offset": item.offset(),
                             "group_id": arguments["group_id"]},
                ))
                if len(messages) >= limit:
                    break
        return messages

    def ack(self, messages, arguments):
        highest = {}
        for message in messages:
            receipt = message.receipt
            key = receipt["topic"], receipt["partition"]
            highest[key] = max(highest.get(key, -1), receipt["offset"])
        if not highest:
            return
        offsets = [confluent_kafka.TopicPartition(topic, partition, offset + 1)
                   for (topic, partition), offset in highest.items()]
        if self._consumer is not None:
            self._consumer.commit(offsets=offsets, asynchronous=False)
            return
        config = dict(self._conf, **call_conf(arguments))
        config.update({"group.id": arguments["group_id"], "enable.auto.commit": False})
        consumer = confluent_kafka.Consumer(config)
        try:
            consumer.commit(offsets=offsets, asynchronous=False)
        finally:
            consumer.close()

    def publish(self, arguments, events, dedupe_key):
        config = dict(self._conf, **call_conf(arguments))
        config.setdefault("enable.idempotence", True)
        config.setdefault("acks", "all")
        producer = confluent_kafka.Producer(config)
        errors = []
        key_field = arguments.get("key")
        for number, event in enumerate(events):
            key = str(event[key_field]) if key_field and event.get(key_field) is not None else None
            producer.produce(arguments["destination"], key=key,
                             value=json.dumps(event, sort_keys=True, default=str),
                             headers=[("dedupe-key", ("%s:%d" % (dedupe_key, number)).encode())],
                             on_delivery=lambda error, _message: errors.append(error) if error else None)
            producer.poll(0)
        remaining = producer.flush(60)
        if errors or remaining:
            raise ConnectorError("kafka publish failed", code="WRITE_ERROR")


def peek(connection, arguments, context):
    """Assign without joining a group; context.call guards every vendor request."""
    config = dict(client_conf(connection), **call_conf(arguments),
                  **{"group.id": "streamwright-peek", "enable.auto.commit": False,
                     "enable.auto.offset.store": False})
    consumer = _call(context, confluent_kafka.Consumer, config)
    destination = arguments["destination"]
    try:
        metadata = _call(context, consumer.list_topics, destination, timeout=10)
        topic = metadata.topics.get(destination)
        if topic is None or topic.error is not None:
            raise ConnectorError("kafka topic %r was not found" % destination, code="NOT_FOUND")
        start = confluent_kafka.OFFSET_BEGINNING if arguments.get("from") == "earliest" else confluent_kafka.OFFSET_END
        _call(context, consumer.assign, [confluent_kafka.TopicPartition(destination, p, start)
                                         for p in sorted(topic.partitions)])
        limit = int(arguments.get("max_messages", 100))
        deadline = time.monotonic() + poll_wait_seconds(arguments, 10)
        page, taken = [], 0
        while taken < limit and time.monotonic() < deadline:
            message = _call(context, consumer.poll, min(1.0, max(0.0, deadline - time.monotonic())))
            if message is None:
                continue
            if message.error():
                raise ConnectorError(_describe_error(message.error(), destination), code="READ_ERROR")
            page.append(_record(message))
            taken += 1
            if len(page) >= 500:
                yield page
                page = []
        if page:
            yield page
    finally:
        _call(context, consumer.close)
