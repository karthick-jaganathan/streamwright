"""Kafka wire protocol and offset handling with a fake consumer and producer."""

from types import SimpleNamespace

import pytest

from streamwright.connectors.kafka import CONNECTOR, client
from streamwright.core.sdk.contract import InvalidConnectionError, resolve_values
from streamwright.core.sdk.engines.source import ConnectorError


class Event:
    def topic(self):
        return "arrivals"

    def partition(self):
        return 2

    def offset(self):
        return 8

    def key(self):
        return b"order"

    def value(self):
        return b'{"ok":true}'

    def headers(self):
        return [("source", b"test")]

    def timestamp(self):
        return 0, 1000

    def error(self):
        return None


class Consumer:
    def __init__(self, conf):
        self.conf = conf
        self.events = [Event()]
        self.commits = []
        self.closed = False

    def subscribe(self, destinations):
        self.destinations = destinations

    def consume(self, num_messages, timeout):
        batch, self.events = self.events[:num_messages], self.events[num_messages:]
        return batch

    def poll(self, timeout):
        return self.events.pop(0) if self.events else None

    def list_topics(self, topic, timeout):
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions={2: object()})})

    def assign(self, assignments):
        self.assignments = assignments

    def commit(self, offsets, asynchronous):
        self.commits.append(offsets)

    def close(self):
        self.closed = True


def test_client_connection_and_secret_redaction():
    assert client.validate_connection({"bootstrap_servers": "broker", "sasl_mechanism": "AWS_MSK_IAM"}) == [
        "AWS_MSK_IAM needs region", "sasl_mechanism needs a SASL security_protocol"]
    with pytest.raises(InvalidConnectionError) as caught:
        resolve_values(CONNECTOR, {"bootstrap_servers": "broker", "sasl_mechanism": "PLAIN",
                                   "security_protocol": "SASL_SSL", "password": "dont-show"})
    assert "dont-show" not in str(caught.value)
    assert client.client_conf({"bootstrap_servers": "broker", "username": "u", "password": "p"})[
        "sasl.password"] == "p"


def test_consume_and_ack_use_offsets_and_connector_poll_policy(monkeypatch):
    instances = []

    def make_consumer(config):
        consumer = Consumer(config)
        instances.append(consumer)
        return consumer

    monkeypatch.setattr(client.confluent_kafka, "Consumer", make_consumer)
    broker = client.KafkaClient({"bootstrap_servers": "broker"})
    arguments = {"destination": "arrivals", "group_id": "workers", "max_messages": 1, "max_wait": "2s"}
    messages = broker.poll(arguments, 1, 0.1)
    assert messages[0].message_id == "arrivals:2:8"
    assert instances[0].conf["enable.auto.commit"] is False
    assert instances[0].commits == []
    broker.ack(messages, arguments)
    assert instances[0].commits[0][0].offset == 9
    assert broker.poll_limit(arguments, 10) == 1
    assert broker.poll_wait_seconds(arguments, 30) == 2
    assert broker.poll_limit(dict(arguments, max_messages=1000), 500) == 1000
    assert broker.poll_wait_seconds(dict(arguments, max_wait="1m"), 30) == 60


def test_peek_is_context_guarded_and_never_commits(monkeypatch):
    consumer = Consumer({})
    monkeypatch.setattr(client.confluent_kafka, "Consumer", lambda conf: consumer)
    calls = []

    def call(function, *args, **kwargs):
        calls.append(function)
        return function(*args, **kwargs)

    records = list(client.peek({"bootstrap_servers": "broker"},
                               {"destination": "arrivals", "max_messages": 1}, SimpleNamespace(call=call)))
    assert records[0][0]["offset"] == 8
    assert consumer.closed and not consumer.commits
    assert len(calls) >= 4


def test_source_vendor_error_is_redacted_inside_the_context(monkeypatch):
    class VendorError(Exception):
        pass

    monkeypatch.setattr(client.confluent_kafka, "KafkaException", VendorError)
    context = SimpleNamespace(call=lambda function: function(), redact=lambda value: value.replace("secret", "***"))

    def fail():
        raise VendorError("secret")

    with pytest.raises(ConnectorError) as caught:
        client._call(context, fail)
    assert "secret" not in str(caught.value)
    assert caught.value.retryable


def test_rejected_vendor_conf_is_not_retried():
    context = SimpleNamespace(call=lambda function: function(), redact=lambda value: value)
    with pytest.raises(ConnectorError, match=r"fetch\.wait\.max\.ms") as caught:
        client._call(context, client.confluent_kafka.Consumer,
                     {"bootstrap.servers": "broker", "group.id": "g", "fetch.wait.max.ms": "not-a-number"})
    assert not caught.value.retryable


def test_vendor_arguments_reach_the_kafka_client(monkeypatch):
    # a resolved connection forwards undeclared librdkafka conf keys (and the alias security.protocol) verbatim
    connection = resolve_values(CONNECTOR, {"bootstrap_servers": "broker", "socket.timeout.ms": "5000",
                                            "security.protocol": "SSL"})
    conf = client.client_conf(connection)
    assert conf["socket.timeout.ms"] == "5000" and conf["security.protocol"] == "SSL"
    # a call forwards both `options` and any top-level vendor key (e.g. fetch.wait.max.ms) to the consumer config
    instances = []
    monkeypatch.setattr(client.confluent_kafka, "Consumer",
                        lambda config: instances.append(Consumer(config)) or instances[-1])
    broker = client.KafkaClient(connection)
    arguments = {"destination": "arrivals", "group_id": "workers", "max_messages": 1,
                 "fetch.wait.max.ms": 250, "options": {"fetch.min.bytes": 1}}
    broker.poll(arguments, 1, 0.1)
    assert instances[0].conf["fetch.wait.max.ms"] == 250
    assert instances[0].conf["fetch.min.bytes"] == 1
    assert instances[0].conf["socket.timeout.ms"] == "5000"
