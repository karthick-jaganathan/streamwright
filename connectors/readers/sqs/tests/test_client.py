"""SQS visibility, acknowledgment and FIFO publishing with a fake boto3 client."""

from types import SimpleNamespace

import pytest

from streamwright.connectors.sqs import CONNECTOR, client
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.engines.source import ConnectorError

URL = "https://sqs.us-east-1.amazonaws.com/123456789012/arrivals.fifo"


class Sqs:
    def __init__(self):
        self.receives = []
        self.deletes = []
        self.sends = []

    def receive_message(self, **kwargs):
        self.receives.append(kwargs)
        return {"Messages": [{"MessageId": "id-1", "Body": "{}", "ReceiptHandle": "receipt-1"}]}

    def delete_message_batch(self, **kwargs):
        self.deletes.append(kwargs)
        return {}

    def send_message_batch(self, **kwargs):
        self.sends.append(kwargs)
        return {}


def test_client_follows_credential_chain_and_explicit_credentials(monkeypatch):
    received = []
    monkeypatch.setattr(client.boto3, "client", lambda service, **options: received.append(options) or Sqs())
    assert client.validate_connection({"key_id": "a"}) == ["key_id and secret go together"]
    assert client.validate_connection({"session_token": "x"}) == ["session_token needs key_id and secret"]
    client.create_client({"region": "us-east-1"})
    assert received[-1] == {"region_name": "us-east-1"}
    client.create_client({"region": "us-east-1", "key_id": "k", "secret": "s", "session_token": "t"})
    assert received[-1]["aws_session_token"] == "t"


def test_peek_has_zero_visibility_and_never_acks(monkeypatch):
    sqs = Sqs()
    monkeypatch.setattr(client.boto3, "client", lambda service, **options: sqs)
    calls = []

    def call(function, *args, **kwargs):
        calls.append(function)
        return function(*args, **kwargs)

    records = list(client.peek({"region": "us-east-1"}, {"destination": URL, "max_messages": 1},
                               SimpleNamespace(call=call)))
    assert records[0][0]["message_id"] == "id-1"
    assert sqs.receives[0]["VisibilityTimeout"] == 0
    assert not sqs.deletes
    assert len(calls) == 2


def test_consume_ack_and_fifo_publish_keep_dedupe_key(monkeypatch):
    sqs = Sqs()
    monkeypatch.setattr(client.boto3, "client", lambda service, **options: sqs)
    broker = client.SqsClient({"region": "us-east-1"})
    arguments = {"destination": URL, "max_messages": 1, "max_wait": "1s", "visibility_timeout": 120}
    messages = broker.poll(arguments, 1, 0.1)
    assert messages[0].receipt["handle"] == "receipt-1"
    assert sqs.receives[0]["VisibilityTimeout"] == 120
    assert not sqs.deletes
    broker.ack(messages, arguments)
    assert sqs.deletes[0]["Entries"][0]["ReceiptHandle"] == "receipt-1"
    broker.publish({"destination": URL, "group_id": "orders"}, [{"id": 1}], "batch:output")
    entry = sqs.sends[0]["Entries"][0]
    assert entry["MessageDeduplicationId"] == "batch:output-0"
    assert entry["MessageGroupId"] == "orders"
    assert broker.destination(arguments) == URL
    assert broker.poll_limit(arguments, 10) == 1


def test_long_fifo_dedupe_key_retains_event_identity():
    long_key = "batch-" + "x" * 140
    first = client.deduplication_id(long_key, 0)
    second = client.deduplication_id(long_key, 1)
    assert first != second
    assert len(first) <= 128 and len(second) <= 128
    assert first == client.deduplication_id(long_key, 0)


def test_source_vendor_error_is_redacted_inside_the_context():
    context = SimpleNamespace(call=lambda function: function(), redact=lambda value: value.replace("secret", "***"))

    def fail():
        raise client.ClientError({"Error": {"Code": "AccessDenied", "Message": "secret"}}, "ReceiveMessage")

    with pytest.raises(ConnectorError) as caught:
        client._call(context, fail)
    assert "secret" not in str(caught.value)


def test_vendor_arguments_reach_the_boto3_calls(monkeypatch):
    received = []
    sqs = Sqs()
    monkeypatch.setattr(client.boto3, "client", lambda service, **options: received.append(options) or sqs)
    # an undeclared connection key is forwarded to boto3.client(...)
    connection = resolve_values(CONNECTOR, {"region": "us-east-1", "config": {"read_timeout": 5}})
    broker = client.SqsClient(connection)
    # a top-level vendor receive parameter and the WaitTimeSeconds alias both reach receive_message
    arguments = {"destination": URL, "MaxNumberOfMessages": 7, "WaitTimeSeconds": 3,
                 "MessageSystemAttributeNames": ["All"], "options": {"ReceiveRequestAttemptId": "a1"}}
    assert broker.poll_limit(arguments, 10) == 7
    broker.poll(arguments, broker.poll_limit(arguments, 10), 0.1)
    assert received[-1]["config"] == {"read_timeout": 5}
    assert sqs.receives[0]["MessageSystemAttributeNames"] == ["All"]
    assert sqs.receives[0]["ReceiveRequestAttemptId"] == "a1"
    # a vendor send parameter reaches send_message_batch entries
    broker.publish({"destination": URL, "group_id": "g", "MessageSystemAttributes": {"x": 1}}, [{"id": 1}], "k")
    assert sqs.sends[0]["Entries"][0]["MessageSystemAttributes"] == {"x": 1}
