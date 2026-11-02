"""Amazon SQS client construction, peek reads and broker calls shared across engines."""

import hashlib
import json
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from streamwright.core.sdk import ConnectorError
from streamwright.core.sdk.messaging import BrokerClient, Message, poll_limit, poll_wait_seconds

__all__ = ["SqsClient", "call_kwargs", "create_client", "peek", "validate_connection"]

# arguments the connector handles; any other key in a call is a boto3 request parameter (a vendor argument)
_HANDLED = frozenset({"destination", "group_id", "visibility_timeout", "options", "max_messages", "max_wait",
                      "MaxNumberOfMessages", "WaitTimeSeconds", "VisibilityTimeout"})


def validate_connection(connection):
    problems = []
    if bool(connection.get("key_id")) != bool(connection.get("secret")):
        problems.append("key_id and secret go together")
    if connection.get("session_token") and not connection.get("key_id"):
        problems.append("session_token needs key_id and secret")
    return problems


def call_kwargs(arguments):
    """Extra boto3 request parameters a call carries: its `options` plus any undeclared key (a vendor argument)."""
    extras = dict(arguments.get("options") or {})
    extras.update({key: value for key, value in arguments.items() if key not in _HANDLED})
    return extras


def _visibility(arguments):
    value = arguments.get("visibility_timeout")
    if value is None:
        value = arguments.get("VisibilityTimeout")
    return None if value is None else int(value)


def create_client(connection):
    options = {"region_name": connection["region"]}
    if connection.get("endpoint"):
        options["endpoint_url"] = connection["endpoint"]
    if connection.get("key_id"):
        options.update(aws_access_key_id=connection["key_id"], aws_secret_access_key=connection["secret"])
        if connection.get("session_token"):
            options["aws_session_token"] = connection["session_token"]
    options.update(dict(getattr(connection, "vendor", {}) or {}))  # undeclared connection keys -> boto3 client options
    return boto3.client("sqs", **options)


def deduplication_id(key, number):
    value = f"{key}-{number}"
    return value if len(value) <= 128 else f"{hashlib.sha256(key.encode()).hexdigest()}-{number}"


def _call(context, function, *args, **kwargs):
    def invoke():
        try:
            return function(*args, **kwargs)
        except (BotoCoreError, ClientError) as exc:
            raise ConnectorError(context.redact(f"sqs: {exc}"), code="READ_ERROR") from None

    return context.call(invoke)


class SqsClient(BrokerClient):
    def __init__(self, connection):
        self._connection = connection
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = create_client(self._connection)
        return self._client

    def destination(self, arguments):
        return arguments["destination"]

    def poll_limit(self, arguments, default):
        if arguments.get("MaxNumberOfMessages") is not None:
            return int(arguments["MaxNumberOfMessages"])
        return poll_limit(arguments, default)

    def poll_wait_seconds(self, arguments, default):
        if arguments.get("WaitTimeSeconds") is not None:
            return float(arguments["WaitTimeSeconds"])
        return poll_wait_seconds(arguments, default)

    def poll(self, arguments, limit, max_wait_seconds, watermarks=None):
        messages, deadline = [], time.monotonic() + max_wait_seconds
        destination = arguments["destination"]
        while len(messages) < limit and time.monotonic() < deadline:
            wait = int(max(0, min(20, deadline - time.monotonic())))
            kwargs = {"QueueUrl": destination, "MaxNumberOfMessages": min(10, limit - len(messages)),
                      "WaitTimeSeconds": wait, "MessageAttributeNames": ["All"], "AttributeNames": ["All"]}
            visibility = _visibility(arguments)
            if visibility is not None:
                kwargs["VisibilityTimeout"] = visibility
            kwargs.update(call_kwargs(arguments))  # options + vendor boto3 params override the defaults
            reply = self._get_client().receive_message(**kwargs)
            for item in reply.get("Messages", ()):
                messages.append(Message(
                    message_id=item["MessageId"], key=item.get("Attributes", {}).get("MessageGroupId"),
                    body=item["Body"], timestamp=item.get("Attributes", {}).get("SentTimestamp"),
                    headers={key: value.get("StringValue", "")
                             for key, value in (item.get("MessageAttributes") or {}).items()},
                    receipt={"queue_url": destination, "handle": item["ReceiptHandle"], "id": item["MessageId"]},
                ))
        return messages

    def ack(self, messages, arguments):
        items = list(messages)
        for start in range(0, len(items), 10):
            chunk = items[start:start + 10]
            reply = self._get_client().delete_message_batch(
                QueueUrl=chunk[0].receipt["queue_url"],
                Entries=[{"Id": str(index), "ReceiptHandle": message.receipt["handle"]}
                         for index, message in enumerate(chunk)],
            )
            if reply.get("Failed"):
                raise ConnectorError("could not delete %d SQS message(s)" % len(reply["Failed"]),
                                     code="WRITE_ERROR")

    def publish(self, arguments, events, dedupe_key):
        destination = arguments["destination"]
        items = list(events)
        fifo = destination.endswith(".fifo")
        for start in range(0, len(items), 10):
            entries = []
            for offset, event in enumerate(items[start:start + 10]):
                entry = {"Id": str(offset), "MessageBody": json.dumps(event, sort_keys=True, default=str)}
                if fifo:
                    entry["MessageGroupId"] = str(arguments.get("group_id") or "streamwright")
                    entry["MessageDeduplicationId"] = deduplication_id(dedupe_key, start + offset)
                entry.update(call_kwargs(arguments))
                entries.append(entry)
            reply = self._get_client().send_message_batch(QueueUrl=destination, Entries=entries)
            if reply.get("Failed"):
                raise ConnectorError("could not send %d SQS message(s)" % len(reply["Failed"]), code="WRITE_ERROR")


def peek(connection, arguments, context):
    """Receive with zero visibility so a source never acknowledges or removes a message."""
    sqs = _call(context, create_client, connection)
    limit = int(arguments.get("max_messages", 10))
    deadline = time.monotonic() + poll_wait_seconds(arguments, 5)
    seen, page = set(), []
    while len(seen) < limit and time.monotonic() < deadline:
        wait = int(max(0, min(20, deadline - time.monotonic())))
        kwargs = {"QueueUrl": arguments["destination"], "MaxNumberOfMessages": min(10, limit - len(seen)),
                  "WaitTimeSeconds": wait, "VisibilityTimeout": 0,
                  "AttributeNames": ["All"], "MessageAttributeNames": ["All"]}
        kwargs.update(call_kwargs(arguments))  # vendor boto3 params (VisibilityTimeout stays 0: a peek never acks)
        kwargs["VisibilityTimeout"] = 0
        reply = _call(context, sqs.receive_message, **kwargs)
        for item in reply.get("Messages", ()):
            if item["MessageId"] in seen:
                continue
            seen.add(item["MessageId"])
            page.append({"message_id": item["MessageId"], "body": item["Body"],
                         "attributes": item.get("Attributes") or {},
                         "message_attributes": {key: value.get("StringValue")
                                                for key, value in (item.get("MessageAttributes") or {}).items()}})
    if page:
        yield page
