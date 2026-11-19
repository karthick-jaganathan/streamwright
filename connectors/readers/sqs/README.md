# StreamWright SQS connector

The SQS connector uses [connector contract v1.1](../../../docs/design/connector-contract.md). Its sole
`streamwright.connectors` entry point exports `streamwright.connectors.sqs:CONNECTOR`.
The source engine (and a DuckDBJob call step) peeks with zero visibility timeout without deleting messages.
DuckDBJob and SparkJob consume messages, delete them only after output writes and the outbox commit, and publish
FIFO events with stable deduplication IDs.

```yaml
auth:
  provider: sqs
  region: us-east-1
requests:
  - name: peek
    operation:
      service: message
      method: read
      arguments:
        destination: https://sqs.us-east-1.amazonaws.com/123456789012/arrivals
        max_messages: 10
        max_wait: 5s
```

SparkJob `message.consume` and `message.publish` share `destination`; consume accepts `max_messages` and
`max_wait`, plus `visibility_timeout` and `options`. Publish accepts `group_id` and `options`. Connection
credentials (`key_id`, `secret`, `session_token`) are secret references; omit them for the platform chain.

The packaged `guide.md` supplies the full connection, operations and engines guide with generated tables.
Tests use protocol-faithful fake clients; live AWS/LocalStack has not been exercised in this migration.
