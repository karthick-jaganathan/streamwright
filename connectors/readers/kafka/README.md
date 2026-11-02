# StreamWright Kafka connector

The Kafka connector uses [connector contract v1.1](../../../docs/design/connector-contract.md). Its sole
`streamwright.connectors` entry point exports `streamwright.connectors.kafka:CONNECTOR`.
The source engine peeks without joining a consumer group; SparkJob consumes with a group, commits offsets only
after output writes and the outbox commit, and publishes events with stable deduplication headers.
DuckDBJob call steps use the source adapter for read-only topic inspection.

```yaml
auth:
  provider: kafka
  bootstrap_servers: broker:9092
requests:
  - name: peek
    sdk: kafka
    service: message
    method: read
    arguments: {destination: arrivals, max_messages: 100, max_wait: 10s, from: latest}
```

In SparkJob use `message.consume` with `destination` and `group_id` (optionally `from`, `max_messages`,
`max_wait`, `options`); use `message.publish` with `destination` (optionally `key`, `options`).
SASL PLAIN/SCRAM requires `username` and `password` as secret references. MSK IAM requires a SASL protocol,
`region` and the `[msk-iam]` extra.

The packaged `guide.md` supplies the full connection, operations and engines guide with generated tables.
Tests use protocol-faithful fake clients; a live Kafka broker has not been exercised in this migration.
