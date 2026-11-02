# Apache Kafka (`kafka`)

## Connection

```yaml
provider: kafka
bootstrap_servers: broker:9092
security_protocol: SASL_SSL
sasl_mechanism: PLAIN
username: "{{ secrets.kafka_user }}"
password: "{{ secrets.kafka_password }}"
```

`security_protocol` defaults to `PLAINTEXT`. `AWS_MSK_IAM` requires `region` and a SASL protocol; install
`streamwright-kafka[msk-iam]` to sign tokens. `username` and `password` must be secret references when supplied.
Do not pass credentials in `options` or Spark configuration.

## Operations

`message.read` peeks at a topic without a group join or offset commit. `message.consume` uses a `group_id` and
acknowledges offsets only after writes and outbox commit. `message.publish` uses a stable outbox deduplication
key in the `dedupe-key` header. All use `destination` (a topic name); reads and consumes accept `max_messages`
and `max_wait`. `from` is Kafka-specific (`latest` for peek, `earliest` for consume by default);
`options` on consume/publish supplies additional Kafka client properties, and publish accepts `key` as an event
field. Argument and connection tables are generated from the definition.

## Engines

| Engine | Adapter |
|---|---|
| source (`streamwright run`) | `engines/source.py` |
| DuckDBJob calls (via the source adapter) | `engines/source.py` |
| SparkJob | `engines/spark.py` |
