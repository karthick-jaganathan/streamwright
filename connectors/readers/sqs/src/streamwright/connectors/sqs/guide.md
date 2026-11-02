# Amazon SQS (`sqs`)

## Connection

```yaml
provider: sqs
region: us-east-1
# endpoint: http://localhost:4566
# key_id: "{{ secrets.aws_key }}"
# secret: "{{ secrets.aws_secret }}"
```

Without explicit credentials SQS uses the platform credential chain. `key_id`, `secret` and `session_token`
must be secret references. An explicit key and secret must be supplied together.

## Operations

`message.read` peeks without deleting (zero visibility timeout); `message.consume` deletes messages only
after writes and outbox commit. `message.publish` writes events in batches of ten. All operations use
`destination` (queue URL); reads and consumes accept `max_messages` and `max_wait`. SQS-specific
`visibility_timeout` and `options` extend consume; `group_id` and `options` extend publish. FIFO publishing
uses the outbox deduplication key as `MessageDeduplicationId`. Argument and connection tables are generated.

## Engines

| Engine | Adapter |
|---|---|
| source (`streamwright run`) | `engines/source.py` |
| DuckDBJob calls (via the source adapter) | `engines/source.py` |
| SparkJob | `engines/spark.py` |
