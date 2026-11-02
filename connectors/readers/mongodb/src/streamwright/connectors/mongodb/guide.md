# MongoDB (`mongodb`)

Read-only `find` and `aggregate` reads of MongoDB collections (`streamwright run` sources and SparkJob steps), and
collection writes (SparkJob outputs). Each document is one record: ObjectId as text, dates as ISO 8601, Decimal128 as a
number, binary as base64.

## Connection

```yaml
# source.yaml (auth:) or a job's connections.<name>: one URI secret...
provider: mongodb
uri: "{{ secrets.mongo_uri }}"            # mongodb:// or mongodb+srv://, its path the database
# ...or the connection keys:
# host: cluster0.example.net
# port: 27017
# database: production
# username: analytics_ro
# password: "{{ secrets.mongo_password }}"
# auth_source: admin
```

- Either `uri` or `host` and `database` (with `username` and `password` together), not both. `uri` and `password` must
  each be exactly one `{{ secrets.NAME }}` reference; their values - and the password inside a URI, as written and
  decoded - are `***` in every log line and error.
- Spark hands the URI to the MongoDB Spark connector as a read or write option, never in Spark's configuration.

## Operations

mongodb declares its own collection operations (MongoDB has no SQL tables); the Engines column says which engine
takes an argument.

- **`collection.find`** reads the documents a `filter` matches, with a `projection`, a `sort` (`[field, direction]`
  pairs) and a `limit`. The source engine pages them by `batch_size`; Spark runs the find as an aggregation pipeline
  and takes the connector's `options`.
- **`collection.aggregate`** reads the documents an aggregation `pipeline` returns.
- **`collection.write`** (SparkJob outputs): `append` inserts a document per row; `merge` updates the documents
  matching on `primary_key` (default `_id`) and inserts the rest, in place on the executors (no staging).

## Engines

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`) | `engines/source.py` | pymongo; the first page is read through the stream's rate limit and retries (server timeouts are retryable) |
| SparkJob | `engines/spark.py` | `org.mongodb.spark:mongo-spark-connector_2.12`; `pip install "streamwright-mongodb[spark]"` |
