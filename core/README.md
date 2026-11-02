# StreamWright

[![PyPI](https://img.shields.io/pypi/v/streamwright.svg)](https://pypi.org/project/streamwright/)
[![Python](https://img.shields.io/pypi/pyversions/streamwright.svg)](https://pypi.org/project/streamwright/)
[![License](https://img.shields.io/pypi/l/streamwright.svg)](https://github.com/karthick-jaganathan/streamwright/blob/master/LICENSE)
[![Docs](https://img.shields.io/badge/docs-streamwright.web.app-brightgreen.svg)](https://streamwright.web.app/docs/)

Declarative data pipelines in YAML: read from APIs, files, databases and ads platforms, shape with DuckDB SQL,
write to files, DuckDB, DuckLake, Singer or dlt.

- **No per-connector code**: authentication, paging, retries, rate limits and incremental state are configuration.
- **Connectors on demand** from the StreamWright hub: `sw connectors -i <name>`.
- **Jobs**: `kind: DuckDBJob` SQL pipelines with replay-safe writes; `kind: SparkJob` with
  [streamwright-spark](https://pypi.org/project/streamwright-spark/).

## Install

```bash
pip install streamwright
sw connectors -i restapi
```

## Example

A source is a folder: `source.yaml` and one file per stream.

```yaml
# my_api/source.yaml
kind: source
name: my_api
spec:
  secrets:
    api_token: {type: string}
auth:
  provider: restapi
  type: bearer
  token: "{{ secrets.api_token }}"
http:
  base_url: https://api.example.com
```

```yaml
# my_api/streams/items.yaml
requests:
  - name: raw_items
    http: {path: /v1/items}
transform:
  mode: page
  steps:
    - name: items
      select: SELECT record->>'id' AS id, record->>'name' AS name FROM raw_items
export:
  items: {step: items, primary_key: [id]}
```

```bash
sw validate my_api
STREAMWRIGHT_SECRET_API_TOKEN=... sw run my_api --output jsonl:out
```

## Links

[Documentation](https://streamwright.web.app/docs/) ·
[Quickstart](https://streamwright.web.app/docs/quickstart/) ·
[Source code](https://github.com/karthick-jaganathan/streamwright)
