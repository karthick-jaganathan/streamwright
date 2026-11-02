#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

"""
Specification and single source of truth for the StreamWright source and stream declarative format (`kind: source`).

Defines structural schemas, valid keys, data types, authentication schemes, transformation contracts,
and export definitions for `streamwright validate`, streamwright-mcp, and AI runtime assistants.

### 1. `source.yaml` Schema Structure
```yaml
kind: source                     # Required: must be 'source'
name: my_pipeline                # Required: snake_case identifier
description: "Pipeline purpose"  # Optional: text description

spec:                            # Declares runtime parameters and credentials
  config:                        # Visible parameters passed via CLI (--set) or environment
    tenant_id: {type: string, required: true}
    batch_limit: {type: integer, default: 500}
    start_date: {type: date, required: false}
  secrets:                       # Redacted credentials from STREAMWRIGHT_SECRET_<NAME> or --secrets
    api_key: {type: string, required: true}

auth:                            # Built-in HTTP authentication (or use provider for an installed connector)
  type: bearer
  token: "{{ secrets.api_key }}"
  # Standard HTTP Auth Types:
  # - bearer: {token: "{{ secrets.token }}"}
  # - api_key: {name: "X-Api-Key", value: "{{ secrets.key }}", in: "header" | "query"}
  # - basic: {username: "user", password: "{{ secrets.password }}"}
  # - oauth2_refresh_token: {token_url, client_id, client_secret, refresh_token}

http:                            # Optional HTTP defaults shared by all streams
  base_url: "https://api.example.com"
  headers: {Accept: "application/json"}
  params_encoding: plain         # "plain" | "dotted"
  paginator:                     # "offset" | "cursor" | "page_number" | "link_header" | "none"
    type: cursor
    token_path: next_cursor
    param: cursor
  records: {path: data}          # Dotted path to extract a record array from its envelope
  rate_limit: {requests: 10, per: 1s}
  retry: {codes: [429, 500, 502, 503, 504], max_attempts: 5, backoff: exponential}

# Streams can be declared inline under `streams:` or as individual YAML files in `streams/<name>.yaml`.
```

### 2. `streams/<name>.yaml` Schema Structure
```yaml
description: "Stream documentation"

# Optional partitioning (parallel or sequential chunk execution)
# partitions:
#   - {name: account_id, values: "{{ config.account_ids }}"}
#   - {name: child_id, from_stream: parent_stream, field: id}

# Optional incremental state sync
# incremental:
#   cursor_field: updated_at
#   start: "2026-01-01"
#   window: 1d
#   lookback: 1d

requests:                        # 1 or more extraction requests
  # HTTP extraction:
  - name: raw_items
    http:
      path: /v1/items
      method: GET
      params: {status: active}

  # Or Connector SDK extraction:
  # - name: raw_records
  #   sdk: postgres
  #   service: database
  #   method: query
  #   arguments:
  #     query: "SELECT * FROM orders WHERE created_at >= $since"
  #     params: {since: "{{ window.start }}"}

transform:                       # Relational DuckDB SQL transformations
  mode: page                     # "page" (streamed per page) or "run" (accumulated whole run)
  steps:
    - name: cleaned_items
      select: |
        SELECT
          record->>'id' AS item_id,
          record->>'name' AS item_name,
          (record->>'price')::DOUBLE AS price
        FROM raw_items

export:                          # Final output datasets and identity contracts
  items:
    step: cleaned_items
    primary_key: [item_id]
```

### 3. Programmatic Schemas
Use `json_schema()` and `stream_json_schema()` to get JSON Schema Draft 7 specifications
for validating source and stream configurations programmatically.

The vocabulary (keys, types, enums) is in validation.vocabulary, the JSON Schemas in validation.json_schemas.
"""

from streamwright.core.validation.json_schemas import json_schema, stream_json_schema
from streamwright.core.validation.vocabulary import KIND, SUPPORTED_VERSIONS, VERSION, split_outside_quotes

__all__ = ["KIND", "SUPPORTED_VERSIONS", "VERSION", "json_schema", "split_outside_quotes", "stream_json_schema"]
