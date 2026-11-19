# REST API (`restapi`)

Declarative HTTP extraction for any REST API.

Connects to REST endpoints, handles Bearer/Basic/API Key/OAuth2 authentication,
manages rate limits and retries, and paginates responses automatically.

## Connection

### `source.yaml` contract

```yaml
kind: source
name: my_rest_source
spec:
  config:
    base_url: {type: string, default: "https://api.example.com/v1"}
  secrets:
    api_token: {type: string, required: true}

auth:
  type: bearer                  # "bearer" | "api_key" | "basic" | "oauth2_refresh_token"
  token: "{{ secrets.api_token }}"
  # For api_key: {type: api_key, name: "X-API-Key", in: "header", value: "{{ secrets.api_token }}"}
  # For basic: {type: basic, username: "...", password: "{{ secrets.pwd }}"}
  # For oauth2: {type: oauth2_refresh_token, token_url: "...", client_id: "...", client_secret: "{{ secrets.sec }}", refresh_token: "{{ secrets.tok }}"}

http:
  base_url: "{{ config.base_url }}"
  headers:
    Accept: "application/json"
policy:
  rate_limit: {requests: 10, per: 1s}
  retry: {codes: [429, 500, 502, 503], max_attempts: 5, backoff: exponential}
```

### Authentication & Secrets

- `auth.type`: `bearer` (needs `token`), `api_key` (needs `name`, `value`, optional `in: "header"|"query"`),
  `basic` (needs `username`, `password`), `oauth2_refresh_token` (needs `token_url`, `client_id`, `client_secret`, `refresh_token`, optional `scopes`).
- Secrets masking: Tokens and passwords are automatically redacted from all network logs.

## Operations

The connector offers one operation, `http.request`: the `operation:` of each `requests:` item of a stream is an HTTP
request, run by the engine's HTTP reader with the source's `http:` defaults (base URL, headers, paginator, records,
retry, rate limit).

### `streams/<stream>.yaml` contract

```yaml
requests:
  - name: list_items
    operation:
      path: "/items"            # Appended to http.base_url
      method: GET               # GET | POST | PUT | PATCH | DELETE
      params:
        status: "active"
      records:
        path: "data.items"        # Object with 'path' (or 'explode')
      paginator:
        type: cursor              # "cursor" | "offset" | "page_number" | "link_header"
        param: "starting_after"
        token_path: "meta.next_cursor"
        # For page_number: {type: page_number, page_param: "page", page_size: 100, size_param: "limit"}
        # For offset:      {type: offset, offset_param: "offset", limit_param: "limit", page_size: 100}
        # For link_header: {type: link_header}

transform:
  mode: page                    # "page" | "run"
  steps:
    - name: items_clean
      select: |
        SELECT 
          CAST(record->>'id' AS VARCHAR) AS item_id,
          record->>'name' AS name,
          CAST(record->>'price' AS DECIMAL(10,2)) AS price,
          strptime(record->>'created_at', '%Y-%m-%dT%H:%M:%SZ') AS created_at
        FROM list_items

export:
  items:
    step: items_clean
    primary_key: [item_id]
```

### Transform & Data Shaping

- Emits rows matching the records extracted from the HTTP response payload.
- In `transform` steps, each request's name (`list_items`) is available as a relational table in DuckDB SQL.
- Use DuckDB functions for JSON unnesting (`unnest()`, `json_extract()`), string operations, and type casting.

### Vendor arguments

Any key in an HTTP `operation:`'s `params` block is a query parameter forwarded verbatim to the API; the paths and
parameters you call are whatever the API exposes, never a closed list in the connector (contract v1.3 §6b). For
example `params: {statusFilter: ENABLED}` reaches the request URL unchanged; pagination cursors the paginator
injects take precedence.

## DuckDBJob

A complete DuckDBJob that calls this connector (`streamwright duckdb validate` checks it offline; only the account values and the secret are yours):

```yaml
kind: DuckDBJob
version: 1
name: rest_items
spec:
  config:
    base_url: {type: string, default: "https://api.example.com/v1"}
  secrets:
    api_token: {type: string, required: true}

connections:
  rest_api:
    provider: restapi
    type: bearer
    token: "{{ secrets.api_token }}"
    http:
      base_url: "{{ config.base_url }}"
      headers: {Accept: application/json}
    policy:
      rate_limit: {requests: 10, per: 1s}
  out:
    provider: files
    roots: [output]

steps:
  - name: items
    connection: rest_api
    operation:
      path: /items
      method: GET
      params: {status: active}
      records: {path: data.items}
      paginator: {type: cursor, param: starting_after, token_path: meta.next_cursor}
    policy:
      retry: {codes: [429, 500, 502, 503], max_attempts: 5, backoff: exponential}
    sql: |
      SELECT record->>'id' AS item_id,
             record->>'name' AS name,
             CAST(record->>'price' AS DECIMAL(10, 2)) AS price
      FROM records

outputs:
  - name: items_file
    connection: out
    step: items
    mode: overwrite
    path: rest_items.parquet
    format: parquet
```

- **Connection**: the authentication keys, the HTTP defaults in `http: {base_url, headers, params_encoding,
  paginator, records}` and the default `policy: {retry, rate_limit}`. `validate` rejects keys the connector would
  ignore.
- **Step**: `operation: {path, method, params, headers, json, records, paginator}`, plus `policy: {retry,
  rate_limit, timeout, on_error}`. `path` is relative to the connection's `http.base_url`, or an absolute `https://`
  URL. This is a REST API connector: a step's operation is an HTTP request, never a call (`service`, `arguments`:
  an SDK connector's).
- **SQL**: the step's `sql` reads the table `records`, one JSON column `record`; extract with `->>`.

```bash
STREAMWRIGHT_SECRET_API_TOKEN=... streamwright duckdb run items.yaml
```

## Engines

### Execution & Behavior

- **Transport**: `http` (Native declarative requests session).
- **Retries**: Automatic exponential or constant backoff on specified HTTP status codes.
- **Paging**: Automatically loops until paginator finds no next page, empty response, or termination condition.

| Engine | Adapter | Notes |
|---|---|---|
| source (`streamwright run`, DuckDBJob call steps) | `streamwright.core.sdk.engines.source` (HttpSourceAdapter) | the engine's HTTP reader runs each request's `operation:` with the source's `http:` defaults: the connector declares the authentication it takes, the source declares the API |
