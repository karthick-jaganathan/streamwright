# StreamWright OpenAI Ads connector

The `openai_ads` connector for [streamwright](../../../core/README.md): the OpenAI Ads HTTP API, read through the
engine's HTTP reader. The connector declares the authentication it takes (a bearer token); each request's
`operation:` is an HTTP request, with the source's `http:` defaults (base URL, paginator, records). Example: the
source folder [examples/sources/ads/openai_ads/](../../../examples/sources/ads/openai_ads/); the full reference is
[guide.md](src/streamwright/connectors/openai_ads/guide.md).

## Install

```bash
make install-openai-ads    # from the repository root; or: pip install ./connectors/ads/openai_ads
streamwright connectors    # lists openai_ads (with its network loggers)
```

## Auth

```yaml
auth:
  provider: openai_ads
  type: bearer
  token: "{{ secrets.openai_ads_key }}"    # a secret reference: `***` in every log line and error

http:
  base_url: https://api.ads.openai.com
  paginator: {type: cursor, token_path: last_id, param: after, has_more_path: has_more}
  records: {path: data}
```

## Requests

```yaml
partitions:
  - {name: account_id, values: "{{ config.account_ids }}"}
requests:
  - name: raw_campaigns
    operation:
      path: /v1/campaigns             # /v1/ad_accounts, /v1/ad_groups, /v1/ads, /v1/campaigns/{id}/insights, ...
      method: GET
      params:
        ad_account_id: "{{ partition.account_id }}"
        limit: 500
```

- `path` is relative to `http.base_url` (or an absolute `https://` URL); `params` are query parameters forwarded
  verbatim, so any path and parameter the API exposes can be read. The paginator follows `last_id` into `after` while
  `has_more` is true; records come from the response's `data`.
- Child objects can be read per account (`ad_groups.yaml`, `ads.yaml`) or per parent with request-level partitions
  (`ad_group_tree.yaml`, `ad_tree.yaml`); insights per object (`campaign_performance.yaml`).

## Errors

The source's `policy.retry` retries HTTP 429 and 5xx responses, honoring `Retry-After`; other HTTP errors fail with
the API's status code and message.

## Logs

Requests are sent via `requests`: urllib3 logs connection details on `urllib3.connectionpool`. With
`--log streamwright.network=DEBUG`, streamwright logs each request's and response's headers and body, with the
bearer token masked.
