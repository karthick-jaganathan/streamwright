# StreamWright Meta Ads connector

The `meta_ads` connector for [streamwright](../../../core/README.md): the Meta Marketing API through the
[facebook_business](https://pypi.org/project/facebook-business/) SDK. Examples:
the source folder [examples/sources/ads/meta_ads/](../../../examples/sources/ads/meta_ads/), and the job that sends
Conversions API events, [examples/meta_ads/](../../../examples/meta_ads/).

## Install

```bash
make install-meta-ads    # from the repository root; or: pip install ./connectors/ads/meta_ads (installs facebook_business)
streamwright connectors             # lists meta_ads (with its SDK logger)
```

## Auth

| Key | Meaning |
|---|---|
| `access_token` | required: a system-user (or long-lived) access token |
| `app_secret` | optional: every call then carries an `appsecret_proof` |
| `app_id` | optional |
| `api_version` | e.g. `v26.0` (default: the SDK's version) |

## Requests

```yaml
requests:
  - name: raw_campaign_insights
    operation:
      service: AdAccount            # AdAccount, Campaign, AdSet, Ad, AdCreative, Business, User, CustomAudience, AdsPixel
      method: get_insights          # api_get (the object itself) or a declared get_<edge> (see the guide)
      arguments:
        id: "act_{{ partition.account_id }}"
        fields: [campaign_id, date_start, spend]
        params: {level: campaign, time_increment: 1, time_range: {since: "{{ window.start }}", until: "{{ window.end }}"}}
```

- Requests only read (writes are job apply outputs, below); async insights jobs are not supported yet, so use daily
  windows (`incremental`).
- Edges are paged by the SDK, and every page is fetched with the stream's rate limit and retries, so a failed page is
  retried alone.
- Records are the objects' fields as plain dicts, one row each in the request's table. Insights numbers are text, as
  the API returns them, so cast them in a `transform` step: `(record->>'spend')::DOUBLE AS spend`. Dates in `params`
  are sent as `YYYY-MM-DD`.
- The example folder reads campaigns (`get_campaigns`) and ad sets with their targeting (`get_ad_sets`: countries,
  ages and custom audiences as columns, and all of `targeting` as an object), and daily campaign insights. Budgets
  are in the currency's smallest unit (cents for USD), and times are written in UTC.

## Writes (apply outputs)

A DuckDBJob or SparkJob apply output writes rows to Meta (the engine guide's Apply outputs section has the policies):

| Operation | A row is | Per call | Idempotent |
|---|---|---|---|
| `*.create_*` (`AdAccount.create_campaign`, `AdAccount.create_ad_set`, ...) | a new object's parameters; arguments `id` (the parent, e.g. `act_123`) and `params` (every row's) | 1 row (up to 100 a chunk) | no: a cut-short chunk is held (`on_unknown`) |
| `*.api_update` | an `id` column and the parameters to change; argument `params` | 1 row (up to 100 a chunk) | yes |
| `*.api_delete` | an `id` column; needs `allow_remove: true` | 1 row (up to 100 a chunk) | yes |
| `AdsPixel.create_event` | a Conversions API server event; arguments `id` (the pixel) and `params` (e.g. `test_event_code`) | the whole chunk (up to 1000) | yes: Meta deduplicates on event_name + event_id |

- Columns are Graph API parameters: structs and lists go as JSON, a text value holding a JSON object (`user_data`
  from a JSON column) is sent as the object, NULLs are not sent (use `nulls: skip_field`), and an empty `params`
  value is left out. Hash `user_data` in SQL: `sha256(lower(trim(email)))`.
- Results: a created object's `id`; an updated or deleted object's `id`; each event's call's `events_received` and
  `fbtrace_id`. A row Meta rejects fails alone with its Graph API code (e.g. `100`) and message; a throttled or
  temporary error cuts the chunk short: the rows sent so far keep their results, the rest stay in flight.

## Errors

Throttling errors (codes 4, 17, 32, 613 and 80000-80014), temporary errors and HTTP 5xx are retried, waiting as long
as the `x-business-use-case-usage` header asks (raise `retry.max_delay` to wait longer than 60 seconds). Other errors
fail the window with the API's message, code and fbtrace_id.

## Logs

The SDK sends its requests with the requests library, and urllib3, under it, logs each request's method, URL and status
at `DEBUG` on `urllib3.connectionpool`, which `streamwright connectors` lists. With `--log streamwright.network=DEBUG`, streamwright also logs
each request's and response's headers and body. streamwright never turns the SDK's logger on; name it with `--log`:

```bash
streamwright run examples/sources/ads/meta_ads --set account_ids=123 \
  --log streamwright.network=DEBUG --log urllib3.connectionpool=DEBUG
```

The lines are redacted like streamwright's own: the access token, the app secret and `appsecret_proof` are `***`.
`--log streamwright.network=INFO` alone gives streamwright's line per page of records, after its stream, request, partition and
window, e.g. `meta_ads AdAccount.get_insights page 1: 500 record(s), 1.32 s, attempt 1`.
