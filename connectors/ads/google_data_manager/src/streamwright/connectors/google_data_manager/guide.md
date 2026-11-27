# Google Data Manager (`google_data_manager`)

The Google Data Manager API: one API to send first-party data to Google Ads, Google Analytics, Display & Video 360,
Campaign Manager 360 and Search Ads 360 - offline and online conversions (events) and Customer Match audience members -
and to read user lists, partner links, licenses, reports and the processing status of what was sent.

Connects with the official `google-ads-datamanager` library (its gRPC service clients). Writes are apply outputs of a
DuckDBJob (a chunk of rows is one request); reads are call steps and source streams.

## Connection

```yaml
connections:
  dm:
    provider: google_data_manager
    # an OAuth refresh token granted the https://www.googleapis.com/auth/datamanager scope ...
    client_id: "{{ secrets.client_id }}"
    client_secret: "{{ secrets.client_secret }}"
    refresh_token: "{{ secrets.refresh_token }}"
    # ... or a service account key instead (JSON text or the path of a JSON file)
    # service_account_json: "{{ secrets.service_account_json }}"
```

### Authentication & Security

- `provider`: `google_data_manager`.
- Credentials: one kind, as `{{ secrets.* }}` references - `refresh_token` with `client_id` and `client_secret`, or
  `service_account_json`. The credentials, the key's private key and the access tokens they get (refreshed ones too)
  are redacted from every log.
- The refresh token must be granted the `datamanager` scope: a token made for the Google Ads API alone fails its
  refresh (`REFRESH_FAILED`).
- Writes run only as apply outputs: a source stream or call step refuses `ingest_*` and `remove_*` methods.

## Apply outputs (writes)

A chunk of rows is one request. The operation's arguments are the request's fields, and each row is one element of
its list (an `Event`, an `AudienceMember`): its columns are the element's field paths - `transaction_id`,
`ad_identifiers.gclid` - or a mapping or list (a SQL STRUCT, LIST, or JSON text) for a nested message such as
`user_data`. Field names are the API's, in snake_case or camelCase; enum values are names (`WEB`, `CONSENT_GRANTED`).
Timestamps may be SQL timestamps or RFC 3339 text (a timestamp without an offset is UTC).

| Method | Rows | Per request | Idempotent |
|---|---|---|---|
| `IngestionService.ingest_events` | `Event` (conversions, store sales, web and app events) | 2000 | yes: Google deduplicates on `transaction_id`, which every event needs (`INVALID_ROW` without) |
| `IngestionService.ingest_audience_members` | `AudienceMember` (Customer Match) | 10000 | yes |
| `IngestionService.remove_audience_members` | `AudienceMember`; `op: remove`, needs `allow_remove: true` | 10000 | yes |
| `IngestionService.ingest_*` / `remove_*` (users, ad events, ...) | the element of the request's list | 1000 | no: held for you (`on_unknown`) |

- Arguments: `destinations` (up to 10 `Destination` objects: `operating_account`, `login_account`,
  `linked_account` - each `{account_type, account_id}` - and `product_destination_id`, the conversion action or user
  list), `consent` (the request's default; a row's own `consent` takes precedence; send only the consent actually
  collected), `encoding` (`HEX`, the default, or `BASE64`: how hashed identifiers are encoded), `validate_only`
  (default false: Google checks the request and ingests nothing - a dry run), and for `ingest_audience_members`
  `terms_of_service`. For the `ingest_*`/`remove_*` families every request field is a vendor argument.
- Destinations, as Google's request model has them: a request lists its destinations (at most 10, each with a
  unique `reference`), and an event names the ones it goes to in `destination_references` (none: all of them).
  - Known up front: the output's `destinations` argument, with a `reference` each, and the events' own
    `destination_references` column (a SQL LIST: `['purchase']`) picks them.
  - Looked up (e.g. a conversion action's id found by its name with the `google_ads` connector,
    `GoogleAdsService.search_stream` over `conversion_action`): a row's `destination` column - a `Destination`, or a
    list of them (an event for Google Ads and Google Analytics), as a STRUCT or JSON text - says where it goes. The
    request's destinations are then the chunk's rows' (deduplicated, each given its own `reference` or
    `destination_<n>`), and every event's `destination_references` names its own; a row without one goes to the
    output's `destinations`. A chunk needs a second request only past 10 destinations; then a request that fails
    fails its own rows only.
  - A row with no destination at all, a `destination` that is not one, both `destination` and
    `destination_references`, a `destination_references` entry that names none of the output's destinations, or more
    than 10 destinations of its own fails alone as `INVALID_ROW`.
- Identifiers: Google wants emails, phone numbers and names normalized and SHA-256 hashed (its formatting guide). Do
  it in SQL, so only hashes reach the state store and the logs: emails trimmed and lowercase, with the dots and the
  `+suffix` of a gmail.com or googlemail.com user removed; phones as `+` and their digits; then `sha256(...)`
  (lowercase hex, `encoding: HEX`). The examples' SQL gives the same values as the google-ads-datamanager-util
  Formatter. Encryption (confidential matching) is not supported.
- Google reports in three places, and each reaches the rows:
  - Rejected records (fast-fail): a request with an invalid record fails whole and nothing in it is processed; its
    `BadRequest` names each record (`events.events[3].user_data.user_identifiers`, reason `INVALID_HEX_ENCODING`).
    Those rows fail alone - error code Google's reason, message its description and the field in the record - and
    the request is sent again without them (Google: fix and resend). A problem with the request itself (a
    destination, the consent: a path that names no record) fails the chunk, with Google's reasons and its support
    request id.
  - Field warnings: an accepted request may drop optional fields (`fieldWarnings`). The row keeps them in its result:
    `warnings: [{field, reason, description}]` (`field` within the record, e.g. `cart_data.items[0].merchant_product_id`).
  - Processing errors, later: each applied row keeps `{request_id, record_index}`;
    `IngestionService.retrieve_request_status` reports the request per destination, the failed records by
    `record_index` (examples' requests/status.yaml).
  A dry run (`validate_only: true`) gets the same rejections and warnings, its rows' results `validate_only: true`.
  A row that does not fit the element (an unknown column, a bad enum name) fails alone as `INVALID_ROW` before
  anything is sent.
- Logging them: the output's `log:` reads the results (`outcome.result->>'$.request_id'`,
  `outcome.result->'$.warnings'` - parenthesize `->` in a comparison - and `outcome.error_code`); the examples log a
  line per request, rejected row and row with warnings.

```yaml
outputs:
  - name: ingest
    connection: dm
    step: events            # transaction_id, event_timestamp, event_source, ad_identifiers, user_data, ...
    operation:
      service: IngestionService
      method: ingest_events
      arguments:
        destinations:
          - operating_account: {account_type: GOOGLE_ADS, account_id: "{{ config.operating_account }}"}
            login_account: {account_type: GOOGLE_ADS, account_id: "{{ config.login_account }}"}
            product_destination_id: "{{ config.conversion_action_id }}"
        consent: {ad_user_data: "{{ config.ad_user_data_consent }}",          # the consent collected: never assumed
                  ad_personalization: "{{ config.ad_personalization_consent }}"}
        validate_only: "{{ config.validate_only }}"
    apply:
      nulls: skip_field
      on_row_error: continue
    policy:
      retry: {max_attempts: 5, backoff: exponential}
```

```sql
-- a step's SQL: a complete Event a row (user_data as JSON text; ad_identifiers and the conversion's own consent as
-- STRUCTs - a conversion without `consent` gets the request's)
SELECT transaction_id,
       converted_at AS event_timestamp,
       'WEB' AS event_source,
       CASE WHEN gclid IS NOT NULL THEN {'gclid': gclid} END AS ad_identifiers,
       value AS conversion_value,
       currency,
       json_object('user_identifiers', [json_object('email_address', sha256(lower(trim(email))))]) AS user_data,
       CASE WHEN ad_user_data_consent IS NOT NULL
            THEN {'ad_user_data': ad_user_data_consent, 'ad_personalization': ad_personalization_consent}
       END AS consent
FROM input
```

## Reads

| Service | Method | Arguments | Records |
|---|---|---|---|
| `IngestionService` | `retrieve_request_status` | `request_id` | one per destination, with its `request_id` |
| `*Service` | `list_*`, `search_*` (paged) | the request's fields, e.g. `parent: accountTypes/GOOGLE_ADS/accounts/1234567890` | one per resource |
| `*Service` | `get_*`, `retrieve_*` | the request's fields | the response's list items, else the response |

- The installed library decides which services and methods exist (`UserListService.list_user_lists`,
  `PartnerLinkService.search_partner_links`, `ReportingService.retrieve_partner_match_report`, ...); a typo is caught
  before a run with a "did you mean" suggestion.
- Records are plain dicts keyed by the API's snake_case field names, as in its JSON: int64 values are text and enums
  are names (`record->>'$.request_status'`, `record->'$.error_info.error_counts'`).

## Errors

Rate limits (`RESOURCE_EXHAUSTED`, 300 requests a minute per project and service group; waiting as long as Google
asks), `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, `UNKNOWN` and `ABORTED` are retried, as Google's guide says. A daily quota (a
`...PerDayPerProject` quota limit) is `DAILY_QUOTA_EXHAUSTED` and is not retried: the quota resets at midnight
Pacific time, so the run fails and a later one resumes. Other errors fail with their status, Google's reason and the
fields it named, and the request id Google's support asks for (`RequestInfo`). The gRPC clients accept 16 MB of
response headers (Google's advice), so a request with many invalid records reports every one. A step's or output's
`retry: {codes: [...]}` replaces the retried codes.

## Engines

### Execution & Behavior

- **Transport**: `sdk` (`google-ads-datamanager`, gRPC).
- **Services & Methods**: resolved dynamically against the installed library's `gapic_metadata.json` (contract v1.3
  §6b).

| Engine                                            | Adapter             | Notes                                                              |
|---------------------------------------------------|---------------------|--------------------------------------------------------------------|
| source (`streamwright run`, DuckDBJob call steps and apply outputs) | `engines/source.py` | the library's service clients; reads page by page, writes a request a chunk |
