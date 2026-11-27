# StreamWright Google Data Manager connector

The `google_data_manager` connector for [streamwright](../../../core/README.md): the
[Google Data Manager API](https://developers.google.com/data-manager/api) through the official
[google-ads-datamanager](https://pypi.org/project/google-ads-datamanager/) library. A DuckDBJob's apply outputs send
conversions (events) and Customer Match audience members to Google Ads, Google Analytics, Display & Video 360 and
Campaign Manager 360; call steps and source streams read the processing status of what was sent, user lists, partner
links and reports. Examples: [examples/google_data_manager/](../../../examples/google_data_manager/README.md).

## Install

```bash
make install-google-data-manager    # from the repository root; or: pip install ./connectors/ads/google_data_manager
streamwright connectors google_data_manager
```

## Auth

| Key | Meaning |
|---|---|
| `client_id`, `client_secret`, `refresh_token` | an OAuth refresh token granted the `https://www.googleapis.com/auth/datamanager` scope |
| `service_account_json` | a service account key (JSON text or a file path), instead of a refresh token |

## Writes (apply outputs)

| Method | A row is | Per request |
|---|---|---|
| `IngestionService.ingest_events` | an `Event`: `transaction_id`, `event_timestamp`, `event_source`, `ad_identifiers`, `user_data`, `conversion_value`, `currency`, ... | 2000 |
| `IngestionService.ingest_audience_members` | an `AudienceMember`: `user_data`, `mobile_data`, `pair_data`, `consent`, ... | 10000 |
| `IngestionService.remove_audience_members` | an `AudienceMember` (needs `allow_remove: true`) | 10000 |
| `IngestionService.ingest_*` / `remove_*` | the element of the request's list (users, ad events, ...) | 1000 |

- Arguments: `destinations`, `consent`, `encoding` (`HEX` default), `validate_only`, and for audiences
  `terms_of_service`; the families take their request fields as vendor arguments.
- Destinations as in Google's guide: a request lists them (up to 10, each with a `reference`) and an event's
  `destination_references` picks its own. Looked up destinations come from the rows: a row's `destination` column
  (built in SQL, e.g. from a conversion action looked up by name with the google_ads connector) becomes the request's
  destinations and the event's references
  ([conversions/ingest_by_action_name.yaml](../../../examples/google_data_manager/jobs/conversions/ingest_by_action_name.yaml)).
- Normalize and SHA-256 hash the identifiers in SQL (Google's formatting guide), so only hashes reach the state store
  and the logs; the examples show the SQL, which gives the same values as Google's google-ads-datamanager-util.
- Google processes a request later: each applied row keeps `request_id` and its `record_index`, and
  `IngestionService.retrieve_request_status` reports failed records by that index
  ([examples/google_data_manager/jobs/requests/status.yaml](../../../examples/google_data_manager/jobs/requests/status.yaml)).
- A request refused for some rows' fields fails those rows alone and is sent again without them.

## Reads

| Service | Method | Arguments | Records |
|---|---|---|---|
| `IngestionService` | `retrieve_request_status` | `request_id` | one per destination |
| `*Service` | `list_*`, `search_*`, `get_*`, `retrieve_*` | the request's fields (vendor arguments) | one per resource |

## Errors

- Rejected records (Google's fast-fail): each row Google names fails alone, its error code Google's reason
  (`INVALID_HEX_ENCODING`, ...), and the request is sent again without it; a request-level problem fails the chunk.
- Field warnings (optional fields dropped): kept in the row's result, `warnings: [{field, reason, description}]`.
- Processing errors: later, by `record_index`, from `IngestionService.retrieve_request_status`.
- Rate limits and transient errors (`RESOURCE_EXHAUSTED`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, `UNKNOWN`,
  `ABORTED`) are retried, waiting as long as Google asks. A daily quota is `DAILY_QUOTA_EXHAUSTED`, not retried (it
  resets at midnight Pacific time). Other errors fail with their status, Google's reasons, the fields it named and its
  support request id.

## Logs

The library logs on `google.ads.datamanager_v1` and the token refresh on `google.auth`, which `streamwright connectors`
lists; streamwright never turns them on. Name them with `--log`, redacted like streamwright's own lines:

```bash
streamwright duckdb run examples/google_data_manager/jobs/requests/status.yaml --set request_id=REQUEST_ID \
  --secrets ~/.streamwright/google-data-manager-secrets.yaml --log google.ads.datamanager_v1=DEBUG
```
