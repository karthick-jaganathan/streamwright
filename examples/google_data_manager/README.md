# Google Data Manager examples

```
jobs/
  conversions/ingest.yaml     offline conversions from --input to a Google Ads conversion action (ingest_events)
  conversions/ingest_by_action_name.yaml
                              the same to the actions named in the payload: looked up with google_ads, each event
                              routed by its own `destination`
  audiences/ingest.yaml       Customer Match members from --input added to a user list (ingest_audience_members)
  requests/status.yaml        an ingestion's processing status per destination, into a DuckDB table
payloads/                     the --input rows of each job (placeholders)
config/secrets.yaml.example   the credentials the jobs read
```

Setup, once (`pip install streamwright streamwright-google-data-manager`):

```bash
cp examples/google_data_manager/config/secrets.yaml.example ~/.streamwright/google-data-manager-secrets.yaml  # fill in
mkdir -p examples/google_data_manager/.streamwright           # the jobs' state (git-ignored)
```

The refresh token must be granted the `https://www.googleapis.com/auth/datamanager` scope (a Google Ads API token is
not enough). Every job takes the accounts with `--set`; nothing in them names a real account. Run the jobs that write
with `--set validate_only=true` first: Google checks every row and ingests nothing.

```bash
D=examples/google_data_manager
ACCOUNT="--set operating_account=CUSTOMER_ID --set login_account=MANAGER_ID"
# the consent you collected, sent for rows without their own: no default, never assume CONSENT_GRANTED
CONSENT="--set ad_user_data_consent=CONSENT_GRANTED --set ad_personalization_consent=CONSENT_GRANTED"
SECRETS="--secrets ~/.streamwright/google-data-manager-secrets.yaml --apply-state-migrations"

streamwright duckdb run $D/jobs/conversions/ingest.yaml --input $D/payloads/conversions/offline.json \
  $ACCOUNT $CONSENT --set conversion_action_id=ACTION_ID --set validate_only=true $SECRETS
streamwright duckdb run $D/jobs/conversions/ingest_by_action_name.yaml \
  --input $D/payloads/conversions/offline_by_action_name.json $ACCOUNT $CONSENT --set validate_only=true $SECRETS
streamwright duckdb run $D/jobs/audiences/ingest.yaml --input $D/payloads/audiences/members.json \
  $ACCOUNT $CONSENT --set user_list_id=USER_LIST_ID --set validate_only=true $SECRETS
streamwright duckdb run $D/jobs/requests/status.yaml --set request_id=REQUEST_ID $SECRETS
```

What a run logs, per job:

- **conversions/ingest.yaml**: a warning per conversion with no gclid, email or phone (`skipped transaction ...`); a
  line per call (`output ingest: call 1/1: 3 row(s), 3 applied, 0 failed`); then what Google returned: a line per
  request (`request 1265...: applied: 3 event(s)`, the id to check with requests/status.yaml), per rejected event
  (`transaction ...: INVALID_HEX_ENCODING: Email is not hex encoded. (field: user_data.user_identifiers)`) and per
  event accepted with field warnings (`transaction ...: warnings: [{"field":"cart_data.items[0]...", ...}]`).
- **conversions/ingest_by_action_name.yaml**: one Google Ads search for the actions the payload names; a warning
  per conversion naming an unknown action (`skipped transaction ...: no enabled upload-click action named ...`); then
  one request listing the actions as destinations, each event referencing its own, and the same lines of what
  Google returned. It also needs the
  Google Ads `developer_token` in the secrets, and the refresh token both the adwords and the datamanager scopes.
- **audiences/ingest.yaml**: the same lines of what Google returned (`request ...: applied: 3 member(s)`, rejected
  and warned members by row).
- **requests/status.yaml**: a warning per error reason of a destination, with the failed records' `record_index`
  (`... 1 record(s) failed with PROCESSING_ERROR_REASON_INVALID_GCLID: [2]`); the status lands in
  `data_manager_request_status` (`/tmp/google_data_manager_dw.duckdb`), a row per request and destination.

The identifiers are normalized and SHA-256 hashed in the jobs' SQL, as Google's formatting guide asks (the same values
as its google-ads-datamanager-util Formatter), so only hashes reach the state store and the logs.
