# Google Ads examples

```
jobs/
  campaigns/sync.yaml               campaigns into a DuckDB warehouse (a call step, SQL, a merge)
  campaigns/create.yaml             campaigns created from a list; existing names skipped
  campaigns/update.yaml             campaigns updated (name, status, budget) by id or resource name
  ad_groups/sync.yaml               campaigns, then each campaign's ad groups (partitions)
  conversion_actions/create.yaml    offline click-conversion actions created; existing names skipped
  conversions/upload.yaml           click conversions from --input, by action name
  conversions/upload_folder.yaml    a folder of conversion files, file by file, 5 at once, each file once
payloads/                           the --input rows of each job (placeholders)
config/secrets.yaml.example         the credentials the jobs read
```

Setup, once (`pip install streamwright streamwright-google-ads`):

```bash
cp examples/google_ads/config/secrets.yaml.example ~/.streamwright/google-ads-secrets.yaml   # then fill it in
mkdir -p examples/google_ads/.streamwright                    # the jobs' state (git-ignored)
```

Every job takes the account and its manager account with `--set`; nothing in them names a real account. Run the
jobs that write with `--set validate_only=true` first: Google checks every row and changes nothing.

```bash
G=examples/google_ads
ACCOUNT="--set customer_id=CUSTOMER_ID --set login_customer_id=MANAGER_ID"
SECRETS="--secrets ~/.streamwright/google-ads-secrets.yaml --apply-state-migrations"

streamwright duckdb run $G/jobs/conversion_actions/create.yaml --input $G/payloads/conversion_actions/create.json \
  $ACCOUNT --set validate_only=true $SECRETS --summary
streamwright duckdb run $G/jobs/conversions/upload.yaml --input $G/payloads/conversions/clicks.json \
  $ACCOUNT --set validate_only=true $SECRETS
streamwright duckdb run $G/jobs/conversions/upload_folder.yaml --set folder=$PWD/$G/payloads/conversions/incoming \
  $ACCOUNT --set validate_only=true $SECRETS --summary
streamwright duckdb run $G/jobs/campaigns/create.yaml --input $G/payloads/campaigns/create.json \
  $ACCOUNT --set validate_only=true $SECRETS --summary
streamwright duckdb run $G/jobs/campaigns/update.yaml --input $G/payloads/campaigns/update.json \
  $ACCOUNT --set validate_only=true $SECRETS --summary
```

What a run logs, per job:

- **conversions/upload.yaml**: one search for just the actions the payload names (a step makes them one list, the
  search's partition; GAQL `conversion_action.name IN (...)`); a warning per conversion naming an action the account
  lacks (`skipped gclid ...`); a line per call (`output upload: call 1/3: 10 row(s), ... 1.1 s`); then a warning per
  conversion Google rejected, from the output's results (`row 3: gclid ... rejected: CODE: reason (field: ...)`).
  The payload's gclids are placeholders, so Google rejects them all: use real ones (from clicks on the account,
  within the action's lookback window) to record conversions.
- **conversions/upload_folder.yaml**: the same per file, every line tagged with its file
  (`[google_ads_conversions_upload_folder file=conversions-02.json ...]`); a second run skips the files already
  uploaded (`skip: job_key=file-..., reason=a batch with this job key already ran`) and uploads new or changed ones.
- **conversion_actions/create.yaml**, **campaigns/create.yaml**: a line per item that exists already, then each new
  item's resource name or Google's reason, from the output's results.

`examples/serve/serve.yaml` serves these jobs over HTTP (`streamwright serve`).
