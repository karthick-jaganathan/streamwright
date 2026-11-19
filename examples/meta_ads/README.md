# Meta Ads examples

```
jobs/
  campaigns/create.yaml             campaigns created paused from a list; existing names skipped
  custom_conversions/create.yaml    custom conversions (a pixel's events by URL rule); existing names skipped
  conversions/send_events.yaml      in-store purchases sent to a pixel as Conversions API events (hashed in SQL)
payloads/                           the --input rows of each job (placeholders)
config/secrets.yaml.example         the credentials the jobs read
```

Setup, once (`pip install streamwright streamwright-meta-ads`):

```bash
cp examples/meta_ads/config/secrets.yaml.example ~/.streamwright/meta-ads-secrets.yaml   # then fill it in
mkdir -p examples/meta_ads/.streamwright                      # the jobs' state (git-ignored)
```

Every job takes the ad account or pixel with `--set`; nothing in them names a real account. Send with `--set test_event_code=...`
first (from Events Manager > Test Events): the events show there and are not counted.

```bash
M=examples/meta_ads
SECRETS="--secrets ~/.streamwright/meta-ads-secrets.yaml --apply-state-migrations"

streamwright duckdb run $M/jobs/campaigns/create.yaml --input $M/payloads/campaigns/create.json \
  --set account_id=ACCOUNT_ID $SECRETS
streamwright duckdb run $M/jobs/custom_conversions/create.yaml --input $M/payloads/custom_conversions/create.json \
  --set account_id=ACCOUNT_ID --set pixel_id=PIXEL_ID $SECRETS
streamwright duckdb run $M/jobs/conversions/send_events.yaml --input $M/payloads/conversions/events.json \
  --set pixel_id=PIXEL_ID --set test_event_code=TEST12345 $SECRETS --summary
```

What a run logs:

- **campaigns/create.yaml**, **custom_conversions/create.yaml**: a warning per name the account already has (skipped),
  a line per call, then a line per row from the results: `campaign NAME: created, id ...`, or `rejected: 100:
  Invalid parameter: This campaign's budget is too small.` A row Meta rejects fails alone (one request per row).
  Campaigns are created paused; a row without `daily_budget` sets `is_adset_budget_sharing_enabled: false`, as Meta
  asks when the ad sets carry the budgets.

- **conversions/send_events.yaml**: a line per call (`output send: call 1/1: 5 row(s), ... 0.4 s`); then, if Meta
  rejects a call, a warning per event of it, from the output's results (`row 3: event EXAMPLE-ORDER-0003 rejected:
  100: ...`). Email and phone are hashed in the job's SQL (`sha256(lower(trim(email)))`), so no plain contact detail
  is sent. The payload's purchases are placeholders: use your own orders (at most 62 days old) to record sales.
