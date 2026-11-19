# Microsoft Advertising examples

```
jobs/
  campaigns/create.yaml             Search campaigns created paused from a list; existing names skipped
  conversion_goals/create.yaml      offline conversion goals created; existing names skipped
  conversions/upload.yaml           offline conversions from --input, by goal name; unknown goals skipped, rejected
                                    ones logged
payloads/                           the --input rows of each job (placeholders)
config/secrets.yaml.example         the credentials the jobs read
```

Setup, once (`pip install streamwright streamwright-microsoft-ads`):

```bash
cp examples/microsoft_ads/config/secrets.yaml.example ~/.streamwright/microsoft-ads-secrets.yaml   # then fill it in
mkdir -p examples/microsoft_ads/.streamwright                 # the jobs' state (git-ignored)
```

Every job takes the customer and the account with `--set`; nothing in them names a real account. Microsoft has no
dry run for uploads: a conversion it accepts is recorded.

```bash
M=examples/microsoft_ads
ACCOUNT="--set customer_id=CUSTOMER_ID --set account_id=ACCOUNT_ID"
SECRETS="--secrets ~/.streamwright/microsoft-ads-secrets.yaml --apply-state-migrations"

streamwright duckdb run $M/jobs/campaigns/create.yaml --input $M/payloads/campaigns/create.json $ACCOUNT $SECRETS
streamwright duckdb run $M/jobs/conversion_goals/create.yaml --input $M/payloads/conversion_goals/create.json \
  $ACCOUNT $SECRETS
streamwright duckdb run $M/jobs/conversions/upload.yaml --input $M/payloads/conversions/clicks.json $ACCOUNT $SECRETS
```

The customer id is the account's parent (Accounts summary > the customer); `GetUser` returns it. Swapping the two
fails with `119 InvalidAccountId`.

What a run logs:

- **campaigns/create.yaml**, **conversion_goals/create.yaml**: a warning per name the account already has (skipped),
  then a line per row from the results: `campaign NAME: created, id 123...` or `rejected: CODE: reason`. Campaigns
  are created paused, with a daily budget and `Languages: [All]`; goals are `OfflineConversionGoal`s (the `@type`
  column names the concrete type).
- Microsoft accepts conversions for an offline goal only 2 hours after the goal is created: earlier uploads are
  rejected per row with `OfflineConversionNotAcceptedForGoal`.

- **conversions/upload.yaml**: a line per call (`output upload: call 1/3: 2 row(s), ... 0.8 s`); then a warning per
  conversion Microsoft rejected, from the output's results (`row 1: click ... rejected: CODE: reason (field: ...)`).
  The payload's click ids are placeholders, so Microsoft rejects them all: use real ones (the `msclkid` of clicks on
  the account, within the goal's conversion window) to record conversions.
