# LinkedIn Ads examples

```
jobs/
  conversions/send_events.yaml      offline conversions from --input, sent to the Conversions API in batches
payloads/                           the --input rows of each job (placeholders)
config/secrets.yaml.example         the credentials the jobs read
```

Setup, once (`pip install streamwright` and `sw connectors -i linkedin_ads`):

```bash
cp examples/linkedin_ads/config/secrets.yaml.example ~/.streamwright/linkedin-secrets.yaml   # then fill it in
mkdir -p examples/linkedin_ads/.streamwright                  # the jobs' state (git-ignored)
```

```bash
L=examples/linkedin_ads
streamwright duckdb run $L/jobs/conversions/send_events.yaml --input $L/payloads/conversions/events.json \
  --set conversion_rule_id=RULE_ID --secrets ~/.streamwright/linkedin-secrets.yaml --apply-state-migrations
```

The job is a REST API connector's apply output: its `operation:` is a request, and `json: {elements: "{{ rows }}"}`
sends a chunk of rows in one request. Emails are hashed (SHA-256) in the job's SQL, so plain emails never leave it.
The conversion rule must exist in the ad account with the Conversions API as its source.
