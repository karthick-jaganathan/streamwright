# streamwright serve example

`serve.yaml` serves the Google Ads jobs of `../google_ads/jobs` as a REST API on `127.0.0.1:8080`, with Swagger UI at
`/docs` and Prometheus metrics at `/metrics`. Callers authenticate with an API key whose SHA-256 digest is in the
environment; the manager account a job goes through comes from the key's claims, never from a request.

```bash
pip install "streamwright[serve]" streamwright-google-ads
export API_KEY=$(openssl rand -hex 32)
export SW_KEY_OPS_SHA256=$(echo -n "$API_KEY" | shasum -a 256 | cut -d' ' -f1)
streamwright validate examples/serve/serve.yaml          # without the environment: structure, jobs, manifests
streamwright serve examples/serve/serve.yaml
# another terminal, with the jobs' secrets: STREAMWRIGHT_SECRET_DEVELOPER_TOKEN, _CLIENT_ID, _CLIENT_SECRET, _REFRESH_TOKEN
streamwright worker examples/serve/serve.yaml --processes 4
```

A dry run of a conversion upload (Google checks the rows, records none):

```bash
curl -H "Authorization: Bearer $API_KEY" -H "Content-Type: application/json" -H "Idempotency-Key: upload-0001" \
     -d "{\"set\": {\"customer_id\": \"1234567890\", \"validate_only\": true},
          \"input\": $(cat examples/google_ads/payloads/conversions/clicks.json)}" \
     http://127.0.0.1:8080/v1/jobs/google_ads.conversions.upload/runs
```

The run queue is `queue.db` beside `serve.yaml` (loopback only). For several hosts, set `state: {url: ...}` to a
network database and bind another address; `streamwright systemd install` installs the systemd units. The full
reference is the serve documentation (`docs/jobs/serve`).
