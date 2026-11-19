#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

"""The HTTP API of `streamwright serve` and the security suite of plan §8 (the API's part)."""

import json
import logging
import threading
import time

import pytest

testclient = pytest.importorskip("starlette.testclient")
app = pytest.importorskip("streamwright.core.serve.app")
auth = pytest.importorskip("streamwright.core.serve.auth")
to_thread = pytest.importorskip("anyio.to_thread")

DAY = "2026-11-16"
KEY, OTHER_KEY = "sk-test-crm-0123456789", "sk-test-ops-0123456789"


def _headers(key=KEY, **extra):
    return {"Authorization": "Bearer %s" % key, "Content-Type": "application/json", **extra}


@pytest.fixture
def api(feed):
    service = feed.service()
    return testclient.TestClient(app.create_app(service)), service, feed.worker(service)


def _run(client, body, key=KEY, **headers):
    return client.post("/v1/jobs/client_feed/runs", content=json.dumps(body), headers=_headers(key, **headers))


def _queued(client, body, key=KEY, **headers):
    response = _run(client, dict(body, options={"async": True}), key, **headers)
    assert response.status_code == 202, response.text
    return response.json()["run_id"]


def test_a_run_is_queued_run_by_a_worker_and_read_by_its_caller(api, feed):
    client, _service, worker = api
    feed.write_feed("t1", "1001")
    run_id = _queued(client, {"set": {"client_id": "1001", "feed_date": DAY}})
    response = client.get("/v1/runs/%s" % run_id, headers=_headers())
    assert response.json()["status"] == "queued" and "Location" not in response.headers
    assert worker.run_one() == run_id
    shown = client.get("/v1/runs/%s" % run_id, headers=_headers()).json()
    assert shown["status"] == "succeeded" and shown["result"]["outputs"] == [
        {"name": "totals", "rows": 1, "already_committed": False}]
    assert "inputs" not in shown and "principal" not in shown
    assert feed.output("t1", "1001") == ["tenant,n,total", "t1,2,30"]
    # another caller does not see it
    assert client.get("/v1/runs/%s" % run_id, headers=_headers(OTHER_KEY)).status_code == 404


def test_a_sync_run_waits_for_the_worker(feed):
    service = feed.service()
    worker = feed.worker(service)
    feed.write_feed("t1", "1001")
    stop = threading.Event()
    thread = threading.Thread(target=worker.run, args=(stop,), daemon=True)
    worker.poll_seconds = 0.05
    thread.start()
    try:
        client = testclient.TestClient(app.create_app(service))
        response = _run(client, {"set": {"client_id": "1001", "feed_date": DAY}})
    finally:
        stop.set()
        thread.join()
    assert response.status_code == 200 and response.json()["status"] == "succeeded"


def _one_thread():
    to_thread.current_default_thread_limiter().total_tokens = 1


def _while_busy(client, busy, request):
    """How long `request(client)` takes while `busy(client)` is under way in another thread; busy's status."""
    statuses = []
    thread = threading.Thread(target=lambda: statuses.append(busy(client).status_code))
    thread.start()
    time.sleep(0.5)
    started = time.monotonic()
    assert request(client).status_code == 200
    elapsed = time.monotonic() - started
    thread.join()
    return elapsed, statuses


def test_a_run_request_that_waits_holds_no_thread(feed):
    service = feed.service({"client_feed": {"manifest": "job.yaml", "principals": ["crm"],
                                            "set": {"tenant_id": {"from_claim": "tenant"}},
                                            "limits": {"wait_seconds": 3}}})
    with testclient.TestClient(app.create_app(service)) as client:
        client.portal.call(_one_thread)           # the threads endpoints share: one
        elapsed, statuses = _while_busy(client, lambda c: _run(c, {"set": {"client_id": "1", "feed_date": DAY}}),
                                        lambda c: c.get("/readyz"))
    assert statuses == [202] and elapsed < 1.5   # the run waited 3s (no worker) without the thread


def test_a_slow_issuer_holds_up_no_other_request(feed):
    class SlowJwks:
        def get_signing_key_from_jwt(self, token):
            time.sleep(2)
            raise ConnectionError("the issuer does not answer")

    service = feed.service(auth={"jwt": {"issuer": "https://idp.example.com", "audience": "streamwright",
                                         "jwks_url": "https://idp.example.com/jwks"}})
    authenticator = auth.Authenticator(service.config, jwks_client=SlowJwks())
    with testclient.TestClient(app.create_app(service, authenticator)) as client:
        elapsed, statuses = _while_busy(client, lambda c: c.get("/v1/jobs", headers={"Authorization": "Bearer a.b.c"}),
                                        lambda c: c.get("/healthz"))
    assert statuses == [401] and elapsed < 1


@pytest.mark.parametrize("authorization", [None, "Bearer wrong-key", "Basic %s" % KEY, "Bearer ",
                                           "Bearer " + "x" * 9000])
def test_callers_must_authenticate(api, authorization):
    client, _, _ = api
    headers = {} if authorization is None else {"Authorization": authorization}
    response = client.get("/v1/jobs", headers=headers)
    assert response.status_code == 401 and response.headers["WWW-Authenticate"] == "Bearer"
    assert response.json()["code"] == "unauthorized"


def test_jobs_are_listed_and_reached_only_by_their_principals(feed):
    jobs = {"client_feed": {"manifest": "job.yaml", "principals": ["crm"],
                            "set": {"tenant_id": {"from_claim": "tenant"}}}}
    client = testclient.TestClient(app.create_app(feed.service(jobs)))
    assert [job["name"] for job in client.get("/v1/jobs", headers=_headers()).json()["jobs"]] == ["client_feed"]
    assert client.get("/v1/jobs", headers=_headers(OTHER_KEY)).json()["jobs"] == []
    response = _run(client, {"set": {"client_id": "1", "feed_date": DAY}}, OTHER_KEY)
    assert response.status_code == 403 and response.json()["code"] == "forbidden"
    assert client.get("/v1/jobs/nope/plan", headers=_headers()).status_code == 404
    described = client.get("/v1/jobs", headers=_headers()).json()["jobs"][0]
    assert sorted(described["set"]) == ["client_id", "feed_date", "label", "region"]     # not tenant_id


@pytest.mark.parametrize("body,code,text", [
    ({"set": {"tenant_id": "t2", "client_id": "1", "feed_date": DAY}}, "input-not-settable", "bound by the service"),
    ({"set": {"api_token": "x", "client_id": "1", "feed_date": DAY}}, "input-not-settable", "secrets are never set"),
    ({"set": {"clientid": "1", "feed_date": DAY}}, "input-not-settable", "unknown config input 'clientid'"),
    ({"set": {"client_id": "12a", "feed_date": DAY}}, "invalid-input", "does not match ^[0-9]{1,12}$"),
    ({"set": {"client_id": "1", "feed_date": DAY, "region": "ap"}}, "invalid-input", "is not one of: eu, us"),
    ({"set": {"client_id": "1"}}, "invalid-input", "missing required config 'feed_date'"),
    ({"set": {"client_id": "1", "feed_date": DAY}, "allow_lookup_exec": True}, "invalid-request",
     "never a request's"),
    ({"set": {"client_id": "1", "feed_date": DAY}, "outputs": [{"path": "/tmp"}]}, "invalid-request", "unknown key"),
    ({"set": {"client_id": "1", "feed_date": DAY}, "options": {"async": True, "strict": False}}, "invalid-request",
     "only option is `async`"),
    ({"set": {"client_id": "1", "feed_date": DAY}, "input": [{"a": 1}]}, "invalid-request", "takes no rows"),
    ({"set": {"client_id": "../x", "feed_date": DAY}}, "invalid-input", "does not match"),
    ({"set": {"client_id": "*", "feed_date": DAY}}, "invalid-input", "does not match"),
    ({"set": {"client_id": "%2e%2e", "feed_date": DAY}}, "invalid-input", "does not match"),
])
def test_bodies_set_only_what_the_caller_may_set(api, body, code, text):
    client, service, _ = api
    response = _run(client, body)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == code and text in response.json()["message"]
    assert service.store.count_requests(("queued", "running")) == 0


@pytest.mark.parametrize("raw,status,text", [
    ('{"set": {"client_id": "1", "client_id": "2"}}', 422, "repeats the key 'client_id'"),
    ('{"set": ' + '{"a": ' * 40 + '1' + '}' * 41, 422, "deeper than 32 levels"),
    ('{"set": ', 422, "is not JSON"),
    ('[1]', 422, "is a JSON object"),
    ('{"set": {"label": "' + "x" * 70000 + '"}}', 413, "larger than"),
])
def test_bodies_are_bounded_and_strict(api, raw, status, text):
    client, _, _ = api
    response = client.post("/v1/jobs/client_feed/runs", content=raw, headers=_headers())
    assert response.status_code == status and text in response.json()["message"], response.text


def test_a_body_is_json_and_queries_are_refused(api):
    client, _, _ = api
    response = client.post("/v1/jobs/client_feed/runs", content="client_id=1",
                           headers={"Authorization": "Bearer %s" % KEY, "Content-Type": "text/plain"})
    assert response.status_code == 415
    assert client.get("/v1/jobs?debug=1", headers=_headers()).status_code == 400


def test_a_tenant_reaches_only_its_own_feed(api, feed):
    client, _service, worker = api
    feed.write_feed("t1", "1001", rows=((1, 5),))
    feed.write_feed("t2", "1001", rows=((1, 7), (2, 7)))
    for key in (KEY, OTHER_KEY):
        _queued(client, {"set": {"client_id": "1001", "feed_date": DAY}}, key)
    while worker.run_one():
        pass
    assert feed.output("t1", "1001")[1] == "t1,1,5"
    assert feed.output("t2", "1001")[1] == "t2,2,14"


def test_idempotency_keys_bind_a_body_and_return_its_run(api):
    client, _service, _worker = api
    body = {"set": {"client_id": "1001", "feed_date": DAY}}
    first = _queued(client, body, **{"Idempotency-Key": "order-7"})
    assert _queued(client, body, **{"Idempotency-Key": "order-7"}) == first
    other = _run(client, {"set": {"client_id": "1002", "feed_date": DAY}}, **{"Idempotency-Key": "order-7"})
    assert other.status_code == 422 and other.json()["code"] == "idempotency-key-reused"
    assert _queued(client, body, OTHER_KEY, **{"Idempotency-Key": "order-7"}) != first   # a key is per caller
    bad = _run(client, body, **{"Idempotency-Key": "a b"})
    assert bad.status_code == 422 and bad.json()["code"] == "invalid-idempotency-key"


def test_concurrent_duplicates_queue_one_run(api):
    client, service, _ = api
    body = {"set": {"client_id": "1001", "feed_date": DAY}, "options": {"async": True}}
    statuses = []

    def post():
        statuses.append(_run(client, body, **{"Idempotency-Key": "same"}).status_code)

    threads = [threading.Thread(target=post) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert service.store.count_requests(("queued",)) == 1
    assert set(statuses) <= {202, 409}


def test_a_busy_scope_is_refused_with_the_running_id(api):
    client, _service, _ = api
    first = _queued(client, {"set": {"client_id": "1001", "feed_date": DAY}})
    response = _run(client, {"set": {"client_id": "1001", "feed_date": "2026-11-17"}})
    assert response.status_code == 409 and first in response.json()["message"]
    assert response.headers["Location"] == "/v1/runs/%s" % first
    assert _queued(client, {"set": {"client_id": "1002", "feed_date": DAY}}) != first   # another scope


def test_a_busy_scope_of_another_caller_is_not_named(feed):
    job = feed.job()
    job["runtime"]["state"]["scope"] = "client:{{ config.client_id }}"     # shared by the tenants
    feed.write_job(job)
    client = testclient.TestClient(app.create_app(feed.service()))
    first = _queued(client, {"set": {"client_id": "1001", "feed_date": DAY}})
    response = _run(client, {"set": {"client_id": "1001", "feed_date": DAY}}, OTHER_KEY)
    assert response.status_code == 409 and first not in response.text and "Location" not in response.headers


def test_request_ids_are_kept_when_safe_and_replaced_when_hostile(api):
    client, _, _ = api
    assert client.get("/v1/jobs", headers=_headers(**{"X-Request-ID": "abc-123.x_y"})).headers["X-Request-ID"] \
        == "abc-123.x_y"
    for hostile in ("a\tb", "x" * 129, "<script>", "a;b"):
        replaced = client.get("/v1/jobs", headers=_headers(**{"X-Request-ID": hostile})).headers["X-Request-ID"]
        assert replaced != hostile and replaced.startswith("sw_")


def test_failures_tell_the_caller_a_code_and_the_operator_the_cause(api, caplog):
    client, service, worker = api
    run_id = _queued(client, {"set": {"client_id": "4040", "feed_date": DAY, "label": "my-label-value"}})
    with caplog.at_level(logging.INFO):
        worker.run_one()
    shown = client.get("/v1/runs/%s" % run_id, headers=_headers()).json()
    assert shown["status"] == "failed" and shown["error"]["code"] == "run-failed"
    assert "4040" not in json.dumps(shown) and "feeds" not in json.dumps(shown)
    assert "4040" in service.store.get_request(run_id)["error_message"]        # the operator's
    access = [record.getMessage() for record in caplog.records if record.name == "streamwright.serve.access"]
    assert not any("my-label-value" in line for line in access)


def test_secrets_and_rows_leave_no_trace(feed, caplog):
    job = feed.job()
    job["spec"]["input"] = {"columns": {"gclid": {"type": "string", "required": True}}, "max_rows": 2}
    job["steps"] = [{"name": "rows", "sql": "SELECT gclid FROM input"}]
    job["outputs"][0]["sql"] = "SELECT '{{ config.tenant_id }}' AS tenant, count(*) AS n, 0 AS total FROM rows"
    feed.write_job(job)
    service = feed.service()
    client = testclient.TestClient(app.create_app(service))
    rows = [{"gclid": "secret-gclid-1"}, {"gclid": "secret-gclid-2"}]
    too_many = _run(client, {"set": {"client_id": "1", "feed_date": DAY}, "input": rows * 2})
    assert too_many.status_code == 422 and "max_rows" in too_many.json()["message"]
    assert "secret-gclid" not in too_many.text
    run_id = _queued(client, {"set": {"client_id": "1", "feed_date": DAY}, "input": rows})
    assert service.store.get_request(run_id)["payload"] == rows
    with caplog.at_level(logging.DEBUG):
        feed.worker(service).run_one()
    stored = service.store.get_request(run_id)
    assert stored["status"] == "succeeded" and stored["payload"] is None
    assert not any("secret-gclid" in record.getMessage() for record in caplog.records)


def test_health_readiness_and_openapi(api):
    client, _, _ = api
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").status_code == 200
    assert client.get("/openapi.json").status_code == 401
    document = client.get("/openapi.json", headers=_headers()).json()
    schema = document["paths"]["/v1/jobs/client_feed/runs"]["post"]["requestBody"]["content"]["application/json"]
    settable = schema["schema"]["properties"]["set"]
    assert settable["properties"]["client_id"] == {"type": "string", "pattern": "^[0-9]{1,12}$"}
    assert "tenant_id" not in settable["properties"] and settable["additionalProperties"] is False
    assert client.get("/nope").json()["code"] == "not-found"


def test_the_docs_page_is_off_unless_enabled(api):
    client, _, _ = api
    assert client.get("/docs").status_code == 404


def test_the_docs_page_is_swagger_ui_locked_to_its_assets_and_the_service(feed):
    client = testclient.TestClient(app.create_app(feed.service(docs={"enabled": True})))
    response = client.get("/docs")       # a page without data: no key needed; the key is asked for in the page
    assert response.status_code == 200 and response.headers["content-type"].startswith("text/html")
    policy = response.headers["content-security-policy"]
    nonce = policy.split("'nonce-")[1].split("'")[0]
    assert '<script nonce="%s">' % nonce in response.text and "connect-src 'self'" in policy
    assert "default-src 'none'" in policy and "frame-ancestors 'none'" in policy
    assert response.text.count('integrity="sha384-') == 2 and "swagger-ui-dist@5.17.14" in response.text
    assert '<div id="key">' in response.text          # the service has auth: the key field shows
    assert response.headers["x-request-id"] and response.headers["cache-control"] == "no-store"
    assert client.get("/docs").headers["content-security-policy"] != policy     # a new nonce each time


def test_a_service_without_auth_serves_its_one_caller_and_no_key_field(feed):
    service = feed.service({"client_feed": {"manifest": "job.yaml", "set": {"tenant_id": {"value": "t1"}}}}, auth={},
                           docs={"enabled": True})
    client = testclient.TestClient(app.create_app(service))
    assert [job["name"] for job in client.get("/v1/jobs").json()["jobs"]] == ["client_feed"]
    assert '<div id="key" hidden>' in client.get("/docs").text


def test_validate_plan_and_status(api):
    client, _, _worker = api
    body = json.dumps({"set": {"client_id": "1001", "feed_date": DAY}})
    assert client.post("/v1/jobs/client_feed/validate", content=body, headers=_headers()).json()["valid"] is True
    assert "totals" in client.get("/v1/jobs/client_feed/plan", headers=_headers()).json()["plan"]
    run_id = _queued(client, {"set": {"client_id": "1001", "feed_date": DAY}})
    runs = client.get("/v1/jobs/client_feed/status", headers=_headers()).json()["runs"]
    assert [run["run_id"] for run in runs] == [run_id]
    assert client.get("/v1/jobs/client_feed/status", headers=_headers(OTHER_KEY)).json()["runs"] == []


def test_the_rate_limit_counts_the_callers_queued_runs_so_it_holds_on_every_host(feed):
    entry = {"manifest": "job.yaml", "principals": ["crm", "ops"], "set": {"tenant_id": {"from_claim": "tenant"}},
             "server_options": {"apply_state_migrations": True}, "limits": {"rate": {"requests": 2, "per": "1m"}}}
    service = feed.service({"client_feed": entry})
    hosts = [testclient.TestClient(app.create_app(service)), testclient.TestClient(app.create_app(service))]
    _queued(hosts[0], {"set": {"client_id": "1001", "feed_date": DAY}}, **{"Idempotency-Key": "a"})
    _queued(hosts[1], {"set": {"client_id": "1002", "feed_date": DAY}})
    # a replay of an Idempotency-Key is its run, not a new one
    assert _run(hosts[1], {"set": {"client_id": "1001", "feed_date": DAY}, "options": {"async": True}},
                **{"Idempotency-Key": "a"}).status_code == 202
    refused = _run(hosts[1], {"set": {"client_id": "1003", "feed_date": DAY}, "options": {"async": True}})
    assert refused.status_code == 429 and refused.json()["code"] == "rate-limited"
    assert 1 <= int(refused.headers["Retry-After"]) <= 61
    # another caller has a window of its own
    assert _run(hosts[0], {"set": {"client_id": "1003", "feed_date": DAY}, "options": {"async": True}},
                OTHER_KEY).status_code == 202


def test_metrics_report_the_queue_of_the_callers_jobs_and_this_hosts_requests(feed):
    assert testclient.TestClient(app.create_app(feed.service())).get("/metrics", headers=_headers()).status_code == 404
    service = feed.service(metrics={"enabled": True})
    client, worker = testclient.TestClient(app.create_app(service)), feed.worker(service)
    feed.write_feed("t1", "1001")
    _queued(client, {"set": {"client_id": "1001", "feed_date": DAY}})
    _queued(client, {"set": {"client_id": "1002", "feed_date": DAY}})
    worker.run_one()
    assert client.get("/metrics").status_code == 401
    response = client.get("/metrics", headers=_headers())
    assert response.status_code == 200 and response.headers["content-type"].startswith("text/plain; version=0.0.4")
    text = response.text
    for line in ('sw_queue_runs{job="client_feed",status="queued"} 1',
                 'sw_queue_runs{job="client_feed",status="running"} 0',
                 'sw_recent_runs{job="client_feed",status="succeeded"} 1',
                 'sw_recent_runs{job="client_feed",status="failed"} 0',
                 'sw_http_requests_total{route="/v1/jobs/{job}/runs",method="POST",status="202"} 2',
                 'sw_http_requests_total{route="/metrics",method="GET",status="401"} 1',
                 '# TYPE sw_http_request_seconds summary'):
        assert line in text, line
    assert 'sw_queue_oldest_seconds{job="client_feed"} 0\n' not in text
    assert 'sw_recent_run_seconds_max{job="client_feed"}' in text
