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

"""serve.yaml and what a service checks before it exposes a job (plan §5.2-§5.5), and its callers' credentials."""

import time
import types

import pytest
import yaml

from streamwright.core import cli
from streamwright.core.jobs.state.url import open_url
from streamwright.core.serve import cli as serve_cli
from streamwright.core.serve.config import ServiceError, StateSpec, load_service, parse_size
from streamwright.core.serve.envelope import RequestError
from streamwright.core.serve.queue_store import open_queue
from streamwright.core.serve.rules import PATH_CHARACTERS, SQL_CHARACTERS, confines
from streamwright.core.serve.service import Service
from streamwright.core.validation.engine import validate_paths

jwt = pytest.importorskip("jwt")
auth = pytest.importorskip("streamwright.core.serve.auth")
rsa = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.rsa")
ec = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.ec")


@pytest.mark.parametrize("definition,sql,path", [
    ({"pattern": "^[A-Za-z0-9_-]{1,64}$"}, True, True),
    ({"pattern": "^[0-9]{1,12}$"}, True, True),
    ({"pattern": r"^v[0-9]+\.[0-9]+$"}, True, False),       # a dot: SQL text, never a path
    ({"pattern": r"^\w+$"}, True, True),
    ({"pattern": "^(eu|us)$"}, True, True),
    ({"pattern": ".*"}, False, False),
    ({"pattern": "^[^']*$"}, False, False),                 # a negated class lists nothing
    ({"pattern": r"^\S+$"}, False, False),
    ({"pattern": "^[a-z]+(/[a-z]+)*$"}, False, False),
    ({"pattern": "^[a-z ]+$"}, False, False),
    ({"pattern": "(?i)^[a-k]+$"}, False, False),            # case folding reaches past ASCII
    ({"pattern": "^[ -~]+$"}, False, False),                # a range over punctuation
    ({"pattern": "(["}, False, False),
    ({"enum": ["eu", "us"]}, True, True),
    ({"enum": ["a/b"]}, False, False),
    ({"enum": ["v1.2"]}, True, False),
    ({}, False, False),
])
def test_a_pattern_confines_an_input_when_every_character_it_matches_is_safe(definition, sql, path):
    assert (confines(definition, SQL_CHARACTERS), confines(definition, PATH_CHARACTERS)) == (sql, path)


def _refusals(feed, job, entry=None):
    feed.write_job(job)
    with pytest.raises(ServiceError) as raised:
        feed.service({"client_feed": dict(entry or {"manifest": "job.yaml", "principals": ["crm"],
                                                    "set": {"tenant_id": {"from_claim": "tenant"}}})})
    return "\n".join(raised.value.problems)


def test_a_settable_text_input_in_sql_needs_a_confining_pattern(feed):
    job = feed.job()
    job["spec"]["config"]["label"]["default"] = "all"
    job["outputs"][0]["sql"] += " WHERE '{{ config.label }}' <> ''"
    problems = _refusals(feed, job)
    assert "config input 'label' (settable by callers) is used in outputs[0].sql" in problems
    job["spec"]["config"]["label"]["pattern"] = "^[a-z_]{1,40}$"
    feed.write_job(job)
    assert "client_feed" in feed.service().jobs


def test_a_settable_input_in_a_path_needs_a_path_safe_pattern(feed):
    job = feed.job()
    job["spec"]["config"]["client_id"]["pattern"] = "^[0-9a-z./]{1,12}$"
    problems = _refusals(feed, job)
    assert "config input 'client_id' (settable by callers) is used in steps[0].path" in problems
    assert "outputs[0].path" in problems
    job["spec"]["config"]["client_id"] = {"type": "integer"}
    feed.write_job(job)
    assert "client_feed" in feed.service().jobs


def test_a_tenant_input_left_settable_is_refused_where_it_shapes_paths(feed):
    entry = {"manifest": "job.yaml", "principals": ["crm"]}     # tenant_id is not bound: a caller would set it
    job = feed.job()
    job["spec"]["config"]["tenant_id"]["pattern"] = ".+"
    assert "config input 'tenant_id' (settable by callers) is used in steps[0].path" in _refusals(feed, job, entry)


@pytest.mark.parametrize("binding,text", [
    ({"tenant": {"from_claim": "tenant"}}, "declares no config input 'tenant'"),
    ({"api_token": {"value": "x"}}, "a secret is never set by a request"),
    ({"tenant_id": {"from_claim": "tenant", "value": "t1"}}, "exactly one of from_claim or value"),
])
def test_bindings_name_config_inputs(feed, binding, text):
    assert text in _refusals(feed, feed.job(), {"manifest": "job.yaml", "principals": ["crm"], "set": binding})


def test_an_invalid_job_is_not_served(feed):
    job = feed.job()
    job["steps"][0]["read"] = "xlsx"
    assert "is not a valid job" in _refusals(feed, job)


def test_a_lookup_not_filtered_by_a_claim_is_a_warning(feed, caplog):
    job = feed.job()
    job["connections"]["accounts"] = {"provider": "sqlite", "path": str(feed.folder / "accounts.db")}
    job["spec"]["lookups"] = {"account": {"type": "database", "connection": "accounts",
                                          "query": "SELECT 1 AS id WHERE :client IS NOT NULL",
                                          "params": {"client": "{{ config.client_id }}"}}}
    feed.write_job(job)
    feed.service()
    assert "lookup 'account' does not use an input bound from a claim (tenant_id)" in caplog.text


@pytest.mark.parametrize("top,text", [
    ({"bind": "0.0.0.0:8080", "auth": {}}, "not a loopback address, and serve.yaml has no `auth` section"),
    ({"bind": "localhost"}, "expected host:port"),
    ({"surprise": 1}, "unknown key 'surprise'"),
    ({"state": {"url": "postgresql://sw:{{ secrets.not_set_anywhere }}@pg/db"}}, "the secret not_set_anywhere is "
                                                                                 "not set"),
    ({"state": {"url": "sqlite:///{{ config.day }}.db"}}, "only {{ secrets.NAME }} references are allowed"),
    ({"state": {"url": "sqlite:///q.db", "connection": {"provider": "postgres"}}}, "exactly one of url or connection"),
    ({"state": {"url": "sqlite:///q.db", "schema": "sw"}}, "a URL names its schema (?schema=NAME)"),
    ({"state": {"url": "duckdb:///q.duckdb"}}, "a DuckDB file cannot hold the run queue"),
])
def test_serve_yaml_problems_stop_the_service(feed, top, text):
    with pytest.raises(ServiceError) as raised:
        feed.config(**top)
    assert text in str(raised.value)


def test_principals_are_api_key_ids_and_digests_are_sha256(feed):
    jobs = {"client_feed": {"manifest": "job.yaml", "principals": ["crm", "nobody"]}}
    with pytest.raises(ServiceError, match="'nobody' is not an api_keys id"):
        feed.config(jobs)
    environ = dict(feed.environ(), SW_KEY_CRM="not-a-digest")
    with pytest.raises(ServiceError, match="not a SHA-256 hex digest"):
        load_service(str(feed.serve_yaml()), environ=environ)


def test_principals_are_required_with_auth_and_not_without(feed):
    with pytest.raises(ServiceError, match=r"`principals` is required: serve\.yaml has an `auth` section"):
        feed.config({"client_feed": {"manifest": "job.yaml", "set": {"tenant_id": {"from_claim": "tenant"}}}})
    # a loopback service without `auth` has one caller, which may call every job
    service = feed.config({"client_feed": {"manifest": "job.yaml", "set": {"tenant_id": {"value": "t1"}}}}, auth={})
    assert not service.has_auth and service.jobs["client_feed"].principals == ()


@pytest.mark.parametrize("name", ["a/b", "has space", ".hidden", ""])
def test_a_job_name_is_a_url_segment(feed, name):
    with pytest.raises(ServiceError, match="a job name is its URL segment"):
        feed.config({name: {"manifest": "job.yaml", "principals": ["crm"],
                            "set": {"tenant_id": {"from_claim": "tenant"}}}})


def _without_state(feed, bind):
    data = yaml.safe_load(feed.serve_yaml().read_text())
    del data["state"]
    data["bind"] = bind
    path = feed.folder / "serve.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def test_a_local_service_without_state_queues_beside_its_serve_yaml(feed):
    path = _without_state(feed, "127.0.0.1:8080")
    assert load_service(str(path), environ=feed.environ()).state.url == "sqlite:///%s" % (feed.folder / "queue.db")
    # under systemd (StateDirectory=), in the units' writable folder: serve.yaml's folder is read-only there
    environ = dict(feed.environ(), STATE_DIRECTORY="/var/lib/streamwright")
    assert load_service(str(path), environ=environ).state.url == "sqlite:////var/lib/streamwright/queue.db"
    with pytest.raises(ServiceError, match="state: required when bind"):
        load_service(str(_without_state(feed, "0.0.0.0:8080")), environ=feed.environ())


def test_serve_creates_the_tables_of_a_new_queue(feed, monkeypatch):
    for name, value in feed.environ().items():
        monkeypatch.setenv(name, value)
    service, failed = serve_cli._service(types.SimpleNamespace(serve_yaml=str(feed.serve_yaml())))
    assert failed is None and service.store.check_schema()
    assert open_url("sqlite:///%s" % (feed.folder / "queue.db")).schema_version() > 0


def test_state_is_a_url_or_a_connection_with_secrets_from_the_environment(feed):
    environ = dict(feed.environ(), STREAMWRIGHT_SECRET_STATE_PASSWORD="p@ss/w:rd")
    url = "postgresql://sw:{{ secrets.state_password }}@pg:5432/sw?schema=queue"
    state = load_service(str(feed.serve_yaml(state={"url": url})), environ=environ).state
    assert state.url == "postgresql://sw:p%40ss%2Fw%3Ard@pg:5432/sw?schema=queue"     # percent-encoded in a URL
    relative = load_service(str(feed.serve_yaml(state={"url": "sqlite:///data/q.db"})), environ=environ).state
    assert relative.url == "sqlite:///%s" % (feed.folder / "data" / "q.db")            # beside serve.yaml
    connection = {"provider": "postgres", "host": "pg", "port": 6432, "database": "sw", "user": "sw",
                  "password": "{{ secrets.state_password }}", "sslmode": "require"}
    state = load_service(str(feed.serve_yaml(state={"connection": connection, "schema": "queue"})),
                         environ=environ).state
    assert state.connection["password"] == "p@ss/w:rd" and state.schema == "queue" and state.url is None
    store = open_queue(state)                     # nothing connects until the store is used
    assert store.target == "postgres connection 'state' (sw@pg:6432/sw), schema queue"
    with pytest.raises(ValueError, match="provider files cannot hold the run queue"):
        open_queue(StateSpec(connection={"provider": "files", "roots": ["."]}))
    with pytest.raises(ValueError, match=r"state\.connection: postgres: needs `database`"):
        open_queue(StateSpec(connection={"provider": "postgres", "host": "pg", "surprise": 1}))


def test_sizes():
    assert [parse_size(value) for value in (512, "512", "4KB", "5MB", "1gb")] == [512, 512, 4096, 5 << 20, 1 << 30]
    with pytest.raises(Exception, match="invalid size"):
        parse_size("5 megabytes")


# --- credentials -------------------------------------------------------------------------------------------------

class _Jwks:
    def __init__(self, key):
        self.key = key

    def get_signing_key_from_jwt(self, token):
        return jwt.PyJWK.from_dict(self.key)


@pytest.fixture
def issuer(feed):
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = jwt.algorithms.RSAAlgorithm.to_jwk(private.public_key(), as_dict=True)
    api_keys = yaml.safe_load(feed.serve_yaml().read_text())["auth"]["api_keys"]       # API keys and JWTs
    config = feed.config({"client_feed": {"manifest": "job.yaml", "principals": ["crm", "svc-crm"],
                                          "set": {"tenant_id": {"from_claim": "tenant"}}}},
                         auth={"api_keys": api_keys, "jwt": {"issuer": "https://idp.example.com",
                                                             "audience": "streamwright",
                                                             "jwks_url": "https://idp.example.com/jwks"}})
    authenticator = auth.Authenticator(config, jwks_client=_Jwks(dict(public, alg="RS256", kid="k1")))

    def token(signer=private, algorithm="RS256", **claims):
        now = int(time.time())
        payload = {"iss": "https://idp.example.com", "aud": "streamwright", "sub": "svc-crm", "exp": now + 60,
                   "tenant": "t9", **claims}
        return jwt.encode({key: value for key, value in payload.items() if value is not None}, signer,
                          algorithm=algorithm, headers={"kid": "k1"})

    return authenticator, token


def test_a_valid_jwt_is_its_subject_with_its_claims(issuer):
    authenticator, token = issuer
    principal = authenticator.authenticate("Bearer %s" % token())
    assert principal.id == "jwt:svc-crm" and principal.claims["tenant"] == "t9"


@pytest.mark.parametrize("claims", [
    {"exp": int(time.time()) - 3600}, {"aud": "other"}, {"iss": "https://evil.example.com"}, {"sub": None},
    {"nbf": int(time.time()) + 3600},
])
def test_expired_or_foreign_jwts_are_refused(issuer, claims):
    authenticator, token = issuer
    with pytest.raises(Exception) as raised:
        authenticator.authenticate("Bearer %s" % token(**claims))
    assert getattr(raised.value, "status", None) == 401


def test_unsigned_and_symmetric_jwts_are_refused(issuer):
    authenticator, _ = issuer
    claims = {"sub": "x", "iss": "https://idp.example.com", "aud": "streamwright", "exp": int(time.time()) + 60}
    for token in (jwt.encode(claims, "a-shared-secret-of-32-bytes-long!", algorithm="HS256"),
                  "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ4In0."):
        with pytest.raises(Exception) as raised:
            authenticator.authenticate("Bearer %s" % token)
        assert getattr(raised.value, "status", None) == 401


def test_a_jwt_is_checked_only_with_the_algorithms_of_its_key(issuer):
    authenticator, token = issuer
    private = ec.generate_private_key(ec.SECP256R1())
    public = jwt.algorithms.ECAlgorithm.to_jwk(private.public_key(), as_dict=True)
    with_ec_key = auth.Authenticator(authenticator.service, jwks_client=_Jwks(dict(public, kid="k1")))
    assert with_ec_key.authenticate("Bearer %s" % token(private, "ES256")).id == "jwt:svc-crm"
    with pytest.raises(RequestError) as raised:   # RS256 with the EC key's kid: refused, not an error
        with_ec_key.authenticate("Bearer %s" % token())
    assert raised.value.status == 401


def test_a_jwt_whose_subject_is_an_api_key_id_is_not_that_key(issuer, feed):
    authenticator, token = issuer
    service = Service(authenticator.service, None)
    as_jwt = authenticator.authenticate("Bearer %s" % token(sub="crm"))
    as_key = authenticator.authenticate("Bearer %s" % feed.key)
    assert (as_jwt.kind, as_jwt.id, as_key.kind, as_key.id) == ("jwt", "jwt:crm", "key", "key:crm")
    assert service.granted(as_jwt) == [] and service.granted(as_key) == ["client_feed"]
    with pytest.raises(RequestError) as raised:
        service.job("client_feed", as_jwt)
    assert raised.value.status == 403
    assert service.granted(authenticator.authenticate("Bearer %s" % token())) == ["client_feed"]    # svc-crm


def test_api_keys_match_by_digest(feed):
    authenticator = auth.Authenticator(feed.config())
    assert authenticator.authenticate("Bearer %s" % feed.key).id == "key:crm"
    assert authenticator.authenticate("bearer %s" % feed.other_key).claims["tenant"] == "t2"


def test_queue_purge_and_migrate_need_only_the_state(feed, capsys):
    path = feed.serve_yaml()                      # its API key digests are not in the environment: never read
    service = feed.service()
    for number in range(2):
        service.store.enqueue_request(job_name="client_feed", principal="crm", body_sha256="0" * 64, inputs={},
                                      request_id="r", scope_key="s%d" % number)
    claimed = service.store.claim_request("w", 30)
    service.store.finish_request(claimed["run_id"], "w", succeeded=True)
    assert cli.main(["queue", "migrate", str(path)]) == 0
    assert "up to date" in capsys.readouterr().out
    assert cli.main(["queue", "purge", str(path), "--older-than", "0d", "--dry-run"]) == 0
    assert capsys.readouterr().out.startswith("would purge 1 run(s) that ended before ")
    assert cli.main(["queue", "purge", str(path), "--older-than", "1d"]) == 0
    assert capsys.readouterr().out.startswith("purged 0 run(s)")
    assert cli.main(["queue", "purge", str(path), "--older-than", "0d"]) == 0
    assert capsys.readouterr().out.startswith("purged 1 run(s)")
    assert service.store.count_requests(("queued", "running", "succeeded", "failed")) == 1
    (feed.folder / "bad.yaml").write_text("state: {url: 'sqlite:///q.db', schema: x}\njobs: {a: {manifest: m}}\n")
    assert cli.main(["queue", "purge", str(feed.folder / "bad.yaml")]) == 2
    assert "a URL names its schema" in capsys.readouterr().err


def test_validate_checks_a_serve_yaml_without_its_environment(feed):
    path = feed.serve_yaml()                      # its API key digests are not in the environment: not needed
    files, issues = validate_paths([str(path)])
    assert files == [str(path)] and issues == []
    broken = feed.serve_yaml({"a/b": {"manifest": "missing.yaml", "principals": ["crm"]}}, docs={"enabled": "yes"})
    messages = [issue.message for issue in validate_paths([str(broken)])[1]]
    assert any("docs.enabled" in message for message in messages)
    feed.serve_yaml({"bad name": {"manifest": "job.yaml", "principals": ["crm"]}})
    assert "a job name is its URL segment" in validate_paths([str(path)])[1][0].message
    feed.serve_yaml({"client_feed": {"manifest": "missing.yaml", "principals": ["crm"]}})
    assert "missing.yaml is not a valid job" in validate_paths([str(path)])[1][0].message
