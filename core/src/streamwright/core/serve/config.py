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
"""
serve.yaml (plan §5.2): the jobs a service exposes, who may call them and what they may set. Nothing is reachable
unless it is listed; the manifests are read, never written.

    bind: 127.0.0.1:8080                      # a non-loopback address needs an `auth` section
    state:                                    # the run queue (sw_requests); optional on loopback: queue.db here
                                              # (under systemd: $STATE_DIRECTORY/queue.db)
      url: postgresql://sw:{{ secrets.state_password }}@pg:5432/streamwright?schema=sw   # or sqlite:///queue.db
      # or a connection, as a job declares one, and its schema:
      # connection: {provider: postgres, host: pg, port: 5432, database: streamwright, user: sw,
      #              password: "{{ secrets.state_password }}", sslmode: require}
      # schema: sw
    auth:
      api_keys:                               # SHA-256 hex digests of the keys, from the environment
        - {id: crm_prod, sha256_env: SW_KEY_CRM_PROD_SHA256, claims: {tenant: t1}}
      jwt: {issuer: https://idp.example.com, audience: streamwright, jwks_url: https://idp.example.com/jwks}
    docs: {enabled: true}                     # Swagger UI at /docs (default: off, /docs is 404)
    metrics: {enabled: true}                  # Prometheus metrics at /metrics (default: off)
    jobs:
      conversions:
        manifest: jobs/conversions.yaml       # relative to serve.yaml
        principals: [crm_prod]                # an api_keys id grants that key; any other entry, that JWT `sub`
        set:                                  # only the exceptions: every other spec.config input is settable
          tenant_id: {from_claim: tenant}     #   bound from the caller's claim (a body value is an error)
          region: {value: eu}                 #   fixed
        payload: {max_bytes: 5MB}             # jobs with spec.input
        server_options: {allow_lookup_exec: true, fan_out_concurrency: 1}
        limits: {concurrency: 2, rate: {requests: 10, per: 1m}, timeout: 10m, wait_seconds: 30}
        on_busy: reject                       # a run of the same scope is active: 409 (reject) or wait behind it
"""

import ipaddress
import os
import re
import urllib.parse
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

import yaml

from streamwright.core.config.inputs import SECRET_ENV_PREFIX, InputError, parse_duration, secrets_from_env
from streamwright.core.jobs.fields import Field, check_field, path_text

__all__ = ["DEFAULT_BIND", "SERVE_FIELD", "ApiKey", "JobEntry", "Jwt", "Service", "ServiceError", "StateSpec",
           "load_service", "load_state", "parse_size"]

DEFAULT_BIND = "127.0.0.1:8080"
ON_BUSY = ("reject", "queue")
# the run options a service fixes for a job (the operator's CLI flags; never a caller's)
SERVER_OPTIONS = MappingProxyType({
    "allow_lookup_exec": Field("boolean", description="command and function lookups may run (--allow-lookup-exec)"),
    "fan_out_concurrency": Field("integer", minimum=1, description="fan-out items run at once"),
    "fan_out_on_failure": Field("string", enum=("continue", "stop"), description="after a failed fan-out item"),
    "failed_job_policy": Field("string", enum=("abandon", "retry", "block"), description="a failed batch of the "
                                                                                         "scope: what the run does"),
    "max_job_attempts": Field("integer", minimum=1, description="starts of a failed batch, in all"),
    "state_lock_policy": Field("string", enum=("fail", "wait", "prune"), description="a held lock item"),
    "state_lock_timeout": Field("string", description="how long `wait` waits, e.g. 5m"),
    "allowed_connectors": Field("array", items=Field("string"), description="only these connectors (allow-list)"),
    "apply_state_migrations": Field("boolean", description="the run applies pending state migrations"),
})
_SIZE = Field(("integer", "string"), description="bytes, or text like 512KB, 5MB")
_DURATION = Field("string", pattern=r"^[0-9]+[smhd]$", description="e.g. 30s, 10m, 2h")
SERVE_FIELD = Field("object", fields={
    "bind": Field("string", description="host:port (default %s)" % DEFAULT_BIND),
    "state": Field("object", fields={
        "url": Field("string", description="a state database URL, as `state --url` takes one (sqlite:///queue.db is "
                                           "relative to serve.yaml); {{ secrets.NAME }} for credentials"),
        "connection": Field("object", description="a connection as a job declares one (a provider whose connector "
                                                  "holds state, and its keys); {{ secrets.NAME }} for credentials"),
        "schema": Field("string", description="with connection: the schema of the tables (default streamwright)")},
        description="the run queue: url or connection (required unless bind is a loopback address: then queue.db "
                    "beside serve.yaml; under systemd, in $STATE_DIRECTORY)"),
    "auth": Field("object", fields={
        "api_keys": Field("array", items=Field("object", fields={
            "id": Field("string", required=True, pattern=r"^[A-Za-z0-9._-]{1,64}$"),
            "sha256_env": Field("string", required=True, description="the variable holding the key's SHA-256 (hex)"),
            "claims": Field("object", values=Field(("string", "integer", "boolean")),
                            description="the claims the key carries"),
        })),
        "jwt": Field("object", fields={
            "issuer": Field("string", required=True), "audience": Field("string", required=True),
            "jwks_url": Field("string", required=True, pattern=r"^https://"),
        }),
    }),
    "docs": Field("object", fields={
        "enabled": Field("boolean", description="serve Swagger UI over the caller's API at /docs (default false)")}),
    "metrics": Field("object", fields={
        "enabled": Field("boolean", description="serve Prometheus metrics at /metrics (default false)")}),
    "jobs": Field("object", required=True, min_items=1, values=Field("object", fields={
        "manifest": Field("string", required=True, description="the job manifest, relative to serve.yaml"),
        "principals": Field("array", min_items=1, items=Field("string"),
                            description="who may call the job (required with `auth`; without it, the one caller may)"),
        "set": Field("object", values=Field("object", fields={
            "from_claim": Field("string", description="bound from this claim of the caller"),
            "value": Field(description="a fixed value")})),
        "payload": Field("object", fields={"max_bytes": _SIZE}),
        "server_options": Field("object", fields=dict(SERVER_OPTIONS)),
        "limits": Field("object", fields={
            "concurrency": Field("integer", minimum=1, description="runs of the job at once (all workers)"),
            "rate": Field("object", fields={"requests": Field("integer", required=True, minimum=1),
                                            "per": Field("string", required=True, pattern=_DURATION.pattern)}),
            "timeout": _DURATION, "wait_seconds": Field("integer", minimum=0)}),
        "on_busy": Field("string", enum=ON_BUSY),
    })),
})
_SIZE_TEXT = re.compile(r"^([0-9]+)\s*(B|KB|MB|GB)?$", re.IGNORECASE)
_SIZE_UNITS = MappingProxyType({"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3})
_SECRET = re.compile(r"\{\{\s*secrets\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_REFERENCE = re.compile(r"\{\{.*?\}\}", re.S)
JOB_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")    # e.g. google_ads.campaigns.get
DEFAULT_QUEUE_FILE = "queue.db"
DEFAULT_WAIT_SECONDS = 30
DEFAULT_TIMEOUT_SECONDS = 3600


class ServiceError(Exception):
    """serve.yaml cannot be used: every problem, one per line."""

    def __init__(self, problems):
        super().__init__("\n".join(problems))
        self.problems = list(problems)


def parse_size(value):
    """Bytes from an integer or text like 512KB or 5MB (1 KB = 1024 bytes)."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    match = _SIZE_TEXT.match(str(value).strip())
    if not match:
        raise InputError("invalid size %r (expected bytes, or e.g. 512KB, 5MB)" % (value,))
    return int(match.group(1)) * _SIZE_UNITS[(match.group(2) or "B").upper()]


@dataclass(frozen=True)
class ApiKey:
    id: str
    sha256: str
    claims: Any


@dataclass(frozen=True)
class Jwt:
    issuer: str
    audience: str
    jwks_url: str


@dataclass(frozen=True)
class JobEntry:
    name: str
    manifest: str
    principals: tuple
    bound: Any                                   # {input: ("claim", name) | ("value", value)}
    payload_max_bytes: int | None
    server_options: Any
    concurrency: int | None
    rate: tuple | None                           # (requests, seconds)
    timeout_seconds: int
    wait_seconds: int
    on_busy: str


@dataclass(frozen=True)
class StateSpec:
    """The run queue's database: a URL, or a connection (provider and keys) and its schema; secrets rendered."""
    url: str | None = None
    connection: Any = None
    schema: str | None = None


@dataclass(frozen=True)
class Service:
    path: str
    host: str
    port: int
    state: StateSpec
    api_keys: tuple = ()
    jwt: Jwt | None = None
    jobs: Any = field(default_factory=lambda: MappingProxyType({}))
    docs: bool = False                           # Swagger UI at /docs
    metrics: bool = False                        # Prometheus metrics at /metrics

    @property
    def has_auth(self):
        return bool(self.api_keys) or self.jwt is not None

    @property
    def loopback(self):
        try:
            return ipaddress.ip_address(self.host).is_loopback
        except ValueError:
            return self.host == "localhost"


def _bind(text, problems):
    host, separator, port = str(text).rpartition(":")
    if not separator or not port.isdigit() or not 0 < int(port) < 65536 or not host:
        problems.append("bind: expected host:port, got %r" % (text,))
        return "127.0.0.1", 8080
    return host.strip("[]"), int(port)


def _env(name, what, environ, problems):
    value = environ.get(name)
    if not value:
        problems.append("%s: the environment variable %s is not set" % (what, name))
    return value or ""


def _secrets(value, where, environ, problems, *, in_url=False):
    """`value` with each {{ secrets.NAME }} replaced by STREAMWRIGHT_SECRET_<NAME> (percent-encoded in a URL)."""
    if isinstance(value, dict):
        return {key: _secrets(item, "%s.%s" % (where, key), environ, problems) for key, item in value.items()}
    if not isinstance(value, str) or "{{" not in value:
        return value
    problems.extend("%s: %s: only {{ secrets.NAME }} references are allowed here" % (where, reference)
                    for reference in _REFERENCE.findall(value) if not _SECRET.fullmatch(reference))
    found = secrets_from_env(_SECRET.findall(value), environ)

    def secret(match):
        if match.group(1) not in found:
            problems.append("%s: the secret %s is not set (%s%s)" % (where, match.group(1), SECRET_ENV_PREFIX,
                                                                     match.group(1).upper()))
            return ""
        return urllib.parse.quote(found[match.group(1)], safe="") if in_url else found[match.group(1)]

    return _SECRET.sub(secret, value)


def _beside(url, folder):
    """A sqlite:/// URL with a relative path, made relative to serve.yaml's folder."""
    scheme, separator, rest = url.partition(":///")
    if scheme != "sqlite" or not separator or rest.startswith("/") or rest.split("?")[0] in ("", ":memory:"):
        return url
    return "%s:///%s" % (scheme, os.path.join(folder, rest))


def _default_queue(folder, environ):
    """
    The queue of a service without `state`: queue.db in systemd's StateDirectory when the service runs under one
    (its units' writable folder: serve.yaml's folder is read-only there), else beside serve.yaml.
    """
    state_directory = (environ.get("STATE_DIRECTORY") or "").split(":")[0]
    return "sqlite:///" + os.path.join(state_directory or folder, DEFAULT_QUEUE_FILE)


def _state(state, folder, environ, problems, env_problems=None):
    """The StateSpec of serve.yaml's `state`; a missing secret is one of `env_problems` (default: `problems`)."""
    env_problems = problems if env_problems is None else env_problems
    if not state:                                 # local use (checked by the caller)
        return StateSpec(url=_default_queue(folder, environ))
    if ("url" in state) == ("connection" in state):
        problems.append("state: give exactly one of url or connection")
        return StateSpec(url="")
    if "url" in state:
        if "schema" in state:
            problems.append("state.schema: goes with state.connection; a URL names its schema (?schema=NAME)")
        if str(state["url"]).startswith("duckdb:"):
            problems.append("state.url: a DuckDB file cannot hold the run queue: serve and its workers are separate "
                            "processes, and DuckDB lets one process write a file (sqlite:///queue.db locally, or a "
                            "network database)")
        return StateSpec(url=_beside(_secrets(state["url"], "state.url", environ, env_problems, in_url=True), folder))
    connection = _secrets(dict(state["connection"]), "state.connection", environ, env_problems)
    if not connection.get("provider"):
        problems.append("state.connection.provider: required (a provider whose connector holds state)")
    return StateSpec(connection=MappingProxyType(connection), schema=state.get("schema"))


def _api_keys(auth, environ, problems, env_problems):
    keys = []
    for index, key in enumerate(auth.get("api_keys") or ()):
        digest = _env(key["sha256_env"], "auth.api_keys[%d].sha256_env" % index, environ,
                      env_problems).strip().lower()
        if digest and not re.fullmatch(r"[0-9a-f]{64}", digest):
            env_problems.append("auth.api_keys[%d]: %s is not a SHA-256 hex digest (64 hex digits)" % (
                index, key["sha256_env"]))
        keys.append(ApiKey(key["id"], digest, MappingProxyType(dict(key.get("claims") or {}))))
    ids = [key.id for key in keys]
    problems.extend("auth.api_keys: the id %r is listed twice" % name for name in sorted({i for i in ids
                                                                                       if ids.count(i) > 1}))
    return tuple(keys)


def _job(name, entry, folder, problems):
    where = "jobs.%s" % name
    if not JOB_NAME.match(str(name)):
        problems.append("%s: a job name is its URL segment (/v1/jobs/NAME): 1-128 of letters, digits and _ . -, "
                        "starting with a letter or digit" % where)
    bound = {}
    for input_name, binding in (entry.get("set") or {}).items():
        kinds = [key for key in ("from_claim", "value") if key in binding]
        if len(kinds) != 1:
            problems.append("%s.set.%s: give exactly one of from_claim or value" % (where, input_name))
            continue
        bound[input_name] = ("claim", binding["from_claim"]) if kinds == ["from_claim"] else ("value", binding["value"])
    limits = entry.get("limits") or {}
    try:
        timeout = int(parse_duration(limits["timeout"]).total_seconds()) if "timeout" in limits \
            else DEFAULT_TIMEOUT_SECONDS
        rate = limits.get("rate")
        rate = (int(rate["requests"]), int(parse_duration(rate["per"]).total_seconds())) if rate else None
        max_bytes = (entry.get("payload") or {}).get("max_bytes")
        max_bytes = parse_size(max_bytes) if max_bytes is not None else None
    except InputError as exc:
        problems.append("%s: %s" % (where, exc))
        timeout, rate, max_bytes = DEFAULT_TIMEOUT_SECONDS, None, None
    return JobEntry(
        name=name, manifest=os.path.normpath(os.path.join(folder, entry["manifest"])),
        principals=tuple(entry.get("principals") or ()), bound=MappingProxyType(bound), payload_max_bytes=max_bytes,
        server_options=MappingProxyType(dict(entry.get("server_options") or {})),
        concurrency=limits.get("concurrency"), rate=rate, timeout_seconds=timeout,
        wait_seconds=int(limits.get("wait_seconds", DEFAULT_WAIT_SECONDS)), on_busy=entry.get("on_busy", "reject"))


def _read(path):
    try:
        with open(path, encoding="utf-8") as stream:
            data = yaml.safe_load(stream)
    except (OSError, yaml.YAMLError) as exc:
        raise ServiceError(["%s: %s" % (path, exc)]) from None
    found = check_field(data, SERVE_FIELD)
    if found:
        raise ServiceError(["%s: %s" % (path_text(where) or "serve.yaml", message) for _, where, message in found])
    return data


def load_state(path, environ=None):
    """The StateSpec of serve.yaml at `path` alone (`sw queue`: no API key or manifest is read); raises
    ServiceError."""
    problems = []
    state = _state(_read(path).get("state"), os.path.dirname(os.path.abspath(path)),
                   os.environ if environ is None else environ, problems)
    if problems:
        raise ServiceError(problems)
    return state


def load_service(path, environ=None, *, static=False):
    """
    The Service of serve.yaml at `path` (its jobs' manifests are checked by serve.rules); raises ServiceError.
    `static` (streamwright validate): what the environment gives - API key digests, secrets - is not required.
    """
    environ = os.environ if environ is None else environ
    data = _read(path)
    problems, ignored = [], []
    env_problems = ignored if static else problems
    host, port = _bind(data.get("bind", DEFAULT_BIND), problems)
    folder = os.path.dirname(os.path.abspath(path))
    state = _state(data.get("state"), folder, environ, problems, env_problems)
    auth = data.get("auth") or {}
    jwt = Jwt(**auth["jwt"]) if auth.get("jwt") else None
    service = Service(path=os.path.abspath(path), host=host, port=port, state=state,
                      api_keys=_api_keys(auth, environ, problems, env_problems), jwt=jwt,
                      jobs=MappingProxyType({name: _job(name, entry, folder, problems)
                                             for name, entry in data["jobs"].items()}),
                      docs=bool((data.get("docs") or {}).get("enabled", False)),
                      metrics=bool((data.get("metrics") or {}).get("enabled", False)))
    if not data.get("state") and not service.loopback:
        problems.append("state: required when bind (%s) is not a loopback address: the run queue of a service other "
                        "hosts reach is a database it names (state: {url: ...} or {connection: ...})" % host)
    if not service.has_auth and not service.loopback:
        problems.append("bind: %s is not a loopback address, and serve.yaml has no `auth` section: a service "
                        "reachable from other hosts must authenticate its callers" % host)
    known = {key.id for key in service.api_keys}
    for job in service.jobs.values():
        if service.has_auth and not job.principals:
            problems.append("jobs.%s.principals: `principals` is required: serve.yaml has an `auth` section, and "
                            "a job is reachable only by the callers it lists" % job.name)
        elif service.has_auth and service.jwt is None:
            problems.extend("jobs.%s.principals: %r is not an api_keys id" % (job.name, principal)
                            for principal in job.principals if principal not in known)
    if problems:
        raise ServiceError(problems)
    return service
