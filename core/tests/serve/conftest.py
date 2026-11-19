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

"""Fixtures of the `streamwright serve` tests: a feed job, a serve.yaml, the service, its API and a worker."""

import copy
import hashlib
import json
from types import MappingProxyType

import pytest
import yaml

from streamwright.core.serve.config import load_service
from streamwright.core.serve.queue_store import open_queue
from streamwright.core.serve.service import Service
from streamwright.core.serve.worker import Worker, execute_run

FEED_JOB = MappingProxyType({
    "kind": "DuckDBJob", "name": "client_feed",
    "spec": {"config": {
        "tenant_id": {"type": "string", "pattern": "^[a-z0-9_]{1,32}$"},
        "client_id": {"type": "string", "pattern": "^[0-9]{1,12}$"},
        "feed_date": {"type": "date"},
        "label": {"type": "string", "required": False, "max_length": 40},
        "region": {"type": "string", "enum": ["eu", "us"], "required": False}},
        "secrets": {"api_token": {"type": "string", "required": False}}},
    "connections": {"feeds": {"provider": "files", "roots": ["<feeds>"]},
                    "out": {"provider": "files", "roots": ["<out>"]}},
    "steps": [{"name": "rows", "connection": "feeds", "read": "csv",
               "path": "{{ config.tenant_id }}/{{ config.client_id }}/{{ config.feed_date }}.csv"}],
    "outputs": [{"name": "totals", "connection": "out", "path": "{{ config.tenant_id }}/{{ config.client_id }}",
                 "format": "csv", "mode": "overwrite",
                 "sql": "SELECT '{{ config.tenant_id }}' AS tenant, count(*) AS n, sum(amount) AS total FROM rows"}],
    "runtime": {"state": {"sqlite": "<state>", "scope": "client:{{ config.tenant_id }}/{{ config.client_id }}"}},
})
DEFAULT_ENTRY = MappingProxyType({
    "manifest": "job.yaml", "principals": ["crm", "ops"], "set": {"tenant_id": {"from_claim": "tenant"}},
    "server_options": {"apply_state_migrations": True}, "limits": {"timeout": "2m", "wait_seconds": 20}})


def in_process(manifest, options, timeout, log_settings, lost=None):
    """A worker's execute: the run in this process (no timeout)."""
    return execute_run(manifest, options)


class Feed:
    """A folder with the feed job (feeds/<tenant>/<client>/<day>.csv in, out/<tenant>/<client> out) and its service."""

    key, other_key, day = "sk-test-crm-0123456789", "sk-test-ops-0123456789", "2026-11-16"

    def __init__(self, folder):
        self.folder = folder

    @staticmethod
    def job():
        return copy.deepcopy(dict(FEED_JOB))

    def write_job(self, job=None, name="job.yaml"):
        text = json.dumps(job or self.job())
        for marker, value in (("<feeds>", "feeds"), ("<out>", "out"), ("<state>", "state.db")):
            text = text.replace(marker, str(self.folder / value))
        for folder in ("feeds", "out"):
            (self.folder / folder).mkdir(exist_ok=True)
        path = self.folder / name
        path.write_text(yaml.safe_dump(json.loads(text), sort_keys=False))
        return path

    def write_feed(self, tenant, client, day=None, rows=((1, 10), (2, 20))):
        target = self.folder / "feeds" / tenant / client
        target.mkdir(parents=True, exist_ok=True)
        (target / ("%s.csv" % (day or self.day))).write_text("id,amount\n" + "".join("%s,%s\n" % row for row in rows))

    def output(self, tenant, client):
        return (self.folder / "out" / tenant / client).read_text().splitlines()

    def serve_yaml(self, jobs=None, **top):
        data = {"bind": "127.0.0.1:8080", "state": {"url": "sqlite:///queue.db"},
                "auth": {"api_keys": [{"id": "crm", "sha256_env": "SW_KEY_CRM", "claims": {"tenant": "t1"}},
                                      {"id": "ops", "sha256_env": "SW_KEY_OPS", "claims": {"tenant": "t2"}}]},
                "jobs": jobs or {"client_feed": copy.deepcopy(dict(DEFAULT_ENTRY))}}
        data.update(top)
        path = self.folder / "serve.yaml"
        path.write_text(yaml.safe_dump(data, sort_keys=False))
        return path

    def environ(self):
        return {"SW_KEY_CRM": hashlib.sha256(self.key.encode()).hexdigest(),
                "SW_KEY_OPS": hashlib.sha256(self.other_key.encode()).hexdigest()}

    def config(self, jobs=None, **top):
        return load_service(str(self.serve_yaml(jobs, **top)), environ=self.environ())

    def service(self, jobs=None, **top):
        config = self.config(jobs, **top)
        store = open_queue(config.state)
        store.initialize()
        return Service(config, store)

    @staticmethod
    def worker(service, worker_id="test-worker", **options):
        return Worker(service.jobs, service.store, execute=options.pop("execute", in_process), worker_id=worker_id,
                      **options)

    def headers(self, key=None, **extra):
        return {"Authorization": "Bearer %s" % (key or self.key), "Content-Type": "application/json", **extra}


@pytest.fixture
def feed(tmp_path):
    feed = Feed(tmp_path)
    feed.write_job()
    return feed
