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

"""Validation catches what a call step's connector would reject or ignore at run time, before any request."""

import re

import pytest
import yaml

from streamwright.core.jobs.duckdb.cli import main
from streamwright.core.jobs.duckdb.plugin import JOB_ENGINE
from streamwright.core.jobs.state import url
from streamwright.core.runtime import components
from streamwright.core.runtime.source_connectors import select_transport

REST = tuple(name for name in ("openai_ads", "amazon_ads", "apple_ads", "linkedin_ads", "restapi")
        if name in components.available())


def _job(step, **connection):
    return {
        "kind": "DuckDBJob", "name": "probe",
        "spec": {"secrets": {"tok": {"type": "string", "required": True}}},
        "connections": {"api": {"provider": "openai_ads", "type": "bearer", "token": "{{ secrets.tok }}",
                                **connection},
                        "out": {"provider": "files", "roots": ["out"]}},
        "steps": [{"name": "campaigns", "connection": "api", **step, "sql": "SELECT record FROM records"}],
        "outputs": [{"name": "o", "connection": "out", "step": "campaigns", "mode": "overwrite", "path": "c.parquet",
                     "format": "parquet"}],
    }


def _codes(job):
    return {(issue.code, issue.path) for issue in JOB_ENGINE.validate_job(job).issues}


@pytest.mark.parametrize("name", REST)
def test_every_rest_connector_guide_has_a_duckdb_job_that_validates_clean(name):
    assert select_transport(components.load(name)) == "http"
    section = components.guide(name).split("\n## DuckDBJob\n", 1)[1].split("\n## ", 1)[0]
    text = re.search(r"```yaml\n(.*?)```", section, re.S).group(1)
    report = JOB_ENGINE.validate_job_text(text, strict=True)
    assert report.valid, [str(issue) for issue in report.issues]
    assert yaml.safe_load(text)["connections"]["out"]["provider"] == "files"


@pytest.mark.skipif("openai_ads" not in REST, reason="the openai_ads connector is not installed")
def test_a_rest_connector_rejects_sdk_calls_ignored_keys_and_a_relative_path_without_base_url():
    absolute = {"operation": {"path": "https://api.example.com/v1/campaigns"}}
    assert not {code for code, _ in _codes(_job(absolute))} & {"bad-call", "unsupported-key", "bad-connection"}
    assert ("bad-call", "steps[0]") in _codes(_job({"operation": {"service": "CampaignService", "method": "list"}}))
    assert ("bad-call", "steps[0]") in _codes(_job({"operation": {"path": "/v1/campaigns"}}))    # relative, no base_url
    assert ("unsupported-key", "connections.api") in _codes(_job(absolute, bogus=1))         # would be ignored
    grouped = _codes(_job({"operation": {"path": "/v1/campaigns"}}, http={"base_url": "https://api.example.com"}))
    assert ("bad-call", "steps[0]") not in grouped
    flat = _codes(_job({"operation": {"path": "/v1/campaigns"}}, base_url="https://api.example.com"))
    assert ("unknown-key", "connections.api.base_url") in flat


def test_a_database_lookup_whose_driver_is_missing_warns_offline_and_stops_a_run(monkeypatch):
    if "postgres" not in components.available():
        pytest.skip("the postgres connector is not installed")
    monkeypatch.setattr(url, "DRIVERS", {"postgres": (("no_such_driver_module",), "pip install a-driver")})
    job = _job({"operation": {"path": "https://api.example.com/v1/campaigns"}})
    job["connections"]["db"] = {"provider": "postgres", "host": "db.example.com", "database": "app", "user": "u",
                                "password": "{{ secrets.tok }}"}
    job["spec"]["lookups"] = {"rows": {"type": "database", "connection": "db", "query": "SELECT 1 AS one"}}
    report = JOB_ENGINE.validate_job(job)
    found = [issue for issue in report.issues if issue.code == "driver-not-installed"]
    assert report.valid and [issue.severity for issue in found] == ["warning"]
    assert "no_such_driver_module is not installed (pip install a-driver)" in found[0].message
    assert found[0] in report.run_problems
    assert not JOB_ENGINE.validate_job(job, strict=True).valid


@pytest.mark.skipif("openai_ads" not in REST, reason="the openai_ads connector is not installed")
def test_the_openai_ads_guide_job_runs_unchanged(tmp_path, monkeypatch):
    responses = pytest.importorskip("responses")
    duckdb = pytest.importorskip("duckdb")

    section = components.guide("openai_ads").split("\n## DuckDBJob\n", 1)[1]
    (tmp_path / "campaigns.yaml").write_text(re.search(r"```yaml\n(.*?)```", section, re.S).group(1))
    monkeypatch.setenv("STREAMWRIGHT_SECRET_OPENAI_ADS_KEY", "test-token")
    pages = [{"data": [{"id": "c1", "name": "One", "status": "ACTIVE", "daily_budget": 5}], "has_more": True,
              "last_id": "c1"},
             {"data": [{"id": "c2", "name": "Two", "status": "PAUSED", "daily_budget": 7.5}], "has_more": False}]
    with responses.RequestsMock() as mock:
        for page in pages:
            mock.get("https://api.ads.openai.com/v1/campaigns", json=page)
        assert main(["run", str(tmp_path / "campaigns.yaml"), "--set", "account_id=a1"]) == 0
        assert mock.calls[0].request.headers["Authorization"] == "Bearer test-token"
        assert "after=c1" in mock.calls[1].request.url and "account_id=a1" in mock.calls[1].request.url
    rows = duckdb.sql("SELECT campaign_id, daily_budget FROM '%s' ORDER BY 1" % (
        tmp_path / "output" / "openai_ads_campaigns_a1.parquet")).fetchall()
    assert rows == [("c1", 5.0), ("c2", 7.5)]
