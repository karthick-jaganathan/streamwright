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

"""runtime.state.scope and runtime.state.lock: validation, the lock plan and its admission of items."""

import pytest

from streamwright.core.config.inputs import InputError
from streamwright.core.jobs.duckdb.engine import validate_job
from streamwright.core.jobs.locking import LockPlan, lock_plan, parse_lock_timeout
from streamwright.core.jobs.types import Lease


def _job(lock, scope=None):
    state = {"sqlite": "state.db", "lock": lock}
    if scope is not None:
        state["scope"] = scope
    return {"kind": "DuckDBJob", "name": "j",
            "spec": {"config": {"accounts": {"type": "list", "items": "string", "default": ["1", "2"]},
                                "client": {"type": "string", "default": "a"}}},
            "connections": {"wh": {"provider": "duckdb", "path": ":memory:"}},
            "steps": [{"name": "s", "sql": "SELECT 1 AS x"}],
            "outputs": [{"name": "o", "connection": "wh", "step": "s", "table": "t", "mode": "overwrite"}],
            "runtime": {"state": state}}


@pytest.mark.parametrize(("lock", "message"), [
    ({"each": [1, 2], "token": "a:{{ item }}"}, "must be exactly {{ config.NAME }}"),
    ({"each": "{{ config.accounts }}", "token": "account"}, "must contain {{ item }}"),
    ({"each": "{{ config.accounts }}", "as": "acct", "token": "a:{{ item }}"}, "must contain {{ acct }}"),
    ({"each": "{{ config.accounts }}", "token": "scope:{{ item }}"}, "cannot start with 'scope:'"),
    ({"token": "report", "policy": "prune"}, "policy prune needs each"),
    ({"token": "report", "timeout": "soon"}, "invalid duration"),
])
def test_bad_locks_are_rejected(lock, message):
    report = validate_job(_job(lock))
    assert not report.valid and any(message in issue.message for issue in report.issues), report.issues


def test_the_old_run_key_is_reported_as_renamed():
    job = _job({"token": "t"})
    job["runtime"]["state"]["run_key"] = "daily"
    messages = [issue.message for issue in validate_job(job).issues]
    assert any("run_key was renamed job_key" in text for text in messages), messages


def test_a_scope_is_text_of_a_bounded_length():
    assert validate_job(_job({"token": "t"}, scope="client:{{ config.client }}")).valid
    assert not validate_job(_job({"token": "t"}, scope="x" * 200)).valid


def test_the_plan_narrows_the_lock_input_to_the_admitted_items():
    raw = _job({"each": "{{ config.accounts }}", "as": "acct", "token": "account:{{ acct }}"},
               scope="client:{{ config.client }}")
    resolved = {"runtime": {"state": {"scope": "client:a", "lock": dict(raw["runtime"]["state"]["lock"],
                                                                         each=["1", "2"])}}}
    plan = lock_plan(raw, resolved, {"accounts": ["1", "2"], "client": "a"}, policy="prune")
    assert (plan.tokens, plan.item_tokens) == (("scope:client:a",), ("account:1", "account:2"))
    lease = Lease("j", "o", 1, "", tokens=("scope:client:a", "account:2"), pruned_tokens=("account:1",))
    assert plan.inputs(lease) == {"scope": "client:a", "config": {"accounts": ["2"], "client": "a"},
                                  "tokens": ["account:2"]}
    assert plan.inputs(Lease("j", "o", 1, "", tokens=("scope:client:a",))) is None
    text = LockPlan("", "prune", 0.0, None, "accounts", ("1", "2"), ("a:1", "a:2"), {"accounts": "1,2"})
    assert text.inputs(Lease("j", "o", 1, "", tokens=("", "a:1")))["config"] == {"accounts": "1"}


def test_lock_tokens_fit_the_state_tables_key_columns():
    raw = _job({"token": "t" * 192})
    with pytest.raises(InputError, match=r"the lock token 't+'\.\.\. is 192 characters: at most 191"):
        lock_plan(raw, raw, {})
    raw = _job({"each": "{{ config.accounts }}", "token": "account:{{ item }}"})
    resolved = {"runtime": {"state": {"lock": dict(raw["runtime"]["state"]["lock"], each=["1", "9" * 190])}}}
    with pytest.raises(InputError, match="is 198 characters"):
        lock_plan(raw, resolved, {"accounts": ["1", "9" * 190]})


def test_cli_overrides_are_checked():
    raw = _job({"token": "report"})
    with pytest.raises(InputError, match=r"persistent runtime\.state"):
        lock_plan(raw, raw, {}, policy="wait", persistent=False)
    with pytest.raises(InputError, match="needs lock policy wait"):
        lock_plan(raw, raw, {}, timeout="30s")
    assert lock_plan(raw, raw, {}, policy="wait", timeout="5m").timeout == 300
    assert (parse_lock_timeout("30"), parse_lock_timeout(2.5), parse_lock_timeout(None)) == (30.0, 2.5, 0.0)
