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

"""`streamwright state ACTION --url URL`: the batch actions that change a batch, and the URL's database."""

import pytest

from streamwright.core.cli import main
from streamwright.core.jobs.state import open_sqlite


@pytest.fixture
def state(tmp_path):
    """A SQLite state database with batches: failed jobs a-00000001 and a-00000002, and b-00000001 (job id shared by
    job b's a-00000001 too) holding two in-flight rows of output push."""
    path = str(tmp_path / "state.db")
    store = open_sqlite(path)
    store.initialize()
    for job, batch_id in (("a", "a-00000001"), ("a", "a-00000002"), ("b", "a-00000001")):
        lease = store.acquire_lease(job, "w", 300)
        store.record_batch(lease, batch_id, "processing", {"pinned": {}},
                           batch_sequence=store.next_batch_sequence(lease, ""))
        if job == "b":
            store.commit_batch(lease, batch_id, {}, {"push": ("dest", [{"n": 1}, {"n": 2}])})
            store.mark_events_in_flight(lease, batch_id, "push", [0, 1])
        else:
            store.record_batch(lease, batch_id, "failed", {"pinned": {}}, error="boom")
        store.release_lease(lease)
    return path


def _state(path, *args):
    return main(["state", args[0], "--url", "sqlite:///%s" % path, *args[1:]])


def test_retry_and_abandon_change_one_failed_batch(state, capsys):
    assert _state(state, "retry", "a-00000002") == 0
    assert "a-00000002 resumes from processing at the job's next run" in capsys.readouterr().out
    assert _state(state, "abandon", "a-00000001", "--job", "a") == 0
    assert "a-00000001 abandoned" in capsys.readouterr().out
    store = open_sqlite(state)
    assert [store.get_batch("a", batch_id)["status"] for batch_id in ("a-00000001", "a-00000002")] == [
        "abandoned", "processing"]


def test_a_job_id_in_two_jobs_needs_the_job(state, capsys):
    assert _state(state, "abandon", "a-00000001") == 2
    assert "job id a-00000001 is in several jobs (a, b): give --job" in capsys.readouterr().err
    assert open_sqlite(state).get_batch("a", "a-00000001")["status"] == "failed"


@pytest.mark.parametrize(("action", "status", "said"), [
    ("resend", "pending", "2 held row(s) will be sent again at the job's next run"),
    ("skip", "failed", "2 held row(s) marked failed (UNKNOWN_OUTCOME)")])
def test_resend_and_skip_settle_an_output_s_held_rows(state, capsys, action, status, said):
    assert _state(state, action, "a-00000001", "--job", "b", "--output", "push") == 0
    assert said in capsys.readouterr().out
    assert [row["status"] for row in open_sqlite(state).list_events("b", "a-00000001")] == [status, status]


@pytest.mark.parametrize("scheme", ["sqlite", "duckdb"])
@pytest.mark.parametrize("action", [["list"], ["show", "a-1"], ["check"], ["migrations"], ["migrate", "--dry-run"]])
def test_only_migrate_creates_a_missing_database_file(tmp_path, capsys, scheme, action):
    path = tmp_path / "typo.db"
    assert main(["state", action[0], "--url", "%s:///%s" % (scheme, path), *action[1:]]) == 2
    assert "no state database at %s (`streamwright state migrate` creates it)" % path in capsys.readouterr().err
    assert not path.exists()
    assert main(["state", "migrate", "--url", "%s:///%s" % (scheme, path)]) == 0
    assert path.exists()


def test_a_url_password_is_never_written(capsys):
    url = "postgresql://user:hunter2-SECRET@127.0.0.1:1/db?connect_timeout=1"
    assert main(["state", "list", "--url", url]) != 0
    captured = capsys.readouterr()
    assert "hunter2-SECRET" not in captured.out + captured.err
