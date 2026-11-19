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

"""A step's `log:`: its rows on the streamwright.steps logger, capped, redacted, and only counted under serve."""

import logging

import pytest

from streamwright.core.jobs.duckdb.engine import run_job, validate_job

ROWS_SQL = "SELECT * FROM (VALUES (1, 'a'), (2, NULL), (3, 'tok-SECRET'), (4, 'd')) AS t(id, name) ORDER BY id"


@pytest.fixture
def logged_job():
    """A job whose step `s` (4 rows: id, name) has `log:` (updated per test), and a run that returns its log lines."""
    job = {"kind": "DuckDBJob", "name": "logged", "spec": {"secrets": {"tok": {"type": "string"}}},
           "connections": {"wh": {"provider": "duckdb", "path": ":memory:"}},
           "steps": [{"name": "s", "sql": ROWS_SQL, "log": {"message": "row {id} is {name} ({{ batch.id }})"}}],
           "outputs": [{"name": "o", "connection": "wh", "step": "s", "table": "t", "mode": "overwrite"}]}

    def run(caplog, **options):
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger="streamwright.steps"):
            result = run_job(job, secrets={"tok": "tok-SECRET"}, **options)
        lines = [(record.levelname, record.getMessage()) for record in caplog.records
                 if record.name == "streamwright.steps"]
        return result, lines

    return job, run


def test_a_steps_rows_are_logged_at_its_level_capped_with_the_rest_counted(logged_job, caplog):
    job, run = logged_job
    job["steps"][0]["log"].update(level="warning", max_rows=3)
    result, lines = run(caplog)
    assert result.exit_code == 0, result.error
    assert lines == [("WARNING", "step s: row 1 is a (logged-00000001)"),
                     ("WARNING", "step s: row 2 is null (logged-00000001)"),
                     ("WARNING", "step s: row 3 is *** (logged-00000001)"),       # a secret value is redacted
                     ("WARNING", "step s: ... and 1 more row(s)")]


def test_a_dotted_column_name_is_one_placeholder(logged_job, caplog):
    job, run = logged_job                 # e.g. an apply step's field-path columns: "campaign.name"
    job["steps"][0].update(sql='SELECT 1 AS id, \'x\' AS "campaign.name"', log={"message": "{id}: {campaign.name}"})
    result, lines = run(caplog)
    assert result.exit_code == 0, result.error
    assert lines == [("INFO", "step s: 1: x")]


def test_a_step_without_rows_logs_nothing_at_its_level(logged_job, caplog):
    job, run = logged_job
    job["steps"][0]["sql"] = ROWS_SQL.replace("ORDER BY id", "WHERE id > 9")
    result, lines = run(caplog)
    assert result.exit_code == 0, result.error
    assert lines == [("DEBUG", "step s: no rows to log")]


def test_a_run_that_may_not_log_row_values_logs_only_their_number(logged_job, caplog):
    _, run = logged_job
    result, lines = run(caplog, log_row_values=False)            # what streamwright serve's worker passes
    assert result.exit_code == 0, result.error
    assert lines == [("INFO", "step s: 4 row(s) (row values are not logged by streamwright serve)")]


def test_a_placeholder_the_steps_result_lacks_fails_the_step(logged_job, caplog):
    job, run = logged_job
    job["steps"][0]["log"]["message"] = "{id}: {nope}"
    result, lines = run(caplog)
    assert result.exit_code == 1 and lines == []
    assert "log.message: no column 'nope' in step s; columns: id, name" in result.error


@pytest.mark.parametrize(("log", "path", "expected"), [
    ({"message": "x", "level": "error"}, "steps[0].log.level", "'error' is not one of debug, info, warning"),
    ({"message": "x", "max_rows": 0}, "steps[0].log.max_rows", "must be at least 1"),
    ({"message": "x", "max_rows": 1001}, "steps[0].log.max_rows", "must be at most 1000"),
    ({"message": "x", "max_rows": "5"}, "steps[0].log.max_rows", "expected a whole number"),
    ({"message": "x", "lines": 2}, "steps[0].log.lines", "unknown key 'lines'"),
    ({"level": "info"}, "steps[0].log.message", "`message` is required"),
    ({"message": " "}, "steps[0].log.message", "a non-empty text is required"),
    ({"message": "{a b}"}, "steps[0].log.message", "{a b} is not a column placeholder"),
    ({"message": "id {id"}, "steps[0].log.message", "a lone '{'"),
    ("row {id}", "steps[0].log", "expected a mapping"),
])
def test_a_bad_log_is_a_validation_error(logged_job, log, path, expected):
    job, _ = logged_job
    job["steps"][0]["log"] = log
    found = [(issue.path, issue.message) for issue in validate_job(job).issues if issue.severity == "error"]
    assert len(found) == 1 and found[0][0] == path and expected in found[0][1], found


def test_an_output_takes_no_log(logged_job):
    job, _ = logged_job
    job["outputs"][0]["log"] = {"message": "x"}
    assert [issue.path for issue in validate_job(job).issues] == ["outputs[0].log"]
