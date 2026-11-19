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
"""Input payloads (spec.input, --input): typed rows as the batch's `input` table; input constraints (plan §3, §5.5)."""

import io
import json
import sqlite3
from types import MappingProxyType

import duckdb
import pytest

from streamwright.core.config.inputs import InputError
from streamwright.core.jobs.duckdb.cli import main
from streamwright.core.jobs.duckdb.engine import JobInvalidError, run_job
from streamwright.core.jobs.duckdb.plugin import JOB_ENGINE
from streamwright.core.jobs.payload import PayloadError, read_payload

COLUMNS = MappingProxyType({"gclid": {"type": "string", "required": True, "max_length": 20},
                            "value": {"type": "number", "minimum": 0},
                            "currency": {"type": "string", "enum": ["USD", "EUR"]},
                            "day": {"type": "date"}})


def _job(tmp_path, **declared):
    return {
        "kind": "DuckDBJob", "name": "conversions",
        "spec": {"input": {"columns": dict(COLUMNS), **declared}},
        "connections": {"wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")}},
        "steps": [{"name": "valid", "sql": "SELECT gclid, value, currency, day FROM input WHERE value > 0"}],
        "outputs": [{"name": "out", "connection": "wh", "step": "valid", "table": "conversions", "mode": "append"}],
        "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}},
    }


ROWS = ({"gclid": "g1", "value": 350, "currency": "USD", "day": "2026-11-15"},
        {"gclid": "g2", "value": 0, "currency": "EUR"},
        {"gclid": "g3", "value": 12.5})


def _write(tmp_path, rows, name="payload.json"):
    path = tmp_path / name
    path.write_text(rows if isinstance(rows, str) else json.dumps(rows))
    return str(path)


def test_a_payload_is_the_typed_input_table_and_leaves_only_its_fingerprint_in_the_state(tmp_path):
    result = run_job(_job(tmp_path), input_source=_write(tmp_path, ROWS))
    assert result.exit_code == 0, result.error
    with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as con:
        assert con.execute("SELECT gclid, value, currency, day::VARCHAR FROM conversions ORDER BY 1").fetchall() == [
            ("g1", 350.0, "USD", "2026-11-15"), ("g3", 12.5, None, None)]
        assert dict(con.execute("DESCRIBE conversions").fetchall()[i][:2] for i in range(4)) == {
            "gclid": "VARCHAR", "value": "DOUBLE", "currency": "VARCHAR", "day": "DATE"}
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        pinned = json.loads(con.execute("SELECT checkpoint FROM sw_jobs").fetchone()[0])["input"]
    assert set(pinned) == {"sha256", "rows"} and pinned["rows"] == 3              # the rows were purged


def test_ndjson_and_stdin_payloads_are_read_alike(tmp_path):
    ndjson = "\n".join(json.dumps(row) for row in ROWS) + "\n"
    from_file = read_payload(_write(tmp_path, ndjson, "rows.ndjson"), _job(tmp_path))
    from_stdin = read_payload("-", _job(tmp_path), stdin=io.BytesIO(ndjson.encode()))
    assert from_file == from_stdin and len(from_file.rows) == 3


@pytest.mark.parametrize(("text", "message"), [
    ('[{"gclid": "g1", "extra": 1}]', "unknown column(s) extra"),
    ('[{"value": 1}]', "missing required column 'gclid'"),
    ('[{"gclid": "g1", "currency": "GBP"}]', "is not one of: USD, EUR"),
    ('[{"gclid": "g1", "value": -1}]', "is less than 0"),
    ('[{"gclid": "' + "x" * 21 + '"}]', "is longer than 20 characters"),
    ('[{"gclid": "g1", "gclid": "g2"}]', "repeats the key 'gclid'"),
    ('{"gclid": "g1"', "the payload is not JSON"),
    ('[1, 2]', "a JSON array of objects"),
    ('[{"gclid": ' + '[' * 40 + ']' * 40 + '}]', "deeper than 32 levels"),
])
def test_a_payload_that_does_not_fit_is_refused_before_a_batch_exists(tmp_path, text, message):
    with pytest.raises(PayloadError, match=message.replace("(", r"\(").replace(")", r"\)")):
        read_payload(_write(tmp_path, text), _job(tmp_path))


def test_payload_limits_and_the_unknown_column_policy(tmp_path):
    rows = [{"gclid": "g%d" % n, "note": "x"} for n in range(3)]
    with pytest.raises(PayloadError, match=r"more than spec\.input\.max_rows"):
        read_payload(_write(tmp_path, rows), _job(tmp_path, max_rows=2, unknown_columns="drop"))
    with pytest.raises(PayloadError, match=r"larger than spec\.input\.max_bytes"):
        read_payload(_write(tmp_path, rows), _job(tmp_path, max_bytes=10))
    assert [row["gclid"] for row in read_payload(_write(tmp_path, rows),
                                                _job(tmp_path, unknown_columns="drop")).rows] == ["g0", "g1", "g2"]


def test_retain_none_never_stores_the_rows(tmp_path):
    assert run_job(_job(tmp_path, retain="none"), input_source=_write(tmp_path, ROWS)).exit_code == 0
    with sqlite3.connect(str(tmp_path / "state.db")) as con:
        assert "records" not in json.loads(con.execute("SELECT checkpoint FROM sw_jobs").fetchone()[0])["input"]


def test_a_payload_job_needs_its_input_and_other_jobs_take_none(tmp_path):
    with pytest.raises(InputError, match="takes an input payload"):
        run_job(_job(tmp_path))
    plain = _job(tmp_path)
    del plain["spec"]["input"]
    plain["steps"][0]["sql"] = "SELECT 1 AS one"
    with pytest.raises(InputError, match=r"declares no spec\.input"):
        run_job(plain, input_source=_write(tmp_path, ROWS))


def test_validate_checks_the_declaration_and_a_given_payload(tmp_path, capsys):
    bad = _job(tmp_path)
    bad["spec"]["input"]["columns"] = {"gclid": {"type": "string", "pattern": "("}}
    assert ("bad-input", "spec.input.columns.gclid.pattern") in {
        (issue.code, issue.path) for issue in JOB_ENGINE.validate_job(bad).issues}
    path = tmp_path / "job.yaml"
    path.write_text(json.dumps(_job(tmp_path)))
    assert main(["validate", str(path), "--input", _write(tmp_path, ROWS)]) == 0
    assert "the payload fits spec.input (3 rows)" in capsys.readouterr().out
    assert main(["validate", str(path), "--input", _write(tmp_path, [{"gclid": 1, "currency": "GBP"}])]) == 1
    assert "bad-payload" in capsys.readouterr().err


def test_config_inputs_take_the_same_constraints(tmp_path):
    job = _job(tmp_path)
    del job["spec"]["input"]
    job["spec"]["config"] = {"client_id": {"type": "string", "pattern": "^[0-9]{1,12}$"},
                             "region": {"type": "string", "enum": ["eu", "us"], "default": "eu"}}
    job["steps"][0]["sql"] = "SELECT '{{ config.client_id }}' AS gclid, 1.0 AS value, 'USD' AS currency, NULL AS day"
    with pytest.raises(JobInvalidError, match="client_id' does not match"):
        run_job(job, config={"client_id": "1001' OR 1=1 --"})
    with pytest.raises(JobInvalidError, match="region' is not one of: eu, us"):
        run_job(job, config={"client_id": "1001", "region": "mars"})
    assert run_job(job, config={"client_id": "1001"}).exit_code == 0
