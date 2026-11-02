"""Table and file writes of the built-in adapters."""

import json
import os

import duckdb
import pytest
import yaml

from streamwright.core.jobs.duckdb.cli import main
from streamwright.core.jobs.duckdb.engine import JobInvalidError, run_job

ROWS = "SELECT * FROM (VALUES (1, 'US', 'a'), (2, 'CA', 'b'), (3, 'US', 'c')) t(id, country, v)"


def _job(tmp_path, output, sql=ROWS):
    return {"kind": "DuckDBJob", "name": "writes",
            "connections": {"wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")},
                            "lake": {"provider": "files", "roots": [str(tmp_path / "lake")]}},
            "steps": [{"name": "data", "sql": sql}],
            "outputs": [dict({"name": "out", "step": "data"}, **output)],
            "runtime": {"state": {"sqlite": str(tmp_path / "state.db")}}}


def _query(tmp_path, sql):
    with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as con:
        return con.execute(sql).fetchall()


def _run(job):
    result = run_job(job)
    assert result.exit_code == 0, result.error
    return result


def test_table_merge_upserts_on_the_primary_key_every_run(tmp_path):
    output = {"connection": "wh", "table": "sales.facts", "mode": "merge", "primary_key": ["id"]}
    with duckdb.connect(str(tmp_path / "wh.duckdb")) as con:
        con.execute("CREATE SCHEMA sales")
    _run(_job(tmp_path, output))
    _run(_job(tmp_path, output, "SELECT 1 AS id, 'GB' AS country, 'z' AS v"))
    assert _query(tmp_path, "SELECT * FROM sales.facts ORDER BY id") == [(1, "GB", "z"), (2, "CA", "b"), (3, "US", "c")]


def test_table_append_adds_by_name_and_overwrite_rewrites(tmp_path):
    _run(_job(tmp_path, {"connection": "wh", "table": "t", "mode": "append"}))
    _run(_job(tmp_path, {"connection": "wh", "table": "t", "mode": "append"},
              "SELECT 'x' AS v, 'FR' AS country, 9 AS id"))
    assert _query(tmp_path, "SELECT count(*), max(id) FROM t") == [(4, 9)]
    _run(_job(tmp_path, {"connection": "wh", "table": "t", "mode": "overwrite"}, "SELECT 5 AS id"))
    assert _query(tmp_path, "SELECT * FROM t") == [(5,)]


def test_split_by_writes_one_named_file_per_split_and_keeps_the_column(tmp_path):
    output = {"connection": "lake", "path": "export", "format": "csv", "mode": "overwrite", "split_by": ["country"],
              "file_name": "sales_{country}_{seq}.csv"}
    _run(_job(tmp_path, output))
    folder = tmp_path / "lake" / "export"
    assert sorted(os.listdir(folder)) == ["sales_CA_00001.csv", "sales_US_00002.csv"]
    assert (folder / "sales_US_00002.csv").read_text().splitlines() == ["id,country,v", "1,US,a", "3,US,c"]


def test_the_run_reports_and_logs_the_files_it_wrote(tmp_path, caplog, capsys):
    output = {"connection": "lake", "path": "export", "format": "csv", "mode": "overwrite", "split_by": ["country"],
              "file_name": "sales_{country}_{seq}.csv"}
    folder = tmp_path / "lake" / "export"
    files = [str(folder / "sales_CA_00001.csv"), str(folder / "sales_US_00002.csv")]
    with caplog.at_level("INFO", logger="streamwright.duckdb"):
        result = _run(_job(tmp_path, output))
    assert result.locations == {"out": files}
    assert [record.path for record in caplog.records if getattr(record, "event", None) == "output_file"] == files
    assert "output out: wrote %s" % files[0] in caplog.text

    job = tmp_path / "job.yaml"
    job.write_text(yaml.safe_dump(_job(tmp_path, {"connection": "lake", "path": "one.csv", "format": "csv",
                                                  "mode": "overwrite"})))
    assert main(["run", str(job)]) == 0
    assert "3 rows -> %s" % (tmp_path / "lake" / "one.csv") in capsys.readouterr().out


def test_the_run_summary_is_written_whatever_the_outcome(tmp_path, capsys):
    job = tmp_path / "job.yaml"
    output = {"connection": "lake", "path": "one.csv", "format": "csv", "mode": "error"}
    job.write_text(yaml.safe_dump(_job(tmp_path, output)))
    summary = tmp_path / "summary.json"
    assert main(["run", str(job), "--summary", str(summary)]) == 0
    written = json.loads(summary.read_text())
    assert written["status"] == "ok" and written["job"] == "writes" and len(written["batches"]) == 1
    assert written["outputs"] == [{"name": "out", "rows": 3, "files": [str(tmp_path / "lake" / "one.csv")],
                                   "already_committed": False}]
    assert main(["run", str(job), "--summary", str(summary)]) == 1  # mode error: the file exists already
    failed = json.loads(summary.read_text())
    assert failed["status"] == "failed" and "output exists already" in failed["error"]
    assert main(["run", str(job), "--summary", str(tmp_path / "missing" / "s.json")]) == 2
    assert "the folder" in capsys.readouterr().err


def test_a_bare_summary_is_logged(tmp_path, capsys, caplog):
    job = tmp_path / "job.yaml"
    job.write_text(yaml.safe_dump(_job(tmp_path, {"connection": "lake", "path": "one.csv", "format": "csv",
                                                  "mode": "overwrite"})))
    with caplog.at_level("INFO", logger="streamwright.duckdb"):
        assert main(["run", str(job), "--summary"]) == 0
    line = next(record for record in caplog.records if getattr(record, "event", None) == "run_summary")
    summary = json.loads(line.getMessage().partition("run summary: ")[2])  # one line of compact JSON
    assert summary["status"] == "ok" and summary["outputs"][0]["rows"] == 3
    assert capsys.readouterr().out.startswith("Job: writes\n")  # stdout keeps the outputs' lines


def test_file_name_without_split_by_names_the_one_file_in_its_folder(tmp_path):
    output = {"connection": "lake", "path": "export", "format": "csv", "mode": "overwrite",
              "layout": "{today:%Y/%m}", "file_name": "sales_{today:%Y%m%d}_{batch_id}.csv"}
    _run(_job(tmp_path, output))
    files = [os.path.relpath(os.path.join(root, name), tmp_path / "lake" / "export")
             for root, _, names in os.walk(tmp_path / "lake" / "export") for name in names]
    assert len(files) == 1 and files[0].startswith("20") and "/sales_20" in files[0]
    assert (tmp_path / "lake" / "export" / files[0]).read_text().splitlines()[0] == "id,country,v"


def test_tsv_and_jsonl_are_written_as_the_catalog_formats_say(tmp_path):
    _run(_job(tmp_path, {"connection": "lake", "path": "one.tsv", "format": "tsv", "mode": "overwrite"}))
    assert (tmp_path / "lake" / "one.tsv").read_text().splitlines()[:2] == ["id\tcountry\tv", "1\tUS\ta"]
    _run(_job(tmp_path, {"connection": "lake", "path": "one.jsonl", "format": "jsonl", "mode": "overwrite"}))
    assert (tmp_path / "lake" / "one.jsonl").read_text().splitlines()[0] == '{"id":1,"country":"US","v":"a"}'


def test_mode_error_refuses_to_overwrite(tmp_path):
    output = {"connection": "lake", "path": "one.parquet", "format": "parquet", "mode": "error"}
    _run(_job(tmp_path, output))
    result = run_job(_job(tmp_path, output))
    assert result.exit_code == 1 and "output exists already" in result.error


def test_partition_by_writes_hive_folders(tmp_path):
    _run(_job(tmp_path, {"connection": "lake", "path": "hive", "format": "parquet", "mode": "overwrite",
                         "partition_by": ["country"]}))
    assert sorted(os.listdir(tmp_path / "lake" / "hive")) == ["country=CA", "country=US"]


@pytest.mark.parametrize("output", [
    {"connection": "lake", "path": "x.csv", "format": "csv", "mode": "append"},
    {"connection": "wh", "table": "t", "mode": "merge"},
    {"connection": "wh", "table": "t", "mode": "overwrite", "split_by": ["id"]},
    {"connection": "lake", "path": "x", "format": "csv", "mode": "overwrite", "file_name": "x_{country}.csv"},
    {"connection": "lake", "path": "x", "format": "csv", "mode": "overwrite", "layout": "{today}"},
])
def test_outputs_a_writer_cannot_honour_are_rejected_before_running(tmp_path, output):
    with pytest.raises(JobInvalidError):
        run_job(_job(tmp_path, output))
