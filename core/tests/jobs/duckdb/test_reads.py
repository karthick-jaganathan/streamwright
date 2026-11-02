"""File reads: plain unions, the shared ingest planner compiled to SQL, dynamic paths, row indexes, quoting."""

import duckdb

from streamwright.core.jobs.duckdb.engine import run_job
from streamwright.core.jobs.duckdb.reads import build_projection, read_call
from streamwright.core.jobs.ingest import Group


def _job(tmp_path, step, sql="SELECT * FROM raw"):
    return {"kind": "DuckDBJob", "name": "reads",
            "connections": {"lake": {"provider": "files", "roots": [str(tmp_path / "in")]},
                            "wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")}},
            "steps": [dict({"name": "raw", "connection": "lake"}, **step), {"name": "shaped", "sql": sql}],
            "outputs": [{"name": "out", "connection": "wh", "step": "shaped", "table": "result", "mode": "overwrite"}],
            "runtime": {"state": {"sqlite": str(tmp_path / "state.db")}}}


def _rows(tmp_path, query="SELECT * FROM result ORDER BY ALL"):
    with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as con:
        cursor = con.execute(query)
        return [d[0] for d in cursor.description], cursor.fetchall()


def _write(tmp_path, name, text):
    path = tmp_path / "in" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def test_build_projection_is_pure_and_quotes_everything():
    group = Group("p", ['Amount "usd"', "m_1"], [('Amount "usd"', "amount", "decimal(12,2)")], ["/a/x.csv"],
                  column_groups={"metrics": {"type": "long", "keep_nulls": False, "entries": [("m_1", "1")]}},
                  path_vars_spec=("/a/([^/]+)/", {"part": 1}))
    assert build_projection(group, ("row_no", 1)) == [
        'TRY_CAST("Amount ""usd""" AS DECIMAL(12,2)) AS "amount"',
        "map_from_entries(list_filter([{'key': '1', 'value': TRY_CAST(\"m_1\" AS BIGINT)}], "
        'lambda e: e.value IS NOT NULL)) AS "metrics"',
        'regexp_extract("filename", \'/a/([^/]+)/\', 1) AS "part"', '"filename"',
        'row_number() OVER (PARTITION BY "filename" ORDER BY "ordinality") - 1 + 1 AS "row_no"']


def test_plain_reads_union_files_by_name_with_a_per_file_row_index(tmp_path):
    _write(tmp_path, "a.csv", "id,amount\n1,10\n2,20\n")
    _write(tmp_path, "b.csv", "amount,id,extra\n30,3,x\n")
    result = run_job(_job(tmp_path, {"read": "csv", "path": "*.csv", "row_index": "row_id"},
                          "SELECT id, amount, extra, row_id FROM raw"))
    assert result.exit_code == 0, result.error
    assert _rows(tmp_path)[1] == [(1, 10, None, 0), (2, 20, None, 1), (3, 30, "x", 0)]


def test_row_index_false_adds_no_column(tmp_path):
    _write(tmp_path, "a.csv", "id\n1\n")
    assert run_job(_job(tmp_path, {"read": "csv", "path": "a.csv", "row_index": False})).exit_code == 0
    assert "_row_index" not in _rows(tmp_path)[0]


def test_mapped_reads_rename_type_fold_groups_extract_path_variables_and_reject(tmp_path):
    _write(tmp_path, "acme/x.csv", "Order ID,Amt,m_clicks,m_views\n7,1.5,3,\n")
    _write(tmp_path, "zed/y.csv", "Something,Else\n1,2\n")
    step = {"read": "csv", "path": "*/*.csv", "path_pattern": r"/in/(?P<partner>[^/]+)/",
            "columns": [{"header": "Order ID", "canonical": "order_id", "type": "bigint", "required": True},
                        {"header": "Amt", "canonical": "amount", "type": "double"}],
            "column_groups": [{"name": "metrics", "glob": "m_*", "type": "long", "strip_prefix": "m_"}]}
    result = run_job(_job(tmp_path, step, "SELECT order_id, amount, metrics, partner FROM raw"))
    assert result.exit_code == 0, result.error
    assert _rows(tmp_path)[1] == [(7, 1.5, {"clicks": 3}, "acme")]


def test_a_dynamic_path_reads_the_files_an_earlier_step_lists_and_quotes_them(tmp_path):
    path = _write(tmp_path, "it's.csv", "id\n1\n")
    job = _job(tmp_path, {"read": "csv", "path": "{{ arrivals.uri }}"})
    job["steps"].insert(0, {"name": "arrivals", "sql": "SELECT %s AS uri UNION ALL SELECT NULL" %
                            ("'%s'" % str(path).replace("'", "''"))})
    assert run_job(job).exit_code == 0
    assert _rows(tmp_path)[1] == [(1,)]


def test_an_empty_dynamic_path_keeps_the_declared_shape(tmp_path):
    job = _job(tmp_path, {"read": "csv", "path": "{{ arrivals.uri }}", "row_index": "n",
                          "columns": [{"header": "Id", "canonical": "id", "type": "int"}]},
               "SELECT id, n, filename FROM raw")
    job["steps"].insert(0, {"name": "arrivals", "sql": "SELECT 'x' AS uri WHERE false"})
    assert run_job(job).exit_code == 0
    assert _rows(tmp_path) == (["id", "n", "filename"], [])


def test_a_path_outside_the_roots_fails_the_batch(tmp_path):
    (tmp_path / "in").mkdir()
    result = run_job(_job(tmp_path, {"read": "csv", "path": "../secret.csv"}))
    assert result.exit_code == 1 and "may not contain '..'" in result.error


def test_shared_option_names_are_translated_for_duckdb_not_left_to_its_aliases():
    # DuckDB 1.5 has no `delimiter` alias (2.0 does): every reader translates the shared vocabulary itself
    assert read_call("csv", ["a.csv"], {"delimiter": ";", "header": True}) == \
        "read_csv(['a.csv'], delim=';', header=true) WITH ORDINALITY"
    assert "delim='\\t'" not in read_call("tsv", ["a.tsv"], {"delimiter": "|"})
