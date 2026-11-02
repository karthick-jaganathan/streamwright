"""The files DuckDB adapter (kind: DuckDBJob): reads and writes inside the roots, relative to the job's folder."""

import datetime
import os
from types import MappingProxyType

import pytest

from streamwright.connectors.files import CONNECTOR
from streamwright.core.jobs.duckdb.adapter import AdapterError
from streamwright.core.jobs.duckdb.definition_adapter import DefinitionDuckDBAdapter
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.engines.duckdb import DuckDBWriteAdapter, ReadContext
from streamwright.core.sdk.testing import DuckDBAdapterContract, DuckDBRules, Session, check_job, run_job

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))
DATA = os.path.join(REPO_ROOT, "examples", "sources", "readers", "files_demo", "data")


class TestDuckDBAdapter(DuckDBAdapterContract):
    __test__ = True
    definition = CONNECTOR
    connection = MappingProxyType({"roots": [DATA]})
    read_arguments = MappingProxyType({"path": "orders/east.csv", "format": "csv", "options": {"header": True}})
    expected_records = (
        {"order_id": 1001, "product_id": 1, "quantity": 2, "amount": 39.98, "ordered_on": datetime.date(2026, 10, 1)},
        {"order_id": 1002, "product_id": 3, "quantity": 1, "amount": 149.0, "ordered_on": datetime.date(2026, 10, 1)},
        {"order_id": 1003, "product_id": 2, "quantity": 5, "amount": 12.5, "ordered_on": datetime.date(2026, 10, 2)})


def test_the_adapter_reads_and_writes_without_an_extension_or_a_secret():
    assert isinstance(CONNECTOR.load_adapter("duckdb"), DuckDBWriteAdapter)
    session = Session()
    try:
        CONNECTOR.load_adapter("duckdb").configure(session, "lake", resolve_values(CONNECTOR, {"roots": [DATA]}))
        assert session.con.execute("SELECT count(*) FROM duckdb_secrets()").fetchone() == (0,)
    finally:
        session.close()


def test_paths_stay_inside_the_roots_which_start_at_the_jobs_folder(tmp_path):
    connection = resolve_values(CONNECTOR, {"roots": ["data", "/srv/shared"]})
    operation = CONNECTOR.operation("object", "read")
    session = Session(base_dir=str(tmp_path))
    context = ReadContext(session, "lake", connection, {"name": "s"}, batch=None, pinned={}, limits={}, values=None)
    try:
        adapter = CONNECTOR.load_adapter("duckdb")
        sql = adapter.read_sql(context, operation, {"path": "a.csv", "format": "csv"})
        assert "'%s'" % (tmp_path / "data" / "a.csv") in sql
        assert "'/srv/shared/b.csv'" in adapter.read_sql(context, operation, {"path": "/srv/shared/b.csv",
                                                                           "format": "csv"})
        for outside in ("../a.csv", "/etc/hosts"):
            with pytest.raises(AdapterError):
                adapter.read_sql(context, operation, {"path": outside, "format": "csv"})
    finally:
        session.close()


def job(tmp_path, **output):
    return {"kind": "DuckDBJob", "name": "files_job",
            "connections": {"in": {"provider": "files", "roots": ["data"]},
                            "out": {"provider": "files", "roots": ["lake"]}},
            "steps": [{"name": "orders", "connection": "in", "read": "csv", "path": "orders.csv",
                       "options": {"header": True, "delimiter": ";"}}],
            "outputs": [dict({"name": "o", "connection": "out", "step": "orders", "path": "export/orders.parquet",
                              "format": "parquet", "mode": "overwrite"}, **output)],
            "runtime": {"state": {"sqlite": str(tmp_path / "state.db")}}}


def test_a_job_reads_with_the_shared_option_vocabulary_and_writes_creating_folders(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "orders.csv").write_text("id;country\n1;US\n2;CA\n3;US\n")
    result = run_job(job(tmp_path, split_by=["country"], file_name="orders_{country}.csv", format="csv",
                         path="export"), base_dir=str(tmp_path))
    assert result.exit_code == 0, result.error
    assert sorted(os.listdir(tmp_path / "lake" / "export")) == ["orders_CA.csv", "orders_US.csv"]


def test_a_job_with_a_duckdb_native_option_forwards_it_instead_of_refusing(tmp_path):
    good = job(tmp_path)
    good["steps"][0]["options"] = {"delim": ";"}  # DuckDB's own read_csv parameter name - a vendor option now
    rules = DuckDBRules({"files": DefinitionDuckDBAdapter(CONNECTOR)}, connectors=())
    assert check_job(good, rules) == []
    assert check_job(job(tmp_path), rules) == []
    assert [(i.code, i.path) for i in check_job(job(tmp_path, mode="append"), rules)] == [
        ("unsupported-mode", "outputs[0]")]
