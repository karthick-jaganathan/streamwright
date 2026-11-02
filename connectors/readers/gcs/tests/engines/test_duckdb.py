"""The gcs DuckDB adapter (kind: DuckDBJob): per-root scoped secrets, roots, declared checks."""

from types import MappingProxyType

import pytest

from streamwright.connectors.gcs import CONNECTOR
from streamwright.core.io.files import AccessError
from streamwright.core.jobs.duckdb.definition_adapter import DefinitionDuckDBAdapter
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.engines.duckdb import ReadContext
from streamwright.core.sdk.testing import DuckDBAdapterContract, DuckDBRules, Session, check_job, extension_installed

SECRET = "hmac-SECRET-0042"
CONNECTION = MappingProxyType({"roots": ["gs://bucket-a/in/", "gs://bucket-b/"], "key_id": "GOOGHMAC",
                               "secret": SECRET})
HTTPFS = pytest.mark.skipif(not extension_installed("httpfs"), reason="DuckDB's httpfs extension is not installed")


@HTTPFS
class TestDuckDBAdapter(DuckDBAdapterContract):
    __test__ = True
    definition = CONNECTOR
    connection = CONNECTION
    read_arguments = MappingProxyType({"path": "day=1/*.parquet", "format": "parquet"})


@HTTPFS
def test_each_root_gets_its_own_scoped_secret_and_no_value_shows():
    session = Session()
    try:
        CONNECTOR.load_adapter("duckdb").configure(session, "lake", resolve_values(CONNECTOR, CONNECTION))
        con = session.con
        assert con.execute("SELECT name FROM which_secret('gs://bucket-b/x.parquet', 'gcs')").fetchall() == [
            ("streamwright_lake_1",)]
        assert con.execute("SELECT name FROM which_secret('gs://bucket-c/x', 'gcs')").fetchall() == []
        assert "SECRET-0042" not in str(con.execute("SELECT * FROM duckdb_secrets()").fetchall())
    finally:
        session.close()


def test_read_paths_stay_inside_the_roots():
    adapter, connection = CONNECTOR.load_adapter("duckdb"), resolve_values(CONNECTOR, CONNECTION)
    operation = CONNECTOR.operation("object", "read")
    context = ReadContext(None, "lake", connection, {"name": "s"}, batch=None, pinned={}, limits={}, values=None)
    sql = adapter.read_sql(context, operation, {"path": "day=1/x.csv", "format": "csv"})
    assert "'gs://bucket-a/in/day=1/x.csv'" in sql and SECRET not in sql
    for outside in ("gs://bucket-c/z.csv", "../z.csv", "z.csv?s3_endpoint=evil", "gcs://bucket-a/in/z.csv"):
        with pytest.raises(AccessError):
            adapter.read_sql(context, operation, {"path": outside, "format": "csv"})


def job(connection, **output):
    return {"kind": "DuckDBJob", "name": "j", "spec": {"secrets": {"key": {"type": "string"}}},
            "connections": {"lake": connection},
            "steps": [{"name": "s", "connection": "lake", "read": "parquet", "path": "x.parquet"}],
            "outputs": [dict({"name": "o", "connection": "lake", "step": "s", "path": "out.parquet",
                              "format": "parquet", "mode": "overwrite"}, **output)]}


def test_jobs_are_checked_from_the_spec():
    rules = DuckDBRules({"gcs": DefinitionDuckDBAdapter(CONNECTOR)}, connectors=())
    declared = dict(CONNECTION, provider="gcs", key_id="{{ secrets.key }}", secret="{{ secrets.key }}")
    assert check_job(job(declared), rules) == []
    assert [(i.code, i.path) for i in check_job(job(dict(declared, secret=SECRET)), rules)] == [
        ("literal-secret", "connections.lake.secret")]
    assert [(i.code, i.path) for i in check_job(job(declared, mode="append"), rules)] == [
        ("unsupported-mode", "outputs[0]")]
