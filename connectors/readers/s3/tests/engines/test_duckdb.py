"""The s3 DuckDB adapter (kind: DuckDBJob): per-root scoped secrets, roots, declared-connection checks."""

from types import MappingProxyType

import pytest

from streamwright.connectors.s3 import CONNECTOR
from streamwright.core.io.files import AccessError
from streamwright.core.jobs.duckdb.definition_adapter import DefinitionDuckDBAdapter
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.engines.duckdb import ReadContext
from streamwright.core.sdk.testing import DuckDBAdapterContract, DuckDBRules, Session, check_job, extension_installed

SECRET = "tok'en-SECRET-123"
CONNECTION = MappingProxyType({
    "roots": ["s3://bucket-a/in/", "s3://bucket-b/"], "key_id": "AKIAEXAMPLE", "secret": SECRET,
    "region": "eu-west-1",
})
HTTPFS = pytest.mark.skipif(not extension_installed("httpfs"), reason="DuckDB's httpfs extension is not installed")


@HTTPFS
class TestDuckDBAdapter(DuckDBAdapterContract):
    __test__ = True
    definition = CONNECTOR
    connection = CONNECTION
    read_arguments = MappingProxyType({"path": "day=1/*.parquet", "format": "parquet"})


def configure(session, name, connection):
    CONNECTOR.load_adapter("duckdb").configure(session, name, resolve_values(CONNECTOR, connection))


@HTTPFS
def test_each_root_gets_its_own_scoped_secret_and_no_value_shows():
    session = Session()
    try:
        configure(session, "lake", CONNECTION)
        con = session.con
        assert con.execute("SELECT name FROM which_secret('s3://bucket-b/x.parquet', 's3')").fetchall() == [
            ("streamwright_lake_1",)]
        assert con.execute("SELECT name FROM which_secret('s3://bucket-c/x', 's3')").fetchall() == []
        assert "SECRET-123" not in str(con.execute("SELECT * FROM duckdb_secrets()").fetchall())
    finally:
        session.close()


@HTTPFS
def test_two_connections_never_share_credentials():
    session = Session()
    try:
        configure(session, "a", dict(CONNECTION, roots=["s3://shared/a/"]))
        configure(session, "b", dict(CONNECTION, roots=["s3://shared/b/"], key_id="OTHER"))
        assert session.con.execute("SELECT name FROM which_secret('s3://shared/b/x', 's3')").fetchall() == [
            ("streamwright_b_0",)]
    finally:
        session.close()


def test_read_paths_stay_inside_the_roots():
    adapter, connection = CONNECTOR.load_adapter("duckdb"), resolve_values(CONNECTOR, CONNECTION)
    operation = CONNECTOR.operation("object", "read")
    context = ReadContext(None, "lake", connection, {"name": "s"}, batch=None, pinned={}, limits={}, values=None)
    sql = adapter.read_sql(context, operation, {"path": "day=1/x.csv", "format": "csv"})
    assert "'s3://bucket-a/in/day=1/x.csv'" in sql and SECRET not in sql
    assert "'s3://bucket-b/y.csv'" in adapter.read_sql(context, operation, {"path": "s3://bucket-b/y.csv",
                                                                            "format": "csv"})
    for outside in ("s3://bucket-c/z.csv", "../z.csv", "z.csv?s3_endpoint=evil"):
        with pytest.raises(AccessError):
            adapter.read_sql(context, operation, {"path": outside, "format": "csv"})


def job(connection, **output):
    return {"kind": "DuckDBJob", "name": "j", "spec": {"secrets": {"key": {"type": "string"}}},
            "connections": {"lake": connection},
            "steps": [{"name": "s", "connection": "lake", "read": "parquet", "path": "x.parquet"}],
            "outputs": [dict({"name": "o", "connection": "lake", "step": "s", "path": "out.parquet",
                              "format": "parquet", "mode": "overwrite"}, **output)]}


def test_jobs_are_checked_from_the_spec():
    rules = DuckDBRules({"s3": DefinitionDuckDBAdapter(CONNECTOR)}, connectors=())
    declared = dict(CONNECTION, provider="s3", key_id="{{ secrets.key }}", secret="{{ secrets.key }}")
    assert [(i.code, i.path) for i in check_job(job(declared), rules)] == []
    assert [(i.code, i.path) for i in check_job(job(dict(declared, secret=SECRET)), rules)] == [
        ("literal-secret", "connections.lake.secret")]
    assert [(i.code, i.path) for i in check_job(job(declared, format="tsv"), rules)] == []
    found = check_job(job(dict(declared, url_style="virtual", roots=["gs://x/"]), format="xlsx"), rules)
    assert sorted((i.code, i.path) for i in found) == [
        ("bad-value", "connections.lake"), ("bad-value", "connections.lake"), ("bad-value", "outputs[0]")]
    assert [(i.code, i.path) for i in check_job(job(declared, mode="append"), rules)] == [
        ("unsupported-mode", "outputs[0]")]
