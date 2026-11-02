"""The azure_blob DuckDB adapter (kind: DuckDBJob): per-root scoped secrets, read-only."""

from types import MappingProxyType

import pytest

from streamwright.connectors.azure_blob import CONNECTOR
from streamwright.core.io.files import AccessError
from streamwright.core.jobs.duckdb.definition_adapter import DefinitionDuckDBAdapter
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.engines.duckdb import DuckDBAdapter, DuckDBWriteAdapter, ReadContext
from streamwright.core.sdk.testing import DuckDBRules, check_job

CONNECTION = MappingProxyType({"roots": ["azure://container/in/", "abfss://other/"], "account_name": "acct",
                               "account_key": "key-SECRET-0042"})


class FakeSession:
    def __init__(self):
        self.extensions, self.secrets = [], []

    def load_extension(self, name):
        self.extensions.append(name)

    def create_secret(self, name, kind, options, scope=None):
        self.secrets.append((name, kind, dict(options), scope))


def adapter():
    return CONNECTOR.load_adapter("duckdb")


def test_one_secret_per_root_through_the_azure_extension():
    session = FakeSession()
    adapter().configure(session, "lake", resolve_values(CONNECTOR, CONNECTION))
    assert session.extensions == ["azure"]
    assert [(name, kind, scope) for name, kind, _, scope in session.secrets] == [
        ("lake_0", "azure", "azure://container/in/"), ("lake_1", "azure", "abfss://other/")]
    assert session.secrets[0][2]["CONNECTION_STRING"].startswith("DefaultEndpointsProtocol=https;AccountName=acct;")


def test_the_adapter_reads_only_so_it_has_no_write_to_refuse():
    assert isinstance(adapter(), DuckDBAdapter) and not isinstance(adapter(), DuckDBWriteAdapter)
    assert DefinitionDuckDBAdapter(CONNECTOR).write_target(CONNECTION) is None


def test_read_paths_stay_inside_the_roots():
    connection = resolve_values(CONNECTOR, CONNECTION)
    operation = CONNECTOR.operation("object", "read")
    context = ReadContext(None, "lake", connection, {"name": "s"}, batch=None, pinned={}, limits={}, values=None)
    sql = adapter().read_sql(context, operation, {"path": "day=1/x.csv", "format": "csv"})
    assert "'azure://container/in/day=1/x.csv'" in sql and "key-SECRET-0042" not in sql
    for outside in ("azure://other/z.csv", "../z.csv", "z.csv?x=1", "s3://container/in/z.csv"):
        with pytest.raises(AccessError):
            adapter().read_sql(context, operation, {"path": outside, "format": "csv"})


def job(connection, **output):
    return {"kind": "DuckDBJob", "name": "j", "spec": {"secrets": {"key": {"type": "string"}}},
            "connections": {"lake": connection},
            "steps": [{"name": "s", "connection": "lake", "read": "csv", "path": "x.csv"}],
            "outputs": [dict({"name": "o", "connection": "lake", "step": "s", "path": "out.parquet",
                              "format": "parquet", "mode": "overwrite"}, **output)]}


def test_jobs_read_but_cannot_write_to_azure():
    rules = DuckDBRules({"azure_blob": DefinitionDuckDBAdapter(CONNECTOR)}, connectors=())
    declared = {"provider": "azure_blob", "roots": ["azure://c/"], "account_name": "acct",
                "account_key": "{{ secrets.key }}"}
    assert [(issue.code, issue.path) for issue in check_job(job(declared), rules)] == [
        ("not-writable", "outputs[0].connection")]
    reading = dict(job(declared), steps=[dict(job(declared)["steps"][0], read="parquet", path="x.parquet")])
    assert "unsupported-read" not in [issue.code for issue in check_job(reading, rules)]
