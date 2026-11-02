"""The mysql spec: certified, cheap to import, and the generic checks it drives."""

import subprocess
import sys
from types import MappingProxyType

import pytest

from streamwright.connectors.mysql import CONNECTOR
from streamwright.core.io.capabilities import DUCKDB_CAPABILITIES, ENGINE_CAPABILITIES, SOURCE_CAPABILITIES
from streamwright.core.sdk.contract import InvalidConnectionError, check_arguments, guide_text, resolve_values
from streamwright.core.sdk.testing import certify

TestCertification = certify(CONNECTOR)
RESOLVED = MappingProxyType({"host": "db", "database": "shop", "user": "reader", "password": "Sup3r-S3cret-pw"})


def test_listing_the_connector_imports_no_engine_or_vendor_code():
    probe = ("import sys, streamwright.connectors.mysql; print(sorted(m for m in ('duckdb', 'pyspark', 'pymysql', "
             "'streamwright.connectors.mysql.client') if m in sys.modules))")
    assert subprocess.check_output([sys.executable, "-c", probe], text=True).strip() == "[]"


@pytest.mark.parametrize(("change", "problem"), [
    ({"options": {"ssl_mode": "x"}}, "`options` key `ssl_mode` is not a connection parameter"),
    ({"options": {"password": "x"}}, "`options` cannot set `password`"),
    ({"sslmode": "require"}, "sslmode: expected one of: disabled"),
])
def test_resolved_connections_are_checked(change, problem):
    with pytest.raises(InvalidConnectionError) as caught:
        resolve_values(CONNECTOR, dict(RESOLVED, **change))
    assert problem in str(caught.value) and "Sup3r-S3cret-pw" not in str(caught.value)


def test_mysql_system_schemas_are_refused_and_writes_are_checked_per_engine():
    query, write = CONNECTOR.operation("database", "query"), CONNECTOR.operation("database", "write")
    assert check_arguments(CONNECTOR, query, {"query": "SELECT * FROM orders"}, SOURCE_CAPABILITIES) == []
    merge = {"table": "t", "mode": "merge", "primary_key": ["id"]}
    # DuckDB's mysql extension has no MERGE: the spec's engine limit, checked before a job runs
    assert [issue.message for issue in check_arguments(CONNECTOR, write, merge, DUCKDB_CAPABILITIES)] == [
        "`mode: merge` is not supported by the duckdb engine (supported: append, overwrite)"]
    assert check_arguments(CONNECTOR, write, dict(merge, mode="append"), DUCKDB_CAPABILITIES) == []
    assert [issue.code for issue in check_arguments(CONNECTOR, write, {"table": "t", "staging_table": "s"},
                                                    DUCKDB_CAPABILITIES)] == ["unsupported-argument"]


def test_the_guide_has_its_sections_and_tables_generated_from_the_spec():
    text = guide_text(CONNECTOR, "streamwright.connectors.mysql", ENGINE_CAPABILITIES)
    assert [line for line in text.splitlines() if line.startswith("## ")] == [
        "## Connection", "## Operations", "## Engines"]
    assert "### `database.query` (read; engines: source, duckdb)" in text
