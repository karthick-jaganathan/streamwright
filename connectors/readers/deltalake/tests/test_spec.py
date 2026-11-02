"""The deltalake spec: certified, cheap to import, and the generic checks it drives."""

import subprocess
import sys

import pytest

from streamwright.connectors.deltalake import CONNECTOR
from streamwright.core.io.capabilities import DUCKDB_CAPABILITIES, ENGINE_CAPABILITIES, SOURCE_CAPABILITIES
from streamwright.core.sdk.contract import (
    InvalidConnectionError,
    check_arguments,
    check_connection,
    guide_text,
    resolve_values,
)
from streamwright.core.sdk.testing import certify

TestCertification = certify(CONNECTOR)


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_listing_the_connector_imports_no_engine_or_vendor_code():
    probe = ("import sys, streamwright.connectors.deltalake; print(sorted(m for m in ('duckdb', 'pyspark', 'deltalake.'"
             "'_internal', 'streamwright.connectors.deltalake.client') if m in sys.modules))")
    assert subprocess.check_output([sys.executable, "-c", probe], text=True).strip() == "[]"


def test_credentials_are_secret_references_and_roots_are_required():
    declared = {"roots": ["s3://lake/"], "aws_access_key_id": "{{ secrets.k }}",
                "aws_secret_access_key": "{{ secrets.s }}", "aws_region": "us-east-1", "storage": "lake_s3"}
    assert check_connection(CONNECTOR, declared) == []
    assert codes(check_connection(CONNECTOR, dict(declared, azure_account_key="key", roots=None))) == [
        ("bad-value", "roots"), ("literal-secret", "azure_account_key")]
    assert codes(check_connection(CONNECTOR, {"aws_region": "us-east-1"})) == [("missing-key", "roots")]


@pytest.mark.parametrize(("roots", "problem"), [
    ([], "`roots` must name at least one"), (["/data/../etc"], "goes up a folder"), ([" "], "a non-empty text")])
def test_resolved_roots_keep_the_sandbox_fail_closed(roots, problem):
    with pytest.raises(InvalidConnectionError, match=problem):
        resolve_values(CONNECTOR, {"roots": roots})


def test_arguments_are_checked_against_each_engines_capabilities():
    read, write = CONNECTOR.operation("table", "read"), CONNECTOR.operation("table", "write")
    assert check_arguments(CONNECTOR, read, {"path": "cities", "columns": ["id"], "limit": 5},
                           SOURCE_CAPABILITIES) == []
    assert codes(check_arguments(CONNECTOR, read, {"path": "cities", "limit": 5}, DUCKDB_CAPABILITIES)) == [
        ("unsupported-argument", "arguments.limit")]
    assert codes(check_arguments(CONNECTOR, read, {"path": "cities?x=1"}, SOURCE_CAPABILITIES)) == [
        ("bad-value", "arguments.path")]
    assert [issue.message for issue in check_arguments(CONNECTOR, write, {"path": "t", "mode": "overwrite"},
                                                       DUCKDB_CAPABILITIES)] == [
        "overwrite needs replace_where, or whole_table: true to rewrite the whole table"]
    merge = check_arguments(CONNECTOR, write, {"path": "t", "mode": "merge", "condition": "c",
                                               "when_matched": [{"action": "insert"}]}, DUCKDB_CAPABILITIES)
    assert [issue.message for issue in merge] == ["when_matched[0]: action must be one of update/delete"]
    assert codes(check_arguments(CONNECTOR, write, {"path": "t", "table_name": "x"}, DUCKDB_CAPABILITIES)) == [
        ("unsupported-argument", "arguments.table_name")]


def test_table_query_runs_on_the_source_engine_only():
    query = CONNECTOR.operation("table", "query")
    assert query in CONNECTOR.operations_for(SOURCE_CAPABILITIES)
    assert query not in CONNECTOR.operations_for(DUCKDB_CAPABILITIES)
    assert codes(check_arguments(CONNECTOR, query, {"query": "SELECT 1"}, DUCKDB_CAPABILITIES)) == [
        ("unsupported-operation", "")]


def test_the_guide_has_its_sections_and_tables_generated_from_the_spec():
    text = guide_text(CONNECTOR, "streamwright.connectors.deltalake", ENGINE_CAPABILITIES)
    assert [line for line in text.splitlines() if line.startswith("## ")] == [
        "## Connection", "## Operations", "## Engines"]
    assert "### `table.write` (write; engines: duckdb)" in text
