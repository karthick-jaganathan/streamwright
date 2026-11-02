"""The files spec: certified, cheap to import, and the generic checks it drives."""

import subprocess
import sys

import pytest

from streamwright.connectors.files import CONNECTOR
from streamwright.core.io.file_rules import DUCKDB_CAPABILITIES, ENGINE_CAPABILITIES, SOURCE_CAPABILITIES
from streamwright.core.sdk.contract import (
    InvalidConnectionError,
    check_arguments,
    check_connection,
    guide_text,
    resolve_values,
)
from streamwright.core.sdk.testing import certify
from streamwright.spark.io.file_rules import CAPABILITIES as SPARK_CAPABILITIES

TestCertification = certify(CONNECTOR, (*ENGINE_CAPABILITIES, SPARK_CAPABILITIES))


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_listing_the_connector_imports_no_engine_or_vendor_code():
    probe = ("import sys, streamwright.connectors.files; "
             "print(sorted(m for m in ('duckdb', 'pyspark', 'streamwright.connectors.files.client') "
             "if m in sys.modules))")
    assert subprocess.check_output([sys.executable, "-c", probe], text=True).strip() == "[]"


def test_a_declared_connection_takes_folders_and_no_secret_or_url():
    assert check_connection(CONNECTOR, {"roots": ["{{ config.data_root }}"]}) == []
    assert check_connection(CONNECTOR, {"roots": ["/data", "relative/folder"]}) == []
    assert codes(check_connection(CONNECTOR, {"roots": ["{{ secrets.root }}"]})) == [("secret-placement", "roots")]
    assert codes(check_connection(CONNECTOR, {"roots": ["s3://bucket/prefix/"]})) == [("bad-value", "roots")]
    assert check_connection(CONNECTOR, {"roots": ["/data"], "token": "x"}) == []  # `token` is a vendor argument now
    assert codes(check_connection(CONNECTOR, {})) == [("missing-key", "roots")]
    assert codes(check_connection(CONNECTOR, {"roots": "/data"})) == [("bad-value", "roots")]


@pytest.mark.parametrize(("roots", "problem"), [
    ([], "roots: name at least one folder"),
    ([""], "is not a folder"),
    (["https://files.example.com/pub/"], "does not match"),
])
def test_resolved_roots_are_local_folders(roots, problem):
    with pytest.raises(InvalidConnectionError, match=problem):
        resolve_values(CONNECTOR, {"roots": roots})


def test_the_spec_uses_the_catalog_operations_and_each_engines_capabilities():
    assert [operation.name for operation in CONNECTOR.operations] == ["object.read", "object.write"]
    read, write = CONNECTOR.operation("object", "read"), CONNECTOR.operation("object", "write")
    assert check_arguments(CONNECTOR, read, {"path": "a/*.csv", "match": ".*", "recursive": True},
                           SOURCE_CAPABILITIES) == []
    assert codes(check_arguments(CONNECTOR, read, {"path": "a.csv", "match": ".*"}, DUCKDB_CAPABILITIES)) == [
        ("unsupported-argument", "arguments.match")]
    assert codes(check_arguments(CONNECTOR, write, {"path": "o/", "format": "csv"}, SOURCE_CAPABILITIES)) == [
        ("unsupported-operation", "")]
    assert check_arguments(CONNECTOR, write, {"path": "o/", "format": "csv", "mode": "overwrite"},
                           DUCKDB_CAPABILITIES) == []
    assert check_arguments(CONNECTOR, write, {"path": "o/{{ batch.id }}/", "format": "csv", "mode": "append"},
                           SPARK_CAPABILITIES) == []


@pytest.mark.parametrize("capabilities", [SOURCE_CAPABILITIES, DUCKDB_CAPABILITIES])
def test_the_reader_options_are_typed_for_known_names_and_pass_vendor_names_through(capabilities):
    read = CONNECTOR.operation("object", "read")
    options = {"header": True, "delimiter": ";", "nullstr": "NA", "dateformat": "%Y-%m-%d"}
    assert check_arguments(CONNECTOR, read, {"path": "a.csv", "format": "csv", "options": options},
                           capabilities) == []
    # DuckDB's own read_csv parameter names are vendor options now: forwarded, not rejected
    for vendor in ({"delim": ";"}, {"sep": ";"}, {"nullValue": "NA"}):
        assert check_arguments(CONNECTOR, read, {"path": "a.csv", "format": "csv", "options": vendor},
                               capabilities) == [], vendor
    # a known option with a bad value is still caught
    assert codes(check_arguments(CONNECTOR, read, {"path": "a.csv", "format": "csv",
                                                   "options": {"header": "yes"}}, capabilities)) == [
        ("bad-value", "arguments")]
    # an option a format does not know is a vendor option: forwarded to the engine
    assert check_arguments(CONNECTOR, read, {"path": "a.parquet", "format": "parquet",
                                             "options": {"header": True}}, capabilities) == []


def test_the_spark_engine_types_known_options_and_forwards_vendor_options():
    read = CONNECTOR.operation("object", "read")
    options = {"header": True, "delimiter": ";", "nullstr": "NA", "dateformat": "yyyy", "multiLine": True}
    assert check_arguments(CONNECTOR, read, {"path": "a.csv", "options": options}, SPARK_CAPABILITIES) == []
    # `skip` is a vendor option on Spark (forwarded verbatim); a known option with a bad value is still caught
    found = check_arguments(CONNECTOR, read, {"path": "a.csv", "options": {"skip": 1, "nullstr": ["", "NA"]}},
                            SPARK_CAPABILITIES)
    assert [issue.message for issue in found] == ["option 'nullstr' is a single text on the spark engine"]


def test_the_guide_has_its_sections_and_tables_generated_from_the_spec():
    text = guide_text(CONNECTOR, "streamwright.connectors.files", (*ENGINE_CAPABILITIES, SPARK_CAPABILITIES))
    assert [line for line in text.splitlines() if line.startswith("## ")] == [
        "## Connection", "## Operations", "## Engines"]
    assert "### `object.write` (write; engines: duckdb, spark)" in text
