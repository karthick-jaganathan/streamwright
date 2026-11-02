"""The azure_blob spec: certified, cheap to import, and the generic checks it drives."""

import subprocess
import sys
from types import MappingProxyType

import pytest

from streamwright.connectors.azure_blob import CONNECTOR
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

DECLARED = MappingProxyType({"roots": ["{{ config.container_root }}"], "account_name": "{{ config.account }}",
                             "account_key": "{{ secrets.key }}"})
ROOTS = ("azure://acme-data/in/",)


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_listing_the_connector_imports_no_engine_or_vendor_code():
    probe = ("import sys, streamwright.connectors.azure_blob; "
             "print(sorted(m for m in ('duckdb', 'pyspark', 'streamwright.connectors.azure_blob.client') "
             "if m in sys.modules))")
    assert subprocess.check_output([sys.executable, "-c", probe], text=True).strip() == "[]"


def test_a_declared_connection_is_checked_without_its_values():
    assert check_connection(CONNECTOR, DECLARED) == []
    assert codes(check_connection(CONNECTOR, dict(DECLARED, account_key="written-in-the-source"))) == [
        ("literal-secret", "account_key")]
    assert codes(check_connection(CONNECTOR, dict(DECLARED, roots=["s3://lake/"]))) == [("bad-value", "roots")]
    assert codes(check_connection(CONNECTOR, dict(DECLARED, endpoint={"a": 1}))) == [("bad-value", "endpoint")]


@pytest.mark.parametrize(("values", "problem"), [
    ({}, "needs credentials"),
    ({"account_key": "k0042"}, "account_key requires account_name"),
    ({"client_secret": "s0042"}, "client_secret requires account_name"),
    ({"account_name": "acc", "endpoint": "evil.example.com;AccountKey=other"}, "endpoint: expected a host"),
    ({"account_name": "acc", "account_key": "a;b"}, "cannot hold `;`"),
])
def test_one_way_to_sign_in_and_a_safe_endpoint_are_needed(values, problem):
    with pytest.raises(InvalidConnectionError, match=problem):
        resolve_values(CONNECTOR, dict({"roots": list(ROOTS)}, **values))


def test_the_credential_combinations_resolve_and_never_show_a_secret():
    for values in ({"connection_string": "AccountName=acc;AccountKey=k0042=="}, {"account_name": "acc"},
                   {"account_name": "acc", "client_secret": "s0042", "tenant_id": "t", "client_id": "c"}):
        assert resolve_values(CONNECTOR, dict({"roots": list(ROOTS)}, **values))["roots"] == list(ROOTS)
    connection = resolve_values(CONNECTOR, {"roots": list(ROOTS), "account_name": "acc",
                                            "account_key": "key-SECRET-0042"})
    assert connection.redact("key-SECRET-0042") == "***" and connection["use_ssl"] is True


def test_duckdb_only_reads_which_the_spec_declares_once():
    assert [(limit.engine, limit.operation, dict(limit.choices)) for limit in CONNECTOR.engine_limits] == [
        ("duckdb", "object.write", {})]
    assert [item.name for item in CONNECTOR.operations_for(DUCKDB_CAPABILITIES)] == ["object.read"]
    assert [item.name for item in CONNECTOR.operations_for(SPARK_CAPABILITIES)] == ["object.read", "object.write"]
    write = CONNECTOR.operation("object", "write")
    assert codes(check_arguments(CONNECTOR, write, {"path": "out/", "format": "csv"}, DUCKDB_CAPABILITIES)) == [
        ("unsupported-operation", "")]
    assert check_arguments(CONNECTOR, write, {"path": "out/", "format": "csv"}, SPARK_CAPABILITIES) == []
    assert "object.write" not in CONNECTOR.describe(*ENGINE_CAPABILITIES)["capabilities"]["duckdb"]


def test_each_engine_takes_the_arguments_it_supports():
    read = CONNECTOR.operation("object", "read")
    assert check_arguments(CONNECTOR, read, {"path": "a.csv", "match": ".*"}, SOURCE_CAPABILITIES) == []
    assert codes(check_arguments(CONNECTOR, read, {"path": "a.csv", "match": ".*"}, DUCKDB_CAPABILITIES)) == [
        ("unsupported-argument", "arguments.match")]
    options = {"null_padding": True, "nullstr": ["", "NA"], "dateformat": "%Y-%m-%d", "ignore_errors": True}
    assert check_arguments(CONNECTOR, read, {"path": "a.csv", "options": options}, SOURCE_CAPABILITIES) == []


def test_the_guide_shows_duckdb_for_reads_only():
    text = guide_text(CONNECTOR, "streamwright.connectors.azure_blob", (*ENGINE_CAPABILITIES, SPARK_CAPABILITIES))
    assert [line for line in text.splitlines() if line.startswith("## ")] == [
        "## Connection", "## Operations", "## Engines"]
    assert "### `object.read` (read; engines: source, duckdb, spark)" in text
    assert "### `object.write` (write; engines: spark)" in text
