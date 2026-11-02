"""The gcs spec: certified, cheap to import, and the generic checks it drives."""

import subprocess
import sys
from types import MappingProxyType

import pytest

from streamwright.connectors.gcs import CONNECTOR
from streamwright.core.io.file_rules import DUCKDB_CAPABILITIES, ENGINE_CAPABILITIES, SOURCE_CAPABILITIES
from streamwright.core.sdk.contract import (
    InvalidConnectionError,
    check_arguments,
    check_connection,
    guide_text,
    resolve_values,
)
from streamwright.core.sdk.testing import certify

TestCertification = certify(CONNECTOR)

DECLARED = MappingProxyType({"roots": ["{{ config.bucket_root }}"], "key_id": "{{ secrets.key }}",
                             "secret": "{{ secrets.secret }}"})
RESOLVED = MappingProxyType({"roots": ["gs://acme-data/in/"], "key_id": "GOOGHMAC0042", "secret": "hmac-SECRET-0042"})


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_listing_the_connector_imports_no_engine_or_vendor_code():
    probe = ("import sys, streamwright.connectors.gcs; "
             "print(sorted(m for m in ('duckdb', 'pyspark', 'streamwright.connectors.gcs.client') "
             "if m in sys.modules))")
    assert subprocess.check_output([sys.executable, "-c", probe], text=True).strip() == "[]"


def test_a_declared_connection_is_checked_without_its_values():
    assert check_connection(CONNECTOR, DECLARED) == []
    assert codes(check_connection(CONNECTOR, dict(DECLARED, secret="hmac-written-in-the-source"))) == [
        ("literal-secret", "secret")]
    assert codes(check_connection(CONNECTOR, dict(DECLARED, roots=["s3://lake/"]))) == [("bad-value", "roots")]
    # an undeclared connection key is a vendor argument now, forwarded to the DuckDB secret / httpfs setting
    assert check_connection(CONNECTOR, dict(DECLARED, endpoint="storage.example.com")) == []


def test_resolving_keeps_the_key_out_of_messages_and_takes_both_halves_or_none():
    connection = resolve_values(CONNECTOR, RESOLVED)
    assert connection.redact("key hmac-SECRET-0042") == "key ***"
    assert resolve_values(CONNECTOR, {"roots": ["gs://public/"]})["roots"] == ["gs://public/"]  # anonymous
    with pytest.raises(InvalidConnectionError, match="key_id and secret go together"):
        resolve_values(CONNECTOR, {"roots": ["gs://a/"], "key_id": "GOOG"})
    with pytest.raises(InvalidConnectionError) as caught:
        resolve_values(CONNECTOR, dict(RESOLVED, roots=["gs://acme-data/in/?hmac-SECRET-0042"]))
    assert "hmac-SECRET-0042" not in str(caught.value)


def test_the_spec_uses_the_catalog_operations_and_each_engines_capabilities():
    assert [operation.name for operation in CONNECTOR.operations] == ["object.read", "object.write"]
    read, write = CONNECTOR.operation("object", "read"), CONNECTOR.operation("object", "write")
    assert check_arguments(CONNECTOR, read, {"path": "a.csv", "match": ".*"}, SOURCE_CAPABILITIES) == []
    assert codes(check_arguments(CONNECTOR, read, {"path": "a.csv", "match": ".*"}, DUCKDB_CAPABILITIES)) == [
        ("unsupported-argument", "arguments.match")]
    assert codes(check_arguments(CONNECTOR, write, {"path": "out/", "format": "csv"}, SOURCE_CAPABILITIES)) == [
        ("unsupported-operation", "")]
    assert codes(check_arguments(CONNECTOR, write, {"path": "out/", "format": "csv", "mode": "append"},
                                 DUCKDB_CAPABILITIES)) == [("unsupported-choice", "arguments.mode")]


def test_the_guide_has_its_sections_and_tables_generated_from_the_spec():
    text = guide_text(CONNECTOR, "streamwright.connectors.gcs", ENGINE_CAPABILITIES)
    assert [line for line in text.splitlines() if line.startswith("## ")] == [
        "## Connection", "## Operations", "## Engines"]
    assert "| `key_id` | string | a `{{ secrets.NAME }}` reference |" in text
    assert "### `object.write` (write; engines: duckdb)" in text
