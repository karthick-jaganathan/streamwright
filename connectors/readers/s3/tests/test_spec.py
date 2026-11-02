"""The s3 spec: certified, cheap to import, and the generic checks it drives."""

import subprocess
import sys
from types import MappingProxyType

import pytest

from streamwright.connectors.s3 import CONNECTOR, client
from streamwright.core.io.file_rules import DUCKDB_CAPABILITIES, ENGINE_CAPABILITIES, SOURCE_CAPABILITIES
from streamwright.core.io.locations import secret_parameters
from streamwright.core.sdk.contract import (
    InvalidConnectionError,
    check_arguments,
    check_connection,
    guide_text,
    resolve_values,
)
from streamwright.core.sdk.testing import certify

TestCertification = certify(CONNECTOR)

DECLARED = MappingProxyType({
    "roots": ["{{ config.bucket_root }}"], "key_id": "{{ secrets.key }}", "secret": "{{ secrets.secret }}",
    "region": "{{ config.region }}",
})
RESOLVED = MappingProxyType({
    "roots": ["s3://acme-data/in/"], "key_id": "AKIAEXAMPLE", "secret": "wJalr-SECRET-0042",
})


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_listing_the_connector_imports_no_engine_or_vendor_code():
    probe = ("import sys, streamwright.connectors.s3; "
             "print(sorted(m for m in ('duckdb', 'pyspark', 'streamwright.connectors.s3.client') if m in sys.modules))")
    assert subprocess.check_output([sys.executable, "-c", probe], text=True).strip() == "[]"


def test_a_declared_connection_is_checked_without_its_values():
    assert check_connection(CONNECTOR, DECLARED) == []
    assert codes(check_connection(CONNECTOR, dict(DECLARED, secret="wJalrXUtnFEMIwrittenInTheSource"))) == [
        ("literal-secret", "secret")]
    assert codes(check_connection(CONNECTOR, dict(DECLARED, region="{{ secrets.region }}"))) == [
        ("secret-placement", "region")]
    assert codes(check_connection(CONNECTOR, {"bucket": "x"})) == [("missing-key", "roots")]  # bucket is vendor now
    assert codes(check_connection(CONNECTOR, dict(DECLARED, roots=["gs://lake/"], url_style="virtual"))) == [
        ("bad-value", "roots"), ("bad-value", "url_style")]


@pytest.mark.parametrize(("settings", "problem"), [
    ({"url_style": "virtual"}, "url_style: expected one of: vhost, path"),
    ({"use_ssl": "yes"}, "use_ssl: expected boolean"),
    ({"region": "us east"}, "region: 'us east' does not match"),
    ({"endpoint": "https://minio:9000"}, "endpoint: 'https://minio:9000' does not match"),
    ({"endpoint": "evil.example.com/?s3_endpoint=x"}, "endpoint: 'evil.example.com/?s3_endpoint=x' does not match"),
])
def test_resolved_settings_are_checked(settings, problem):
    with pytest.raises(InvalidConnectionError, match=problem.replace("?", r"\?")):
        resolve_values(CONNECTOR, dict(RESOLVED, **settings))


def test_resolving_applies_defaults_converts_rendered_text_and_never_shows_a_secret():
    connection = resolve_values(CONNECTOR, dict(RESOLVED, use_ssl="false", endpoint="minio.local:9000"))
    assert (connection["use_ssl"], connection["endpoint"]) == (False, "minio.local:9000")
    assert resolve_values(CONNECTOR, RESOLVED)["use_ssl"] is True
    assert connection.redact("key wJalr-SECRET-0042") == "key ***"
    with pytest.raises(InvalidConnectionError) as caught:
        resolve_values(CONNECTOR, dict(RESOLVED, roots=["s3://acme-data/in/?wJalr-SECRET-0042"]))
    assert "wJalr-SECRET-0042" not in str(caught.value)


def test_optional_endpoint_overrides_are_omitted_when_their_config_is_unset():
    # The s3_demo example templates endpoint/url_style/use_ssl from optional config inputs: unset they render to
    # None and are dropped (Amazon S3); set, they target an S3-compatible store.
    unset = resolve_values(CONNECTOR, dict(RESOLVED, endpoint=None, url_style=None, use_ssl=None))
    assert "endpoint" not in dict(unset) and "url_style" not in dict(unset)
    assert unset["use_ssl"] is True                       # the connector default still applies
    pointed = resolve_values(CONNECTOR, dict(RESOLVED, endpoint="127.0.0.1:4566", url_style="path", use_ssl=False))
    assert (pointed["endpoint"], pointed["url_style"], pointed["use_ssl"]) == ("127.0.0.1:4566", "path", False)


def test_the_spec_uses_the_catalog_operations():
    assert [operation.name for operation in CONNECTOR.operations] == ["object.read", "object.write"]
    assert [operation.name for operation in CONNECTOR.operations_for(SOURCE_CAPABILITIES)] == ["object.read"]


def test_arguments_are_checked_against_each_engines_capabilities():
    read, write = CONNECTOR.operation("object", "read"), CONNECTOR.operation("object", "write")
    assert check_arguments(CONNECTOR, read, {"path": "a.csv", "match": ".*"}, SOURCE_CAPABILITIES) == []
    unsupported = check_arguments(CONNECTOR, read, {"path": "a.csv", "match": ".*"}, DUCKDB_CAPABILITIES)
    assert codes(unsupported) == [("unsupported-argument", "arguments.match")]
    assert unsupported[0].message == "`match` is not supported by the duckdb engine"
    assert codes(check_arguments(CONNECTOR, read, {"path": "a.csv", "row_errors": True}, SOURCE_CAPABILITIES)) == [
        ("unsupported-argument", "arguments.row_errors")]
    # an undeclared argument is a vendor option now (forwarded to DuckDB's reader), not an unknown-key error
    assert check_arguments(CONNECTOR, read, {"path": "a.csv", "bucket": "x"}, SOURCE_CAPABILITIES) == []
    assert codes(check_arguments(CONNECTOR, read, {"path": ["a.csv", "b.csv?x=1"], "format": "xlsx"})) == [
        ("bad-value", "arguments.path"), ("bad-value", "arguments.format")]
    assert codes(check_arguments(CONNECTOR, read, {"path": "{{ secrets.x }}"})) == [
        ("secret-placement", "arguments.path")]
    assert codes(check_arguments(CONNECTOR, write, {"path": "out/", "format": "csv"}, SOURCE_CAPABILITIES)) == [
        ("unsupported-operation", "")]
    assert check_arguments(CONNECTOR, write, {"path": "out/", "format": "tsv", "mode": "overwrite"},
                           DUCKDB_CAPABILITIES) == []
    append = check_arguments(CONNECTOR, write, {"path": "out/", "format": "jsonl", "mode": "append"},
                             DUCKDB_CAPABILITIES)
    assert codes(append) == [("unsupported-choice", "arguments.mode")]
    assert append[0].message.startswith("`mode: append` is not supported by the duckdb engine")


def test_the_engines_semantic_checks_run_after_the_declared_ones():
    read = CONNECTOR.operation("object", "read")
    assert [issue.message for issue in check_arguments(CONNECTOR, read, {"path": "in/", "recursive": True},
                                                       SOURCE_CAPABILITIES)] == [
        "`recursive` goes with `match` (the folder to look in is `path`)"]
    write = CONNECTOR.operation("object", "write")
    assert codes(check_arguments(CONNECTOR, write, {"path": "out/", "format": "csv", "split_by": ["day"]},
                                 DUCKDB_CAPABILITIES)) == [("bad-value", "arguments")]


def test_each_engines_schema_holds_only_what_it_supports():
    write = CONNECTOR.operation("object", "write")
    duckdb = write.json_schema(DUCKDB_CAPABILITIES)["properties"]
    assert "single_file" not in duckdb and "partition_by" in duckdb
    assert duckdb["mode"]["anyOf"][0]["enum"] == ["error", "overwrite"]
    assert set(write.json_schema()["properties"]) == {argument.name for argument in write.arguments}
    described = CONNECTOR.describe(*ENGINE_CAPABILITIES)["capabilities"]
    assert sorted(described) == ["duckdb", "source"] and list(described["source"]) == ["object.read"]
    assert described["duckdb"]["object.write"]["mode"] == ["error", "overwrite"]


def test_the_connection_schema_declares_secret_fields_as_references():
    properties = CONNECTOR.json_schema()["properties"]
    assert properties["key_id"]["pattern"].startswith("^")
    assert CONNECTOR.describe()["secret_keys"] == ["key_id", "secret", "session_token"]


def test_the_guide_has_its_sections_and_tables_generated_from_the_spec():
    text = guide_text(CONNECTOR, "streamwright.connectors.s3", ENGINE_CAPABILITIES)
    assert [line for line in text.splitlines() if line.startswith("## ")] == [
        "## Connection", "## Operations", "## Engines"]
    assert "| `use_ssl` | boolean | default `true` |" in text
    assert "### `object.write` (write; engines: duckdb)" in text
    assert "| `match` | string |  | a regex the keys below the folder `path` must fully match | source |" in text
    assert "| duckdb (error, overwrite) |" in text


def test_a_vendor_connection_key_reaches_the_duckdb_secret_parameters():
    connection = resolve_values(CONNECTOR, dict(RESOLVED, requester_pays="true", kms_key_id="alias/data"))
    assert connection.vendor == {"requester_pays": "true", "kms_key_id": "alias/data"}
    parameters = dict(secret_parameters(client.STORE, connection))
    assert parameters["requester_pays"] == "true" and parameters["kms_key_id"] == "alias/data"
