"""The postgres spec: certified, cheap to import, and the generic checks it drives."""

import subprocess
import sys
from types import MappingProxyType

import pytest

from streamwright.connectors.postgres import CONNECTOR, client
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

DECLARED = MappingProxyType({"host": "{{ config.pg_host }}", "port": "{{ config.pg_port }}", "database": "shop",
                             "user": "reader", "password": "{{ secrets.pg_password }}", "sslmode": "require"})
RESOLVED = MappingProxyType({"host": "db.internal", "port": 5432, "database": "shop", "user": "reader",
                             "password": "Sup3r-S3cret-pw"})


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_listing_the_connector_imports_no_engine_or_vendor_code():
    probe = ("import sys, streamwright.connectors.postgres; print(sorted(m for m in ('duckdb', 'pyspark', 'psycopg', "
             "'streamwright.connectors.postgres.client') if m in sys.modules))")
    assert subprocess.check_output([sys.executable, "-c", probe], text=True).strip() == "[]"


def test_a_declared_connection_is_checked_without_its_values():
    assert check_connection(CONNECTOR, DECLARED) == []
    assert check_connection(CONNECTOR, {"dsn": "{{ secrets.pg_dsn }}"}) == []
    assert codes(check_connection(CONNECTOR, dict(DECLARED, password="Sup3r-S3cret-pw"))) == [
        ("literal-secret", "password")]
    assert codes(check_connection(CONNECTOR, dict(DECLARED, host="{{ secrets.host }}", sslmode="maybe"))) == [
        ("secret-placement", "host"), ("bad-value", "sslmode")]
    # `dbname` is libpq's own parameter name: a vendor connection option now, forwarded to the driver
    assert check_connection(CONNECTOR, dict(DECLARED, dbname="shop")) == []


@pytest.mark.parametrize(("change", "problem"), [
    ({"dsn": "postgresql://u@h/d"}, "use either `dsn` or the connection keys, not both"),
    ({"host": None}, "needs `host` (or `dsn`)"),
    ({"host": "db internal"}, "`host` must be a hostname, an IP address or an absolute socket directory"),
    ({"host": "db\x00x"}, "`host` cannot hold a NUL or control character"),
    ({"port": 70000}, "`port` must be a port number (1-65535)"),
    ({"options": {"passfile": "/tmp/x"}}, "`options` cannot set `passfile`"),
    ({"options": {"dbname": "other"}}, "`options` cannot set `dbname`: use the connection key `database`"),
    ({"statement_timeout": "soon"}, "`statement_timeout`: 'soon' is not a timeout"),
])
def test_resolved_connections_are_checked(change, problem):
    values = {key: value for key, value in dict(RESOLVED, **change).items() if value is not None}
    with pytest.raises(InvalidConnectionError) as caught:
        resolve_values(CONNECTOR, values)
    assert problem in str(caught.value) and "Sup3r-S3cret-pw" not in str(caught.value)


def test_an_unknown_libpq_option_passes_through_to_the_driver():
    """§6b: postgres declares no closed option catalog, so any libpq parameter in `options` reaches the driver."""
    connection = resolve_values(CONNECTOR, dict(RESOLVED, options={"target_session_attrs": "read-write",
                                                                   "a_future_libpq_option": "x"}))
    parameters = dict(client.parameters(connection))
    assert parameters["target_session_attrs"] == "read-write" and parameters["a_future_libpq_option"] == "x"


def test_a_dsn_alone_resolves_and_a_bad_one_never_shows():
    assert resolve_values(CONNECTOR, {"dsn": "host=db dbname=shop user=r password='x y'"})["dsn"]
    with pytest.raises(InvalidConnectionError) as caught:
        resolve_values(CONNECTOR, {"dsn": "postgresql://u:Sup3r-S3cret-pw@h:99999999/d"})
    assert "not a valid postgres:// URL" in str(caught.value) and "Sup3r-S3cret-pw" not in str(caught.value)


def test_the_spec_uses_the_catalog_operations_and_each_engine_runs_its_kinds():
    assert [operation.name for operation in CONNECTOR.operations] == [
        "database.query", "database.table", "database.write"]
    assert [operation.name for operation in CONNECTOR.operations_for(SOURCE_CAPABILITIES)] == [
        "database.query", "database.table"]


def test_arguments_are_checked_against_each_engines_capabilities():
    query, write = CONNECTOR.operation("database", "query"), CONNECTOR.operation("database", "write")
    assert check_arguments(CONNECTOR, query, {"query": "SELECT * FROM t WHERE d >= $since",
                                              "params": {"since": "{{ window.start }}"}}, SOURCE_CAPABILITIES) == []
    assert codes(check_arguments(CONNECTOR, query, {"query": "SELECT 1", "options": {"fetchsize": 10}},
                                 SOURCE_CAPABILITIES)) == [("unsupported-argument", "arguments.options")]
    assert codes(check_arguments(CONNECTOR, write, {"table": "t"}, SOURCE_CAPABILITIES)) == [
        ("unsupported-operation", "")]
    assert check_arguments(CONNECTOR, write, {"table": "t", "mode": "merge", "primary_key": ["id"]},
                           DUCKDB_CAPABILITIES) == []
    assert codes(check_arguments(CONNECTOR, write, {"table": "t", "mode": "delete", "primary_key": ["id"]},
                                 DUCKDB_CAPABILITIES)) == [("unsupported-choice", "arguments.mode")]
    assert [issue.message for issue in check_arguments(CONNECTOR, write, {"table": "t", "mode": "merge"},
                                                       DUCKDB_CAPABILITIES)] == [
        "mode merge needs `primary_key` (the key columns)"]


@pytest.mark.parametrize(("query", "problem"), [
    ("SELECT 1; DROP TABLE t", "single statement"),
    ("DELETE FROM t", "read-only"),
    ("SELECT * FROM pg_catalog.pg_user", "system catalog or schema"),
    ("SELECT * FROM duckdb_secrets()", "cannot call duckdb_secrets"),
    ("SELECT * FROM read_csv('/etc/passwd')", "cannot call read_csv"),
    ("SELECT * FROM t WHERE id = ?", "named parameters"),
    ("SELECT * FROM t WHERE id = {{ config.id }}", "cannot hold references"),
])
def test_queries_are_checked_before_any_run(query, problem):
    issues = check_arguments(CONNECTOR, CONNECTOR.operation("database", "query"), {"query": query},
                             SOURCE_CAPABILITIES)
    assert {issue.code for issue in issues} == {"bad-value"} and problem in " ".join(issue.message for issue in issues)


def test_table_reads_check_names_and_conditions():
    table = CONNECTOR.operation("database", "table")
    assert check_arguments(CONNECTOR, table, {"table": "orders", "columns": ["id"], "where": [
        {"column": "status", "op": "IN", "value": ["a", "b"]}]}, SOURCE_CAPABILITIES) == []
    issues = check_arguments(CONNECTOR, table, {"table": "orders", "columns": [], "where": [
        {"column": "x", "op": "BETWEEN", "value": 1}]}, SOURCE_CAPABILITIES)
    assert len(issues) == 2 and "non-empty list" in issues[0].message


def test_the_guide_has_its_sections_and_tables_generated_from_the_spec():
    text = guide_text(CONNECTOR, "streamwright.connectors.postgres", ENGINE_CAPABILITIES)
    assert [line for line in text.splitlines() if line.startswith("## ")] == [
        "## Connection", "## Operations", "## Engines"]
    assert "| `password` | string | a `{{ secrets.NAME }}` reference | the role's password |" in text
    assert "### `database.write` (write; engines: duckdb)" in text


def test_a_vendor_connection_option_reaches_the_libpq_parameters():
    connection = resolve_values(CONNECTOR, dict(RESOLVED, connect_timeout="10", application_name="sw"))
    assert connection.vendor == {"connect_timeout": "10", "application_name": "sw"}
    parameters = dict(client.parameters(connection))
    assert parameters["connect_timeout"] == "10" and parameters["application_name"] == "sw"
    # the Spark JDBC path forwards the same vendor keys as driver properties
    properties = client.jdbc_values(connection)["properties"]
    assert properties["connect_timeout"] == "10" and properties["application_name"] == "sw"
