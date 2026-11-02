"""The mongodb spec: certified, cheap to import, and the generic checks it drives."""

import re
import subprocess
import sys

import pytest

from streamwright.connectors.mongodb import CONNECTOR
from streamwright.core.io.capabilities import ENGINE_CAPABILITIES, SOURCE_CAPABILITIES
from streamwright.core.sdk.contract import (
    InvalidConnectionError,
    check_arguments,
    check_connection,
    guide_text,
    resolve_values,
)
from streamwright.core.sdk.testing import certify

TestCertification = certify(CONNECTOR)


def test_listing_the_connector_imports_no_engine_or_vendor_code():
    probe = ("import sys, streamwright.connectors.mongodb; print(sorted(m for m in ('duckdb', 'pyspark', 'pymongo', "
             "'streamwright.connectors.mongodb.client') if m in sys.modules))")
    assert subprocess.check_output([sys.executable, "-c", probe], text=True).strip() == "[]"


def test_credentials_are_secret_references():
    assert check_connection(CONNECTOR, {"uri": "{{ secrets.uri }}"}) == []
    assert [(issue.code, issue.path) for issue in check_connection(CONNECTOR, {"uri": "mongodb://u:p@h/d"})] == [
        ("literal-secret", "uri")]


@pytest.mark.parametrize(("connection", "problem"), [
    ({"host": "h"}, "needs either `uri` or `host` and `database`"),
    ({"uri": "https://h/d"}, "must be a mongodb:// or mongodb+srv:// URI"),
    ({"uri": "mongodb://h/d", "host": "h"}, "use either `uri` or the connection keys"),
    ({"host": "h", "database": "d", "username": "u"}, "`username` and `password` go together"),
])
def test_resolved_connections_are_checked(connection, problem):
    with pytest.raises(InvalidConnectionError, match=re.escape(problem)):
        resolve_values(CONNECTOR, connection)


def test_the_operations_run_where_their_kind_runs():
    assert [item.name for item in CONNECTOR.operations_for(SOURCE_CAPABILITIES)] == [
        "collection.find", "collection.aggregate"]
    find = CONNECTOR.operation("collection", "find")
    assert check_arguments(CONNECTOR, find, {"collection": "users", "filter": {"status": "active"},
                                             "sort": [["_id", 1]]}, SOURCE_CAPABILITIES) == []
    assert [issue.code for issue in check_arguments(CONNECTOR, find, {"filter": []}, SOURCE_CAPABILITIES)] == [
        "missing-key", "bad-value"]


def test_the_guide_has_its_sections_and_tables_generated_from_the_spec():
    text = guide_text(CONNECTOR, "streamwright.connectors.mongodb", ENGINE_CAPABILITIES)
    assert [line for line in text.splitlines() if line.startswith("## ")] == [
        "## Connection", "## Operations", "## Engines"]
    assert "### `collection.find` (read; engines: source)" in text
