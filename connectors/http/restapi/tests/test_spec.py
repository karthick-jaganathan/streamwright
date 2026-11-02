"""The restapi spec: certified, and the built-in authentication it takes."""

from streamwright.connectors.restapi import CONNECTOR
from streamwright.core.runtime.source_connectors import select_transport
from streamwright.core.sdk.contract import check_connection
from streamwright.core.sdk.testing import certify, components

TestCertification = certify(CONNECTOR)


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_the_connector_declares_http_requests_run_by_the_engine():
    assert select_transport(components.load("restapi")) == "http"
    assert [operation.name for operation in CONNECTOR.operations] == ["http.request"]


def test_every_credential_of_the_built_in_authentication_is_a_secret_reference():
    assert sorted(CONNECTOR.describe()["secret_keys"]) == [
        "client_secret", "password", "refresh_token", "token", "value"]
    written = {"basic": {"type": "basic", "username": "u", "password": "written-in-the-file"},
               "api_key": {"type": "api_key", "name": "X-Key", "value": "written-in-the-file"}}
    assert codes(check_connection(CONNECTOR, written["basic"])) == [("literal-secret", "password")]
    assert codes(check_connection(CONNECTOR, written["api_key"])) == [("literal-secret", "value")]
    assert check_connection(CONNECTOR, {"type": "api_key", "name": "X-Key", "value": "{{ secrets.key }}",
                                        "in": "query"}) == []


def test_an_unknown_authentication_type_is_refused():
    assert codes(check_connection(CONNECTOR, {"type": "digest"})) == [("bad-value", "type")]
