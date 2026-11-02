"""The openai_ads spec: certified, and the authentication it takes."""

from streamwright.connectors.openai_ads import CONNECTOR
from streamwright.core.runtime.source_connectors import select_transport
from streamwright.core.sdk.contract import check_connection
from streamwright.core.sdk.testing import certify, components

TestCertification = certify(CONNECTOR)


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_the_connector_declares_http_requests_run_by_the_engine():
    connector = components.load("openai_ads")
    assert select_transport(connector) == "http"
    assert [operation.name for operation in CONNECTOR.operations] == ["http.request"]


def test_the_token_is_a_secret_reference_and_other_authentication_is_refused():
    assert check_connection(CONNECTOR, {"type": "bearer", "token": "{{ secrets.key }}"}) == []
    assert codes(check_connection(CONNECTOR, {"type": "bearer", "token": "sk-written-in-the-file"})) == [
        ("literal-secret", "token")]
    assert codes(check_connection(CONNECTOR, {"type": "basic", "username": "u", "password": "{{ secrets.p }}"})) == [
        ("bad-value", "type"), ("secret-placement", "password")]
    assert codes(check_connection(CONNECTOR, {"token": "{{ secrets.key }}"})) == [("missing-key", "type")]
