"""The linkedin_ads spec: certified, and the authentication it takes."""

from streamwright.connectors.linkedin_ads import CONNECTOR
from streamwright.core.runtime.source_connectors import select_transport
from streamwright.core.sdk.contract import check_connection
from streamwright.core.sdk.testing import certify, components

TestCertification = certify(CONNECTOR)


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_the_connector_declares_http_requests_run_by_the_engine():
    assert select_transport(components.load("linkedin_ads")) == "http"
    assert [operation.name for operation in CONNECTOR.operations] == ["http.request"]


def test_the_token_is_a_secret_reference_and_other_authentication_is_refused():
    assert check_connection(CONNECTOR, {"type": "bearer", "token": "{{ secrets.key }}"}) == []
    assert codes(check_connection(CONNECTOR, {"type": "bearer", "token": "written-in-the-file"})) == [
        ("literal-secret", "token")]
    assert codes(check_connection(CONNECTOR, {"type": "api_key", "name": "k", "value": "{{ secrets.v }}"})) == [
        ("bad-value", "type"), ("secret-placement", "value")]
