"""The amazon_ads spec: certified, and the authentication it takes."""

from types import MappingProxyType

from streamwright.connectors.amazon_ads import CONNECTOR
from streamwright.core.runtime.source_connectors import select_transport
from streamwright.core.sdk.contract import check_connection
from streamwright.core.sdk.testing import certify, components

TestCertification = certify(CONNECTOR)

OAUTH = MappingProxyType({"type": "oauth2_refresh_token", "token_url": "https://api.amazon.com/auth/o2/token",
         "client_id": "{{ secrets.id }}", "client_secret": "{{ secrets.secret }}",
         "refresh_token": "{{ secrets.refresh }}"})


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_the_connector_declares_http_requests_run_by_the_engine():
    assert select_transport(components.load("amazon_ads")) == "http"
    assert [operation.name for operation in CONNECTOR.operations] == ["http.request"]


def test_oauth_credentials_are_secret_references_including_the_client_id():
    assert check_connection(CONNECTOR, OAUTH) == []
    for key in ("client_id", "client_secret", "refresh_token"):
        assert codes(check_connection(CONNECTOR, dict(OAUTH, **{key: "written-in-the-file"}))) == [
            ("literal-secret", key)]
    assert check_connection(CONNECTOR, {"type": "bearer", "token": "{{ secrets.key }}"}) == []


def test_other_authentication_is_refused():
    assert codes(check_connection(CONNECTOR, {"type": "basic", "username": "u", "password": "{{ secrets.p }}"})) == [
        ("bad-value", "type"), ("secret-placement", "password")]
