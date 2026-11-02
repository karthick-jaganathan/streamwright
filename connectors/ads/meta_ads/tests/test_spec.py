"""The meta_ads spec: certified, read-only, and the connection it takes."""

import pytest
from facebook_business.exceptions import FacebookRequestError

from streamwright.connectors.meta_ads import CONNECTOR, client
from streamwright.core.runtime.source_connectors import check_call
from streamwright.core.sdk.contract import InvalidConnectionError, check_arguments, check_connection, resolve_values
from streamwright.core.sdk.engines.source import ConnectorContext
from streamwright.core.sdk.testing import certify, components, load_source

TestCertification = certify(CONNECTOR)


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_the_example_source_is_valid():
    assert components.check_source(load_source("examples/sources/ads/meta_ads")) == []


def test_the_secrets_are_secret_references():
    assert check_connection(CONNECTOR, {"access_token": "{{ secrets.token }}", "app_secret": "{{ secrets.s }}"}) == []
    assert CONNECTOR.describe()["secret_keys"] == ["access_token", "app_secret"]
    assert codes(check_connection(CONNECTOR, {"access_token": "EAAB-written"})) == [("literal-secret", "access_token")]


def test_reads_are_families_and_the_installed_sdk_decides_which_objects_exist():
    names = [operation.name for operation in CONNECTOR.operations]
    assert names == ["*.api_get", "*.get_*"]  # the vendor's surface is never enumerated (§6b)
    connector = components.load("meta_ads")
    # a write method matches no read family, so it is refused before the SDK is even consulted
    (problem,) = check_call(connector, {"service": "AdAccount", "method": "create_campaign",
                                        "arguments": {"id": "act_1"}})
    assert problem.startswith("meta_ads: AdAccount.create_campaign is not supported (supported: *.api_get, *.get_*")
    # any real object reads (Page was not in the old closed list): the installed facebook_business decides
    assert check_call(connector, {"service": "Page", "method": "api_get", "arguments": {"id": "1"}}) == []
    assert check_call(connector, {"service": "AdAccount", "method": "get_insights",
                                  "arguments": {"id": "act_1"}}) == []


def test_a_typo_is_caught_against_the_installed_sdk_before_a_run():
    connector = components.load("meta_ads")
    (problem,) = check_call(connector, {"service": "AdAccountt", "method": "api_get", "arguments": {"id": "1"}})
    assert "AdAccountt is not a Marketing API object" in problem and "did you mean AdAccount" in problem
    (problem,) = check_call(connector, {"service": "AdAccount", "method": "get_nonsense", "arguments": {"id": "1"}})
    assert "AdAccount has no get_nonsense method" in problem


def test_arguments_are_checked():
    insights = CONNECTOR.operation("AdAccount", "get_insights")
    assert check_arguments(CONNECTOR, insights, {"id": "act_1", "fields": ["spend"], "params": {"level": "ad"}}) == []
    assert check_arguments(CONNECTOR, insights, {"id": 12, "fields": "spend,clicks"}) == []
    assert codes(check_arguments(CONNECTOR, insights, {"fields": ["spend"], "limit": 1})) == [
        ("missing-key", "arguments.id")]  # limit is a vendor Graph API param now
    assert codes(check_arguments(CONNECTOR, insights, {"id": "act_1", "params": "level=ad"})) == [
        ("bad-value", "arguments.params")]


def test_the_api_version_is_a_graph_api_version():
    with pytest.raises(InvalidConnectionError, match="api_version: '26' does not match"):
        resolve_values(CONNECTOR, {"access_token": "t", "api_version": "26"})
    assert resolve_values(CONNECTOR, {"access_token": "t", "api_version": "v26.0"})["api_version"] == "v26.0"


def _graph_error(code, subcode, transient=False):
    body = {"error": {"message": "slow down", "code": code, "error_subcode": subcode, "is_transient": transient}}
    return client.translate_error(FacebookRequestError("slow down", {}, 400, {}, body))


def test_retry_codes_are_the_specs_and_a_stream_replaces_them():
    error = _graph_error(17, 2446079)
    assert error.codes == (17, 2446079) and not error.retryable
    assert ConnectorContext("meta_ads", str, default_codes=CONNECTOR.retry_codes).retryable(error)
    replaced = ConnectorContext("meta_ads", str, retry={"codes": [2446079]}, default_codes=CONNECTOR.retry_codes)
    assert replaced.retryable(error) and not replaced.retryable(_graph_error(613, None, transient=True))
