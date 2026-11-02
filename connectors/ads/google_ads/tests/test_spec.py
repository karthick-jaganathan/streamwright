"""The google_ads spec: certified, read-only, and its query text only from a query builder."""

from types import MappingProxyType

import pytest

from streamwright.connectors.google_ads import CONNECTOR, client
from streamwright.core.engine.queries import BuiltQuery
from streamwright.core.runtime.source_connectors import check_call
from streamwright.core.sdk.contract import InvalidConnectionError, check_arguments, check_connection, resolve_values
from streamwright.core.sdk.engines.source import ConnectorContext
from streamwright.core.sdk.testing import certify, components, load_source

TestCertification = certify(CONNECTOR)

DECLARED = MappingProxyType({"developer_token": "{{ secrets.dev }}", "client_id": "{{ secrets.cid }}",
            "client_secret": "{{ secrets.cs }}", "refresh_token": "{{ secrets.rt }}"})


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_the_credentials_are_secret_references():
    assert check_connection(CONNECTOR, DECLARED) == []
    assert CONNECTOR.describe()["secret_keys"] == ["developer_token", "client_id", "client_secret", "refresh_token"]
    assert codes(check_connection(CONNECTOR, dict(DECLARED, refresh_token="1//written"))) == [
        ("literal-secret", "refresh_token")]
    assert codes(check_connection(CONNECTOR, {"client_id": "{{ secrets.cid }}"})) == [
        ("missing-key", "developer_token"), ("missing-key", "client_secret"), ("missing-key", "refresh_token")]


def test_only_read_only_calls_are_offered():
    assert [operation.name for operation in CONNECTOR.operations] == [
        "GoogleAdsService.search_stream", "GoogleAdsService.search", "*Service.get_*", "*Service.list_*"]
    connector = components.load("google_ads")
    # a mutate matches no read family, so it is refused before the SDK is even consulted
    assert check_call(connector, {"service": "CampaignService", "method": "mutate_campaigns"}) == [
        "google_ads: CampaignService.mutate_campaigns is not supported (supported: GoogleAdsService.search_stream, "
        "GoogleAdsService.search, *Service.get_*, *Service.list_*)"]
    # list_accessible_customers is now covered by the *Service.list_* family (checked against the installed SDK)
    assert check_call(connector, {"service": "CustomerService", "method": "list_accessible_customers"}) == []


def test_a_typo_is_caught_against_the_installed_sdk_before_a_run():
    connector = components.load("google_ads")
    (problem,) = check_call(connector, {"service": "CustomerService", "method": "list_nonexistent"})
    assert "CustomerService has no list_nonexistent method" in problem
    (problem,) = check_call(connector, {"service": "MisspelledService", "method": "get_thing"})
    assert "MisspelledService is not a service" in problem


def test_query_text_comes_from_the_query_builder_not_from_references():
    search = CONNECTOR.operation("GoogleAdsService", "search_stream")
    arguments = {"customer_id": "123-456-7890", "query": "SELECT campaign.id FROM campaign"}
    assert check_arguments(CONNECTOR, search, arguments) == []
    assert check_arguments(CONNECTOR, search, dict(arguments, query=BuiltQuery("gaql"))) == []
    inline = check_arguments(CONNECTOR, search, dict(arguments, query="SELECT x WHERE n = '{{ config.name }}'"))
    assert codes(inline) == [("bad-value", "arguments.query")]
    assert "query builder" in inline[0].message
    assert codes(check_arguments(CONNECTOR, search, {"customer_id": "1"})) == [("missing-key", "arguments.query")]
    # page_size is a vendor search-request field now, forwarded to the Google Ads request
    assert check_arguments(CONNECTOR, search, dict(arguments, page_size=10)) == []
    assert codes(check_arguments(CONNECTOR, search, dict(arguments, customer_id="12a"))) == [
        ("bad-value", "arguments.customer_id")]


def test_the_example_source_is_valid_and_references_in_its_query_are_refused():
    source = load_source("examples/sources/ads/google_ads")
    assert components.check_source(source) == []
    request = next(stream for stream in source["streams"] if stream["name"] == "campaign_performance")["requests"][0]
    request["arguments"]["query"] = "SELECT campaign.id FROM campaign WHERE campaign.name = '{{ config.name }}'"
    problems = components.check_source(source)
    assert len(problems) == 1 and "cannot contain `{{ }}` references" in problems[0]


def test_resolving_an_unsupported_api_version_names_the_versions():
    values = dict.fromkeys(("developer_token", "client_id", "client_secret", "refresh_token"), "x")
    with pytest.raises(InvalidConnectionError, match="api_version 'v1' is not supported by the installed google-ads"):
        resolve_values(CONNECTOR, dict(values, api_version="v1"))


def test_retry_codes_are_the_specs_and_a_stream_replaces_them():
    error = client._connector_error("RESOURCE_EXHAUSTED", None, None, "quota exhausted")
    assert error.codes == ("RESOURCE_EXHAUSTED",) and not error.retryable  # the codes decide, not the client
    assert ConnectorContext("google_ads", str, default_codes=CONNECTOR.retry_codes).retryable(error)
    assert not ConnectorContext("google_ads", str, retry={"codes": ["INTERNAL"]},
                                default_codes=CONNECTOR.retry_codes).retryable(error)
