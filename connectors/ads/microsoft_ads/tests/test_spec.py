"""The microsoft_ads spec: certified, read-only, and the connection and call arguments it takes."""

from types import MappingProxyType

import pytest
from bingads.exceptions import OAuthTokenRequestException

from streamwright.connectors.microsoft_ads import CONNECTOR, client
from streamwright.core.runtime.source_connectors import check_call
from streamwright.core.sdk.contract import InvalidConnectionError, check_arguments, check_connection, resolve_values
from streamwright.core.sdk.engines.source import ConnectorContext
from streamwright.core.sdk.testing import certify, components, load_source

TestCertification = certify(CONNECTOR)

DECLARED = MappingProxyType({"developer_token": "{{ secrets.dev }}", "client_id": "{{ secrets.cid }}",
            "refresh_token": "{{ secrets.rt }}"})


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_the_example_source_is_valid():
    assert components.check_source(load_source("examples/sources/ads/microsoft_ads")) == []


def test_the_credentials_are_secret_references():
    assert check_connection(CONNECTOR, dict(DECLARED, client_secret="{{ secrets.cs }}")) == []
    assert CONNECTOR.describe()["secret_keys"] == ["developer_token", "client_id", "refresh_token", "client_secret"]
    assert codes(check_connection(CONNECTOR, dict(DECLARED, client_secret="written"))) == [
        ("literal-secret", "client_secret")]


def test_the_environment_is_production_by_default_or_sandbox():
    assert resolve_values(CONNECTOR, {"developer_token": "d", "client_id": "c", "refresh_token": "r"})[
        "environment"] == "production"
    with pytest.raises(InvalidConnectionError, match="environment: expected one of: production, sandbox"):
        resolve_values(CONNECTOR, {"developer_token": "d", "client_id": "c", "refresh_token": "r",
                                   "environment": "staging"})


def test_reads_are_families_and_the_installed_wsdl_decides_which_operations_exist():
    names = {operation.name for operation in CONNECTOR.operations}
    assert names == {"*Service.Get*", "*Service.Search*", "*Service.Find*",
                     "ReportingService.SubmitGenerateReport", "ReportingService.PollGenerateReport"}
    assert not [name for name in names if ".Delete" in name or ".Update" in name or ".Add" in name]
    connector = components.load("microsoft_ads")
    # a mutation matches no read family, so it is refused before the SDK is even consulted
    (problem,) = check_call(connector, {"service": "CampaignManagementService", "method": "DeleteCampaigns"})
    assert problem.startswith("microsoft_ads: CampaignManagementService.DeleteCampaigns is not supported (supported: ")
    # a read of any service is allowed by policy; the installed bingads WSDL decides at run time whether it exists
    assert check_call(connector, {"service": "CampaignManagementService", "method": "GetCampaignsByAccountId"}) == []
    assert check_call(connector, {"service": "AdInsightService", "method": "GetKeywordIdeas"}) == []


def test_a_call_takes_the_soap_request_and_the_account_and_customer_of_its_headers():
    operation = CONNECTOR.operation("CampaignManagementService", "GetAdGroupsByCampaignId")
    arguments = {"request": {"CampaignId": 1}, "account_id": 7, "customer_id": "9"}
    assert check_arguments(CONNECTOR, operation, arguments) == []
    assert check_arguments(CONNECTOR, operation, {"CampaignId": 1}) == []  # a vendor request field now
    assert codes(check_arguments(CONNECTOR, operation, {"request": ["CampaignId"]})) == [
        ("bad-value", "arguments.request")]


def test_retry_codes_are_the_specs_and_a_stream_replaces_them():
    error = client.translate_error(OAuthTokenRequestException("server_error", "try later"))
    assert error.codes == ("server_error",) and not error.retryable
    assert ConnectorContext("microsoft_ads", str, default_codes=CONNECTOR.retry_codes).retryable(error)
    assert not ConnectorContext("microsoft_ads", str, retry={"codes": [117]},
                                default_codes=CONNECTOR.retry_codes).retryable(error)
