"""The google_sheets spec: certified (source and Spark), and the generic checks it drives."""

from streamwright.connectors.google_sheets import CONNECTOR
from streamwright.core.io.capabilities import ENGINE_CAPABILITIES
from streamwright.core.sdk.contract import check_arguments, check_connection
from streamwright.core.sdk.testing import certify
from streamwright.spark.io.capabilities import CAPABILITIES as SPARK

TestCertification = certify(CONNECTOR, (*ENGINE_CAPABILITIES, SPARK))


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_every_credential_is_a_secret_reference():
    assert check_connection(CONNECTOR, {"api_key": "{{ secrets.key }}", "spreadsheet_id": "{{ config.sheet }}"}) == []
    assert CONNECTOR.describe()["secret_keys"] == [
        "service_account_json", "credentials", "access_token", "refresh_token", "client_id", "client_secret",
        "api_key"]
    assert codes(check_connection(CONNECTOR, {"api_key": "AIzaWrittenInTheFile"})) == [("literal-secret", "api_key")]


def test_the_source_and_spark_engines_run_the_same_three_reads():
    assert sorted(CONNECTOR.engines) == ["source", "spark"]
    assert [operation.name for operation in CONNECTOR.operations] == [
        "spreadsheet.read", "spreadsheet.get_values", "spreadsheet.list_sheets"]
    for capabilities in (ENGINE_CAPABILITIES[0], SPARK):
        assert len(CONNECTOR.operations_for(capabilities)) == 3


def test_arguments_are_checked():
    get_values = CONNECTOR.operation("spreadsheet", "get_values")
    assert codes(check_arguments(CONNECTOR, get_values, {})) == [("missing-key", "arguments.range")]
    assert check_arguments(CONNECTOR, get_values, {"range": "A1:B2", "header_row": 1}) == []
    read = CONNECTOR.operation("spreadsheet", "read")
    assert check_arguments(CONNECTOR, read, {"key": 1}) == []  # a vendor values.get param now
    assert codes(check_arguments(CONNECTOR, read, {"batch_size": "many", "value_render_option": "RAW"})) == [
        ("bad-value", "arguments.batch_size"), ("bad-value", "arguments.value_render_option")]
    assert check_arguments(CONNECTOR, read, {"header_row": "{{ config.row }}"}) == []
