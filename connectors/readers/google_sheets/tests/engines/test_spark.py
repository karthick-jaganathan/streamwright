"""The google_sheets Spark adapter: driver-side reads, pinned for replay, with no credentials in Spark's settings."""

from types import MappingProxyType, SimpleNamespace
from unittest.mock import MagicMock

import pytest
import responses

from streamwright.connectors.google_sheets import CONNECTOR, client
from streamwright.connectors.google_sheets.engines.spark import GoogleSheetsSparkAdapter
from streamwright.core.sdk import ConnectorError
from streamwright.core.sdk.contract import check_arguments, resolve_values
from streamwright.core.sdk.testing import certify
from streamwright.spark.io.capabilities import CAPABILITIES
from streamwright.spark.sdk import SparkContext

TestSparkCertification = certify(CONNECTOR, (CAPABILITIES,))
READ = CONNECTOR.operation("spreadsheet", "read")
VALUES = "%s/s1/values/%%27T1%%27" % client.SHEETS_API_BASE
ARGUMENTS = MappingProxyType({"spreadsheet_id": "s1", "sheet": "T1"})


def context(pinned):
    runtime = SimpleNamespace(spark=MagicMock(), pinned=pinned)
    return SparkContext(runtime, "targets", resolve_values(CONNECTOR, {"api_key": "key-0042"}))


def test_spark_takes_the_same_arguments_and_holds_no_hadoop_settings():
    assert check_arguments(CONNECTOR, READ, {"sheet": "T1", "header_row": 1}, CAPABILITIES) == []
    assert GoogleSheetsSparkAdapter().hadoop_settings("sheets", {"api_key": "key-0042"}) == {}


@responses.activate
def test_the_driver_fetches_once_and_pins_the_records_for_replay():
    responses.add(responses.GET, VALUES, json={"values": [["a", "b"], ["1", "x"]]})
    pinned, adapter = {}, GoogleSheetsSparkAdapter()
    first = context(pinned)
    adapter.read(first, READ, ARGUMENTS)
    assert pinned == {"targets.*": [{"a": "1", "b": "x"}]}
    first.spark.createDataFrame.assert_called_once_with([{"a": "1", "b": "x"}])

    replay = context(pinned)
    adapter.read(replay, READ, ARGUMENTS)
    assert len(responses.calls) == 1  # the replay reads the pinned records
    replay.spark.createDataFrame.assert_called_once_with([{"a": "1", "b": "x"}])


@responses.activate
def test_an_empty_sheet_is_an_empty_frame():
    responses.add(responses.GET, VALUES, json={"values": []})
    spark_context = context({})
    GoogleSheetsSparkAdapter().read(spark_context, READ, ARGUMENTS)
    (records, schema), _ = spark_context.spark.createDataFrame.call_args
    assert records == [] and len(schema) == 0


@responses.activate
def test_api_errors_stop_the_step_without_showing_the_key():
    responses.add(responses.GET, VALUES, status=403, json={"error": "denied"})
    with pytest.raises(ConnectorError, match="authentication/permission denied") as caught:
        GoogleSheetsSparkAdapter().read(context({}), READ, ARGUMENTS)
    assert "key-0042" not in str(caught.value)


def test_the_connector_is_read_only():
    with pytest.raises(ConnectorError, match="read-only"):
        GoogleSheetsSparkAdapter().write(context({}), None, READ, {})
