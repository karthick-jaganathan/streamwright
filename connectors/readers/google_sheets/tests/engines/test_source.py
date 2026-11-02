"""The google_sheets source adapter (streamwright run), against the Sheets API served by `responses`."""

import os
from types import MappingProxyType

import pytest
import responses

from streamwright.connectors.google_sheets import CONNECTOR, client
from streamwright.core.sdk import ConnectorContext, ConnectorError, Redactor
from streamwright.core.sdk.contract import resolve_values
from streamwright.core.sdk.testing import MemoryOutput, SourceAdapterContract, SourceRunner, load_source

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))
EXAMPLE = os.path.join(REPO_ROOT, "examples", "sources", "readers", "google_sheets_demo")
BASE = client.SHEETS_API_BASE
ROWS = (("campaign_id", "owner"), ("c1", "ada"), ("c2", "lin"))


def serve_budgets(status=200, **kwargs):
    responses.add(responses.GET, "%s/sheet-1/values/%%27Budgets%%27" % BASE, status=status,
                  **(kwargs or {"json": {"values": ROWS}}))


class TestSourceAdapter(SourceAdapterContract):
    __test__ = True
    definition = CONNECTOR
    connection = MappingProxyType({"api_key": "key-0042"})
    read_call = (("spreadsheet", "read"), MappingProxyType({"spreadsheet_id": "sheet-1", "sheet": "Budgets"}))
    expected_records = ({"campaign_id": "c1", "owner": "ada"}, {"campaign_id": "c2", "owner": "lin"})

    @pytest.fixture
    def backend(self):
        with responses.RequestsMock() as mock:
            mock.add(responses.GET, "%s/sheet-1/values/%%27Budgets%%27" % BASE, json={"values": ROWS})
            yield mock


@responses.activate
def test_the_example_runs_and_the_key_never_reaches_the_url():
    serve_budgets()
    responses.add(responses.GET, "%s/sheet-1?fields=sheets.properties" % BASE, json={"sheets": []})
    source = load_source(EXAMPLE)
    source["auth"] = {"provider": "google_sheets", "api_key": "{{ secrets.gcp_service_account_json }}",
                      "spreadsheet_id": "{{ config.spreadsheet_id }}"}
    output = MemoryOutput()
    SourceRunner(source, {"spreadsheet_id": "sheet-1"}, {"gcp_service_account_json": "AIzaSecretKey"},
                 output=output).run()
    assert [record["campaign_id"] for name, record in output.records if name == "budgets"] == ["c1", "c2"]
    assert all(call.request.headers["x-goog-api-key"] == "AIzaSecretKey" and "AIzaSecretKey" not in call.request.url
               for call in responses.calls)


@responses.activate
def test_rate_limits_are_retried_by_the_stream_context_and_other_failures_are_not():
    sleeps = []
    responses.add(responses.GET, "%s/sheet-1/values/%%27Budgets%%27" % BASE, status=429, json={"error": "quota"})
    serve_budgets()
    context = ConnectorContext(CONNECTOR.name, Redactor([]), sleep=sleeps.append)
    session = CONNECTOR.load_adapter("source").open(resolve_values(CONNECTOR, {"api_key": "k"}), context)
    arguments = {"spreadsheet_id": "sheet-1", "sheet": "Budgets"}
    assert [r["owner"] for page in session.read(CONNECTOR.operation("spreadsheet", "read"), arguments, context)
            for r in page] == ["ada", "lin"]
    assert len(sleeps) == 1

    responses.replace(responses.GET, "%s/sheet-1/values/%%27Budgets%%27" % BASE, status=403, json={"error": "no"})
    with pytest.raises(ConnectorError, match="authentication/permission denied") as caught:
        list(session.read(CONNECTOR.operation("spreadsheet", "read"), arguments, context))
    assert caught.value.retryable is False
