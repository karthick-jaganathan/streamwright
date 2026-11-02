import importlib
import os

import pytest

from streamwright.core.runtime.testing import FakeApi

# No dlt telemetry from tests: its request would reach the `responses` mocks of the HTTP tests.
os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "false"


@pytest.fixture
def api():
    """A local fake HTTP API (streamwright.core.runtime.testing.FakeApi)."""
    with FakeApi() as server:
        yield server


@pytest.fixture
def dlt_output():
    """streamwright.core.outputs.dlt_output, which imports the optional dlt library (the test skips without it)."""
    pytest.importorskip("dlt")
    return importlib.import_module("streamwright.core.outputs.dlt_output")
