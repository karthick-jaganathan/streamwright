
import pytest

from streamwright.core.sdk.testing import FakeApi


@pytest.fixture
def api():
    """A local fake HTTP API (streamwright.core.runtime.testing.FakeApi)."""
    with FakeApi() as server:
        yield server
