"""Contract and security checks for the azure_blob Spark adapter."""

from types import MappingProxyType

import pytest

from streamwright.connectors.azure_blob import CONNECTOR
from streamwright.spark.io.files import FileLocation

CONNECTION = MappingProxyType({"roots": ("azure://container/in/", "abfss://other/"), "account_name": "acct"})


def adapter():
    return CONNECTOR.load_adapter("spark")


def test_the_platforms_credentials_sign_in_so_no_setting_is_needed_and_none_hold_a_secret():
    assert adapter().hadoop_settings("lake", CONNECTION) == {}
    for secret in ({"account_key": "key-SECRET"}, {"client_secret": "s-SECRET"},
                   {"connection_string": "AccountName=acct;AccountKey=key-SECRET"}):
        with pytest.raises(ValueError, match="platform's credentials") as error:
            adapter().hadoop_settings("lake", dict(CONNECTION, **secret))
        assert "SECRET" not in str(error.value)
    with pytest.raises(ValueError, match="account_name is required"):
        adapter().hadoop_settings("lake", {"roots": ("azure://c/",)})


def test_the_location_translates_blob_urls_to_abfs_and_back():
    location = adapter().location(CONNECTION)
    assert isinstance(location, FileLocation)
    assert location.to_hadoop("azure://container/in/a.csv") == "abfss://container@acct.dfs.core.windows.net/in/a.csv"
    assert location.to_hadoop("abfss://other/a.csv") == "abfss://other@acct.dfs.core.windows.net/a.csv"
    assert location.resolve_path("a.csv") == "azure://container/in/a.csv"
