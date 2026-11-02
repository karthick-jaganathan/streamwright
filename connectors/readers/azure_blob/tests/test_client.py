"""The azure_blob vendor rules every engine shares."""

import pytest

from streamwright.connectors.azure_blob import client
from streamwright.core.io.files import secret_parameters

KEY = "dGVzdGtleQ==0042"
CONNECTION_STRING = ("DefaultEndpointsProtocol=https;AccountName=testacc;AccountKey=%s;"
                     "EndpointSuffix=core.windows.net;" % KEY)


def test_an_account_and_its_key_build_a_connection_string():
    assert secret_parameters(client.STORE, {"account_key": "mykey0042", "account_name": "myacc"}) == [
        ("CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=myacc;AccountKey=mykey0042;"
                              "EndpointSuffix=core.windows.net;")]
    text = secret_parameters(client.STORE, {"account_key": "mykey0042", "account_name": "myacc", "use_ssl": False,
                                            "endpoint": "127.0.0.1:10000"})[0][1]
    assert text.startswith("DefaultEndpointsProtocol=http;") and "BlobEndpoint=127.0.0.1:10000;" in text
    assert secret_parameters(client.STORE, {"connection_string": CONNECTION_STRING}) == [
        ("CONNECTION_STRING", CONNECTION_STRING)]


def test_a_service_principal_and_a_bare_account_are_the_other_combinations():
    connection = {"client_secret": "csecret0042", "account_name": "myacc", "tenant_id": "tid", "client_id": "cid"}
    assert secret_parameters(client.STORE, connection) == [
        ("PROVIDER", "service_principal"), ("TENANT_ID", "tid"), ("CLIENT_ID", "cid"),
        ("CLIENT_SECRET", "csecret0042"), ("ACCOUNT_NAME", "myacc")]
    assert secret_parameters(client.STORE, {"account_name": "myacc"}) == [("ACCOUNT_NAME", "myacc")]


def test_an_endpoint_cannot_inject_connection_string_parts():
    for endpoint in ("evil.example.com;AccountKey=other", "http://host;x=1", "http://user:pw@host/", "ftp://host",
                     "host name", "http://host/?q=1"):
        assert client.endpoint_problem(endpoint), endpoint
        assert client.validate_connection({"roots": ["azure://c/"], "account_name": "a", "endpoint": endpoint})
        with pytest.raises(ValueError, match="cannot build the connection string"):
            client.connection_string("acc", "key", endpoint)
    for endpoint in ("127.0.0.1:10000", "azurite:10000", "http://127.0.0.1:10000/devstoreaccount1",
                     "https://acc.blob.core.windows.net"):
        assert client.endpoint_problem(endpoint) is None, endpoint


def test_the_account_key_inside_a_connection_string_is_masked_on_its_own():
    connection = {"connection_string": CONNECTION_STRING}
    assert client.STORE.masked_values(connection) == [CONNECTION_STRING, CONNECTION_STRING, KEY]
    built = {"account_key": "mykey-0042-secret", "account_name": "acc"}
    assert "mykey-0042-secret" in client.STORE.masked_values(built)
    assert any(value.startswith("DefaultEndpointsProtocol=") for value in client.STORE.masked_values(built))


def test_the_connection_strings_parts_are_vendor_facts_the_other_adapters_reuse():
    assert (client.account_name({"connection_string": CONNECTION_STRING}),
            client.account_key({"connection_string": CONNECTION_STRING})) == ("testacc", KEY)
    assert client.account_name({"account_name": "x", "connection_string": CONNECTION_STRING}) == "x"
    assert client.hadoop_url("azure://c/dir/a.csv", "acc") == "abfss://c@acc.dfs.core.windows.net/dir/a.csv"
    assert client.hadoop_url("abfss://c/a.csv", "acc") == "abfss://c@acc.dfs.core.windows.net/a.csv"


def test_roots_must_be_container_prefixes():
    problems = client.validate_connection({"roots": ["azure://a/?x=1", "abfss://c@acct.dfs.core.windows.net/",
                                                     "s3://d/", "azure://e/*.csv"], "account_name": "a"})
    assert [problem.split(" ", 1)[0] for problem in problems] == ["roots[0]:", "roots[1]:", "roots[2]:", "roots[3]:"]
    assert client.validate_connection({"roots": ["azure://c/", "abfss://c/"], "account_name": "a"}) == []
