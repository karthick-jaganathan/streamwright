"""The Delta Lake facts every engine shares: roots, storage secrets and options, the query policy."""

import os

import pytest

from streamwright.connectors.deltalake import client
from streamwright.core.io.queries import check_query


def test_paths_resolve_inside_the_roots_only():
    roots = client.normalize_roots(["/data/lake", "S3://Bucket/delta/"])
    assert roots == ("/data/lake", "s3://Bucket/delta")
    assert client.resolve_path("cities", roots) == "/data/lake/cities"
    assert client.resolve_path("s3://Bucket/delta/t", roots) == "s3://Bucket/delta/t"
    for outside in ("/data/lakehouse/t", "/data/lake/../etc", "s3://other/t", ""):
        with pytest.raises(ValueError):
            client.resolve_path(outside, roots)
    assert client.normalize_roots(["lake"], "/jobs/one") == ("/jobs/one/lake",)
    assert client.normalize_roots(["lake"]) == (os.path.abspath("lake"),)


def test_storage_secrets_are_scoped_to_their_schemes_roots():
    connection = {"aws_access_key_id": "AKIA", "aws_secret_access_key": "s", "s3_use_ssl": False,
                  "azure_account_name": "acct", "azure_account_key": "k"}
    secrets = client.storage_secrets(connection, ["s3://a/", "abfss://c@acct/", "/local"])
    assert [(item.secret_type, item.scopes) for item in secrets] == [("s3", ("s3://a/",)),
                                                                     ("azure", ("abfss://c@acct/",))]
    assert secrets[0].parameters == (("KEY_ID", "AKIA"), ("SECRET", "s"), ("USE_SSL", False))
    assert client.storage_secrets({}, ["/local"]) == []
    assert client.masked_values({"azure_connection_string": "A=1;AccountKey=k3y==;B=2"}, ["x"]) == ["x", "k3y=="]


def test_plain_http_only_when_the_connection_says_so():
    assert "AWS_ALLOW_HTTP" not in client.storage_options({"s3_endpoint": "minio:9000"})
    assert client.storage_options({"s3_endpoint": "minio:9000", "s3_use_ssl": False}) == {
        "AWS_ENDPOINT_URL": "http://minio:9000", "AWS_ALLOW_HTTP": "true"}


def test_queries_call_delta_scan_only_on_a_literal_path_inside_the_roots_and_read_the_view():
    policy = client.query_policy(("/data/lake",))
    assert check_query("SELECT * FROM delta_scan('/data/lake/t') JOIN delta_table USING (id)", {}, policy)[1] == []
    for query, problem in (("SELECT * FROM delta_scan('/etc/t')", "outside the roots"),
                           ("SELECT * FROM delta_scan($p)", "literal path"),
                           ("SELECT * FROM read_parquet('/data/lake/x.parquet')", "cannot call read_parquet"),
                           ("SELECT * FROM other_table", None)):
        problems = check_query(query, {"p": 1} if "$p" in query else {}, policy)[1]
        assert (problem in " ".join(problems)) if problem else problems == []


def test_a_scan_query_binds_its_conditions():
    query, parameters = client.scan_query("/l/t", ["id"], [{"column": "city", "op": "=", "value": "x'y"}], 5)
    assert query == "SELECT \"id\" FROM delta_scan('/l/t') WHERE \"city\" = $w0 LIMIT 5" and parameters == {
        "w0": "x'y"}
