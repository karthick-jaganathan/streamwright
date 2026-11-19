#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

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


def test_an_azure_connection_string_reaches_delta_rs_as_its_parts():
    text = "DefaultEndpointsProtocol=http;AccountName=dev;AccountKey=k3y==;BlobEndpoint=http://azurite:10000/dev"
    assert client.storage_options({"azure_connection_string": text}) == {
        "AZURE_STORAGE_ACCOUNT_NAME": "dev", "AZURE_STORAGE_ACCOUNT_KEY": "k3y==",
        "AZURE_ENDPOINT": "http://azurite:10000/dev", "AZURE_ALLOW_HTTP": "true"}
    assert client.storage_options({"azure_connection_string": "AccountName=a;AccountKey=k",
                                   "azure_account_name": "b"})["AZURE_STORAGE_ACCOUNT_NAME"] == "b"


def test_duckdb_reads_gcs_with_an_hmac_key_and_delta_rs_writes_with_the_key_file():
    hmac = {"gcs_hmac_key_id": "GOOG1E", "gcs_hmac_secret": "s"}
    assert client.duckdb_problem({"roots": ["gs://b/"]}) is None
    assert client.duckdb_problem({"gcs_key_file": "/k.json"}).startswith("gcs_key_file: DuckDB reads")
    assert client.duckdb_problem(dict(hmac, gcs_key_file="/k.json")) is None
    secrets = client.storage_secrets(hmac, ["gs://a/", "gcs://b/", "s3://c/"])
    assert [(item.secret_type, item.extension, item.parameters, item.scopes) for item in secrets] == [
        ("gcs", "httpfs", (("KEY_ID", "GOOG1E"), ("SECRET", "s")), ("gs://a/", "gcs://b/"))]
    assert client.storage_options(dict(hmac, gcs_key_file="/k.json")) == {"GOOGLE_SERVICE_ACCOUNT": "/k.json"}


@pytest.mark.parametrize("given", [{"gcs_hmac_key_id": "GOOG1E"}, {"gcs_hmac_secret": "s", "gcs_hmac_key_id": ""}])
def test_the_hmac_key_id_and_secret_go_together(given):
    assert client.validate_connection(dict(given, roots=["gs://b/"])) == [
        "gcs_hmac_key_id and gcs_hmac_secret go together (the HMAC key DuckDB reads GCS with)"]
    assert client.validate_connection({"roots": ["gs://b/"], "gcs_hmac_key_id": "a", "gcs_hmac_secret": "b"}) == []


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
