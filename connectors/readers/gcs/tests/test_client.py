"""The gcs vendor rules every engine shares."""

from types import MappingProxyType

from streamwright.connectors.gcs import client
from streamwright.core.io.files import secret_parameters

CONNECTION = MappingProxyType({"roots": ["gs://bucket-b/x/", "gs://bucket-a/in/", "gs://bucket-a/raw"],
                               "key_id": "GOOG", "secret": "s"})


def test_valid_connections_have_no_problems():
    assert client.validate_connection(CONNECTION) == []
    assert client.validate_connection({"roots": ["gs://public-data/"]}) == []  # anonymous


def test_roots_must_be_gs_bucket_prefixes():
    problems = client.validate_connection({"roots": ["gs://a/in/?x=1", "gs://b/*.csv", "gcs://c/", "s3://d/"]})
    assert [problem.split(" ", 1)[0] for problem in problems] == ["roots[0]:", "roots[1]:", "roots[2]:", "roots[3]:"]
    assert "has a `?`" in problems[0] and "is a glob" in problems[1] and "the gcs connector reads gs://" in problems[2]
    assert client.validate_connection({"roots": []}) == ["roots: name at least one"]


def test_the_key_goes_together():
    assert client.validate_connection({"roots": ["gs://a/"], "secret": "s"}) == [
        "key_id and secret go together (neither: anonymous access)"]


def test_facts_for_the_engines():
    assert client.buckets(CONNECTION) == ("bucket-a", "bucket-b")
    assert client.has_credentials(CONNECTION) and not client.has_credentials({"roots": []})
    assert client.hadoop_url("gs://bucket/key.csv") == "s3a://bucket/key.csv"
    assert client.hadoop_url("relative/key.csv") == "relative/key.csv"


def test_duckdb_secret_parameters_are_the_hmac_key_only():
    assert secret_parameters(client.STORE, CONNECTION) == [("KEY_ID", "GOOG"), ("SECRET", "s")]
