"""The s3 vendor rules every engine shares."""

from types import MappingProxyType

from streamwright.connectors.s3 import client
from streamwright.core.io.files import secret_parameters

CONNECTION = MappingProxyType({
    "roots": ["s3://bucket-b/x/", "s3://bucket-a/in/", "s3://bucket-a/raw"], "key_id": "AKIA", "secret": "s",
})


def test_valid_connections_have_no_problems():
    assert client.validate_connection(CONNECTION) == []
    assert client.validate_connection({"roots": ["s3://public-data/"]}) == []  # anonymous


def test_roots_must_be_s3_bucket_prefixes():
    problems = client.validate_connection({"roots": ["s3://a/in/?s3_endpoint=x", "s3://b/*.csv", "s3://c/../d/",
                                                     "s3://user@e/"]})
    assert [problem.split(" ", 1)[0] for problem in problems] == ["roots[0]:", "roots[1]:", "roots[2]:", "roots[3]:"]
    assert "has a `?`" in problems[0] and "is a glob" in problems[1] and "goes up a folder" in problems[2]
    assert client.validate_connection({"roots": []}) == ["roots: name at least one"]


def test_credentials_go_together():
    assert client.validate_connection({"roots": ["s3://a/"], "key_id": "AKIA"}) == [
        "key_id and secret go together (neither: anonymous access)"]
    assert client.validate_connection({"roots": ["s3://a/"], "session_token": "t"}) == [
        "session_token goes with key_id and secret"]


def test_facts_for_the_engines():
    assert client.buckets(CONNECTION) == ("bucket-a", "bucket-b")
    assert client.has_credentials(CONNECTION) and not client.has_credentials({"roots": []})
    assert client.uses_ssl({}) and not client.uses_ssl({"use_ssl": False})
    assert client.uses_path_style({"url_style": "path"}) and not client.uses_path_style({"url_style": "vhost"})
    assert client.hadoop_url("s3://bucket/key.csv") == "s3a://bucket/key.csv"
    assert client.hadoop_url("relative/key.csv") == "relative/key.csv"


def test_duckdb_secret_parameters_are_ordered_and_typed():
    parameters = secret_parameters(client.STORE, dict(CONNECTION, region="eu-west-1", use_ssl="false"))
    assert parameters == [("KEY_ID", "AKIA"), ("SECRET", "s"), ("REGION", "eu-west-1"), ("USE_SSL", False)]
