"""Contract and security checks for the composable s3 Spark adapter."""

from types import SimpleNamespace

import pytest

from streamwright.connectors.s3 import CONNECTOR, client
from streamwright.connectors.s3.engines import spark as spark_module
from streamwright.core.sdk.contract import check_arguments
from streamwright.core.sdk.testing import certify
from streamwright.spark.io.files import CAPABILITIES, FileLocation
from streamwright.spark.sdk import SparkContext


def test_s3_settings_are_scoped_and_never_contain_credentials():
    adapter = spark_module.S3SparkAdapter()
    connection = {
        "roots": ("s3://landing/events/", "s3://archive/events/"),
        "endpoint": "storage.example.com:9000", "url_style": "path", "use_ssl": False,
        "region": "us-east-1",
    }
    settings = adapter.hadoop_settings("landing", connection)
    assert settings["fs.s3a.bucket.landing.endpoint"] == "storage.example.com:9000"
    assert settings["fs.s3a.bucket.archive.path.style.access"] == "true"
    assert settings["fs.s3a.bucket.landing.connection.ssl.enabled"] == "false"
    assert settings["fs.s3a.bucket.archive.endpoint.region"] == "us-east-1"
    assert not any(secret in str(settings) for secret in ("key-private", "secret-private"))
    assert all(key.startswith("fs.s3a.bucket.") for key in settings)
    with pytest.raises(ValueError, match="credential chain") as error:
        adapter.hadoop_settings("landing", dict(connection, key_id="key-private", secret="secret-private"))
    assert "secret-private" not in str(error.value)


def test_read_write_delegate_to_file_helpers_with_authorized_roots(monkeypatch):
    seen = []

    def fake_read(context, arguments, *, location):
        seen.append(("read", context.name, arguments, location))
        return "frame"

    def fake_write(context, frame, arguments, *, location):
        seen.append(("write", context.name, frame, arguments, location))
        return "receipt"

    monkeypatch.setattr(spark_module, "read_files", fake_read)
    monkeypatch.setattr(spark_module, "write_files", fake_write)
    connection = {"roots": ("s3://landing/events/",)}
    runtime = SimpleNamespace(spark="spark")
    adapter = spark_module.S3SparkAdapter()
    assert adapter.read(SparkContext(runtime, "raw", connection), None,
                        {"path": "s3://landing/events/1.csv"}) == "frame"
    assert adapter.write(SparkContext(runtime, "out", connection), "frame", None,
                         {"path": "s3://landing/events/out/", "format": "csv"}) == "receipt"
    assert seen[0][3] == seen[1][4] == FileLocation(("s3://landing/events/",), "s3", "s3a", client.hadoop_url)
    assert seen[0][3].to_hadoop("s3://landing/events/1.csv") == "s3a://landing/events/1.csv"


TestSparkCertification = certify(CONNECTOR, (CAPABILITIES,))


def test_spark_takes_its_arguments_and_write_modes():
    read, write = CONNECTOR.operation("object", "read"), CONNECTOR.operation("object", "write")
    assert check_arguments(CONNECTOR, read, {"path": "a.csv", "row_errors": True}, CAPABILITIES) == []
    issues = check_arguments(CONNECTOR, read, {"path": "a.csv", "match": ".*"}, CAPABILITIES)
    assert [(issue.code, issue.message) for issue in issues] == [
        ("unsupported-argument", "`match` is not supported by the spark engine")]
    assert check_arguments(CONNECTOR, write, {"path": "out/", "format": "tsv", "mode": "overwrite"}, CAPABILITIES) == []
