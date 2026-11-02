"""Contract and security checks for the gcs Spark adapter."""

from types import MappingProxyType

import pytest

from streamwright.connectors.gcs import CONNECTOR
from streamwright.core.sdk.testing import certify
from streamwright.spark.io.files import CAPABILITIES, FileLocation

TestSparkCertification = certify(CONNECTOR, (CAPABILITIES,))
CONNECTION = MappingProxyType({"roots": ("gs://landing/events/", "gs://archive/events/")})


def adapter():
    return CONNECTOR.load_adapter("spark")


def test_settings_point_s3a_at_gcs_per_bucket_and_never_hold_credentials():
    settings = adapter().hadoop_settings("landing", CONNECTION)
    assert settings == {
        "fs.s3a.bucket.archive.endpoint": "storage.googleapis.com", "fs.s3a.bucket.archive.path.style.access": "true",
        "fs.s3a.bucket.landing.endpoint": "storage.googleapis.com", "fs.s3a.bucket.landing.path.style.access": "true"}
    assert "None" not in str(settings)
    with pytest.raises(ValueError, match="platform's credentials") as error:
        adapter().hadoop_settings("landing", dict(CONNECTION, key_id="key-private", secret="secret-private"))
    assert "secret-private" not in str(error.value)


def test_the_location_translates_gs_urls_to_s3a():
    location = adapter().location(CONNECTION)
    assert isinstance(location, FileLocation)
    assert location.to_hadoop("gs://landing/events/1.csv") == "s3a://landing/events/1.csv"
    assert location.source_file is not None
    assert location.resolve_path("1.csv") == "gs://landing/events/1.csv"
