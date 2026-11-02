"""The files Spark adapter (kind: SparkJob): roots, `file://` URLs, no settings."""

from types import MappingProxyType

from streamwright.connectors.files import CONNECTOR
from streamwright.spark.io.files import FileLocation

CONNECTION = MappingProxyType({"roots": ("/var/data/raw/", "/var/data/curated/")})


def adapter():
    return CONNECTOR.load_adapter("spark")


def test_native_file_urls_need_no_jar_and_no_hadoop_setting():
    assert adapter().hadoop_settings("raw", CONNECTION) == {} and adapter().jars == ()


def test_the_location_names_files_by_url_for_spark_and_by_path_for_the_job():
    location = adapter().location(CONNECTION)
    assert isinstance(location, FileLocation)
    assert location.to_hadoop("/var/data/raw/a.csv") == "file:///var/data/raw/a.csv"
    assert location.resolve_path("a.csv") == "/var/data/raw/a.csv"
    assert location.resolve_path("/var/data/curated/b.csv") == "/var/data/curated/b.csv"
