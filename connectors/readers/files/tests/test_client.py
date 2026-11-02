"""The files vendor rules every engine shares."""

from streamwright.connectors.files import client


def test_roots_are_local_folders():
    assert client.validate_connection({"roots": ["/data", "relative/folder"]}) == []
    problems = client.validate_connection({"roots": ["/data", "  ", 3]})
    assert [problem.split(":")[0] for problem in problems] == ["roots[1]", "roots[2]"]
    assert client.validate_connection({"roots": []}) == ["roots: name at least one folder"]


def test_spark_names_a_local_path_by_its_file_url():
    assert client.hadoop_url("/data/a.csv") == "file:///data/a.csv"
    assert client.hadoop_url("data/a.csv") == "file://data/a.csv"
    assert client.hadoop_url("file:///data/a.csv") == "file:///data/a.csv"
