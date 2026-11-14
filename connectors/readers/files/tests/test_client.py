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
