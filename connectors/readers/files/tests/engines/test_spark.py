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
