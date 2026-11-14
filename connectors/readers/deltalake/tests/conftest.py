#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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

import os
import sys

import pytest

# inside the StreamWright repository: its streamwright/ folder would shadow the installed streamwright packages
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
sys.path[:] = [p for p in sys.path if os.path.abspath(p or os.curdir) != REPO_ROOT]


@pytest.fixture
def lake(tmp_path):
    """(inside, outside): folders each holding the sample table `cities`; only `inside` is a root."""
    deltalake = pytest.importorskip("deltalake")
    pa = pytest.importorskip("pyarrow")
    inside, outside = tmp_path / "lake", tmp_path / "elsewhere"
    for folder in (inside, outside):
        folder.mkdir()
        deltalake.write_deltalake(str(folder / "cities"), pa.table({
            "id": [1, 2, 3], "city": ["New York", "London", "Tokyo"], "population": [8300000, 8900000, 14000000]}))
    return str(inside), str(outside)
