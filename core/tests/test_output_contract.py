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

import io

import pytest

from streamwright.core.outputs.output import OUTPUT_FACTORIES, Output, SingerOutput, open_output


@pytest.mark.parametrize("kind", ["singer", "jsonl", "csv", "tsv", "parquet", "duckdb", "ducklake", "dlt"])
def test_builtin_outputs_implement_contract(kind, tmp_path):
    source = {"name": "contract", "streams": []}
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    if kind == "singer":
        output = SingerOutput(io.StringIO())
    elif kind == "dlt":
        pytest.importorskip("dlt")
        output = open_output("dlt:duckdb:contract", source=source)
    else:
        output = open_output("%s:%s" % (kind, tmp_path / "output"), source=source)
    try:
        assert isinstance(output, Output)
        output.write_columns("records", [("id", "INTEGER")])
        output.write_schema("records", schema, [])
        output.write_record("records", {"id": 1})
        output.mark_partial("records")
        output.write_state({"bookmarks": {}})
        assert output.summary()[0]["records"] == 1
    finally:
        output.close(failed=True)


def test_open_output_takes_other_output_kinds():
    expected = SingerOutput(io.StringIO())
    factories = {**OUTPUT_FACTORIES, "custom": lambda detail, source, file_names: expected}
    assert open_output("custom:destination", factories=factories) is expected
    with pytest.raises(ValueError, match="--output must be"):
        open_output("custom:destination")
