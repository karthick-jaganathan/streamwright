import pytest

from streamwright.core.jobs.ingest import (
    IngestError,
    deserialize_plan,
    normalize_row_index,
    plan_ingest,
    serialize_plan,
)


def test_several_path_groups_need_an_explicit_selector():
    rules = {"acme": [{"header": "Id", "canonical": "id", "type": "int"}]}
    with pytest.raises(IngestError, match=r"columns\.by"):
        plan_ingest(["/in/acme/2026/a.csv"], mappings=rules, headers={"/in/acme/2026/a.csv": ["Id"]},
                    path_pattern=r"/in/(?P<partner>[^/]+)/(?P<year>\d+)/")
    rules["by"] = "partner"
    groups, rejects, _ = plan_ingest(["/in/acme/2026/a.csv", "/in/zed/2026/b.csv"], mappings=rules,
                                     headers={"/in/acme/2026/a.csv": ["Id"], "/in/zed/2026/b.csv": ["Id"]},
                                     path_pattern=r"/in/(?P<partner>[^/]+)/(?P<year>\d+)/")
    assert [g.files for g in groups] == [["/in/acme/2026/a.csv"]]
    assert [r.reason for r in rejects] == ["unknown schema key 'zed' extracted from path"]


def test_plain_reads_apply_the_path_pattern_too():
    files = ["/in/a/x.csv", "/other/y.csv"]
    groups, rejects, _ = plan_ingest(files, headers={f: ["c"] for f in files}, formats=dict.fromkeys(files, "csv"),
                                     path_pattern=r"/in/(?P<part>[^/]+)/")
    assert groups[0].files == ["/in/a/x.csv"] and groups[0].path_vars_spec == ("/in/([^/]+)/", {"part": 1})
    assert [r.path for r in rejects] == ["/other/y.csv"]
    assert deserialize_plan(serialize_plan(groups, rejects, []))[0][0].path_vars_spec == groups[0].path_vars_spec


def test_strict_rejects_files_whose_header_differs_and_options_are_checked():
    files = ["a.csv", "b.csv"]
    groups, rejects, _ = plan_ingest(files, headers={"a.csv": ["x", "y"], "b.csv": ["y", "x"]},
                                     formats={"a.csv": "csv", "b.csv": "csv"}, schema_mode="strict")
    assert groups[0].files == ["a.csv"] and rejects[0].path == "b.csv"
    with pytest.raises(IngestError):
        plan_ingest(files, schema_mode="loose")
    with pytest.raises(IngestError):
        plan_ingest(files, on_unmapped_column="drop")


def test_row_index_forms():
    assert normalize_row_index(False) is None and normalize_row_index(True) == ("_row_index", 0)
    assert normalize_row_index({"name": "n", "start": 1}) == ("n", 1)
    with pytest.raises(IngestError):
        normalize_row_index(3)
