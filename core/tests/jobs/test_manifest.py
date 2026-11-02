import pytest

from streamwright.core.jobs.manifest import MAX_MANIFEST_BYTES, JobError, load_job, load_job_text, merge_overlay


def test_a_folder_means_its_job_yaml_or_job_yml(tmp_path):
    (tmp_path / "job.yml").write_text("kind: DuckDBJob\nname: a\n")
    assert load_job(str(tmp_path))["name"] == "a"
    (tmp_path / "empty").mkdir()
    with pytest.raises(JobError, match=r"has no job\.yaml or job\.yml"):
        load_job(str(tmp_path / "empty"))


def test_overlays_merge_mappings_and_null_deletes(tmp_path):
    (tmp_path / "job.yaml").write_text("name: a\nruntime: {threads: 2, memory_limit: 1GB}\nsteps: [1, 2]\n")
    (tmp_path / "o.yaml").write_text("runtime: {threads: 4, memory_limit: null}\nsteps: [3]\n")
    job = load_job(str(tmp_path / "job.yaml"), overlays=[str(tmp_path / "o.yaml")])
    assert job == {"name": "a", "runtime": {"threads": 4}, "steps": [3]}
    assert merge_overlay({"a": {"b": 1}}, {"a": None}) == {}


def test_yaml_errors_give_the_position_but_never_the_text():
    with pytest.raises(JobError) as caught:
        load_job_text("name: a\npassword: [hunter2\n")
    assert "line 3" in str(caught.value) and "hunter2" not in str(caught.value)


def test_size_limit_and_mapping_required(tmp_path):
    big = tmp_path / "big.yaml"
    big.write_text("a: '%s'\n" % ("x" * MAX_MANIFEST_BYTES))
    with pytest.raises(JobError, match="larger than"):
        load_job(str(big))
    with pytest.raises(JobError, match="must be a mapping"):
        load_job_text("- 1\n")


def test_remote_locations_need_a_reader():
    with pytest.raises(JobError, match="no reader for s3://"):
        load_job("s3://bucket/job.yaml")
    read = {"mem": lambda location, limit: "name: %s\n" % location.split("/")[-1]}
    assert load_job("mem://x/remote", readers=read) == {"name": "remote"}
