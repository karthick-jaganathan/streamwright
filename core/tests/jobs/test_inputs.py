from types import MappingProxyType

import pytest

from streamwright.core.jobs.inputs import InputError, effective_config, job_inputs, parse_set, secret_values
from streamwright.core.jobs.validate import resolve_for_checks

JOB = MappingProxyType({"spec": {
    "config": {"day": {"type": "date"}, "limit": {"type": "integer", "default": 10},
               "tags": {"type": "list", "items": "string", "required": False}},
    "secrets": {"token": {"type": "string"}}}})


def test_parse_set_rejects_items_without_a_name_or_equals():
    assert parse_set(["a=1", " b = x=y "]) == {"a": "1", "b": "x=y"}
    for bad in (["a"], ["=1"]):
        with pytest.raises(InputError):
            parse_set(bad)


def test_config_values_are_typed_required_and_declared():
    values = effective_config(JOB, {"day": "2026-11-01", "limit": "5"})
    assert values["limit"] == 5 and str(values["day"]) == "2026-11-01" and values["tags"] is None
    with pytest.raises(InputError, match="missing required config 'day'"):
        effective_config(JOB, {})
    with pytest.raises(InputError, match="unknown config: typo"):
        effective_config(JOB, {"day": "today", "typo": 1})
    with pytest.raises(InputError, match="must be a mapping"):
        effective_config({"spec": {"config": {"x": 50.0}}})


def test_secrets_come_from_values_then_the_environment_and_never_show_values():
    assert secret_values(JOB, environ={"STREAMWRIGHT_SECRET_TOKEN": "env"}) == {"token": "env"}
    assert secret_values(JOB, {"token": "file"}, environ={"STREAMWRIGHT_SECRET_TOKEN": "env"}) == {"token": "file"}
    with pytest.raises(InputError) as caught:
        secret_values(JOB, {"token": "s3cr3t", "other": "s3cr3t"}, environ={})
    assert "s3cr3t" not in str(caught.value)


def test_a_config_file_with_streams_is_rejected(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text("config: {day: today}\nstreams: [a]\n")
    with pytest.raises(InputError, match="`streams` does not apply"):
        job_inputs(JOB, config_file=str(path), environ={})
    path.write_text("config: {day: today, limit: 3}\n")
    config, secrets = job_inputs(JOB, config_file=str(path), set_items=["limit=4"],
                                 environ={"STREAMWRIGHT_SECRET_TOKEN": "t"})
    assert config["limit"] == 4 and secrets == {"token": "t"}


def test_validation_keeps_a_required_config_input_without_a_value_as_a_reference():
    job = dict(JOB, steps=[{"name": "s", "sql": "SELECT '{{ config.day }}', {{ config.limit }}"}])
    resolved, problems = resolve_for_checks(job)
    assert problems == [] and resolved["steps"][0]["sql"] == "SELECT '{{ config.day }}', 10"
    with pytest.raises(InputError, match="config 'limit'"):
        resolve_for_checks(job, {"limit": "many"})
