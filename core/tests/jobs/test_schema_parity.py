"""
Parity (TST-03): the generated JSON schema, the checker and the writers agree on every example and fixture.
A manifest the schema rejects is rejected by the checker; every example passes all three.
"""

import glob
import os
from types import MappingProxyType

import pytest

from streamwright.core.jobs.duckdb.engine import duckdb_rules, json_schema
from streamwright.core.jobs.manifest import load_job
from streamwright.core.jobs.validate import validate_job
from streamwright.core.jobs.write_rules import OutputSpec, check_output

jsonschema = pytest.importorskip("jsonschema")

EXAMPLES_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "..", "examples", "jobs")
# the job manifests (`<system>/<job>.yaml`); `<job>.<environment>.yaml` files are their overlays
EXAMPLES = sorted(path for path in glob.glob(os.path.join(EXAMPLES_ROOT, "*", "*.yaml"))
                  if "." not in os.path.basename(path).removesuffix(".yaml"))
assert EXAMPLES, "no example jobs under %s" % EXAMPLES_ROOT


def base_job():
    """A valid DuckDBJob manifest, made anew for each fixture."""
    return {"kind": "DuckDBJob", "name": "fixture",
            "connections": {"wh": {"provider": "duckdb", "path": ":memory:"}},
            "steps": [{"name": "a", "sql": "SELECT 1 AS id"}],
            "outputs": [{"name": "o", "connection": "wh", "step": "a", "table": "t", "mode": "overwrite"}]}


FIXTURES = MappingProxyType({
    "valid": lambda j: None,
    "missing kind": lambda j: j.pop("kind"),
    "wrong kind": lambda j: j.update(kind="SparkJob"),
    "bad name": lambda j: j.update(name="Bad Name"),
    "no steps": lambda j: j.update(steps=[]),
    "unknown top key": lambda j: j.update(extra=1),
    "unknown output key": lambda j: j["outputs"][0].update(table_name="t"),
    "bad mode": lambda j: j["outputs"][0].update(mode="upsert"),
    "missing mode": lambda j: j["outputs"][0].pop("mode"),
    "connection key of another provider": lambda j: j["connections"]["wh"].update(bucket="b"),
    "connection without its required key": lambda j: j["connections"]["wh"].pop("path"),
    "bad materialize": lambda j: j["steps"][0].update(materialize="disk"),
    "bad runtime value": lambda j: j.update(runtime={"threads": 0}),
    "bad config definition": lambda j: j.update(spec={"config": {"x": {"type": "decimal"}}}),
    "semantic only: reserved name": lambda j: j["steps"][0].update(name="config"),
})


@pytest.fixture(scope="module")
def schema():
    found = json_schema()
    jsonschema.Draft7Validator.check_schema(found)
    return jsonschema.Draft7Validator(found)


def _rules_or_skip(job):
    """The rules, or a skip when a connection the job reads or writes through has no installed DuckDB adapter."""
    rules = duckdb_rules(job)
    used = [o["connection"] for o in job["outputs"]] + [s["connection"] for s in job["steps"] if "read" in s]
    missing = sorted({job["connections"][name]["provider"] for name in used} - set(rules.adapters))
    if missing:
        pytest.skip("no DuckDB adapter installed for %s" % ", ".join(missing))
    return rules


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: os.path.relpath(p, EXAMPLES_ROOT))
def test_every_example_passes_the_schema_the_checker_and_its_writers(path, schema):
    job = load_job(path)
    rules = _rules_or_skip(job)
    assert [e.message for e in schema.iter_errors(job)] == []
    report = validate_job(path, duckdb_rules)
    assert report.valid, [str(i) for i in report.problems]
    for output in report.job["outputs"]:
        connection = report.job["connections"][output["connection"]]
        target = rules.write_target(connection)
        if target is None:
            pytest.skip("no writer installed for %s" % connection["provider"])
        assert [p for p in check_output(OutputSpec.from_mapping(output), target) if not p.warning] == []


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_the_checker_rejects_whatever_the_schema_rejects(name, schema):
    job = base_job()
    FIXTURES[name](job)
    schema_valid = not list(schema.iter_errors(job))
    checker_valid = validate_job(job, duckdb_rules).valid
    if not schema_valid:
        assert not checker_valid
    assert schema_valid == (name.startswith("semantic only") or name == "valid")
    assert checker_valid == (name == "valid")
