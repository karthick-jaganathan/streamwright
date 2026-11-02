"""Engine reach (contract v1.2 §6a): a connector is usable natively or through a call step; the fixed display order."""

from streamwright.core.runtime import components
from streamwright.core.runtime.definitions import ENGINE_ORDER, engine_reach


class _Definition:
    def __init__(self, engines):
        self.engines = frozenset(engines)


def test_the_order_is_always_source_duckdb_spark():
    assert ENGINE_ORDER == ("source", "duckdb", "spark")


def test_a_source_only_connector_is_reachable_by_the_job_engines_through_call_steps():
    assert engine_reach(_Definition({"source"})) == ["source", "duckdb (call)", "spark (call)"]


def test_a_fully_native_connector_lists_every_engine_without_call():
    assert engine_reach(_Definition({"source", "duckdb", "spark"})) == ["source", "duckdb", "spark"]


def test_a_native_spark_connector_still_reaches_duckdb_through_a_call_step():
    assert engine_reach(_Definition({"source", "spark"})) == ["source", "duckdb (call)", "spark"]


def test_installed_connectors_show_the_right_reach():
    assert engine_reach(components.load("google_ads")) == ["source", "duckdb (call)", "spark (call)"]
    assert engine_reach(components.load("s3")) == ["source", "duckdb", "spark"]
