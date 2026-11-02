import pytest

from streamwright.core.jobs.plan import PlanError, build_plan, sql_table_reads


def test_reads_come_from_the_parser_not_from_text():
    sql = ('WITH recent AS (SELECT * FROM "Orders") SELECT o.id AS customers, customers(1) '
           "FROM recent o JOIN wh.main.dim d ON 1 = 1 -- payments\nWHERE note = 'refunds'")
    assert sql_table_reads(sql) == {"orders"}
    with pytest.raises(PlanError):
        sql_table_reads("DELETE FROM x")


def test_order_is_stable_and_follows_references():
    job = {"steps": [{"name": "late", "sql": "SELECT * FROM early"},
                     {"name": "files", "path": "{{ late.uri }}"},
                     {"name": "early", "sql": "SELECT 1"}],
           "outputs": [{"name": "out", "step": "files"}]}
    plan = build_plan(job)
    assert plan.order == ["early", "late", "files"]
    assert plan.by_name["early"].outputs == {"out"} and plan.by_name["files"].used_by == ["out"]


def test_a_cycle_is_an_error():
    job = {"steps": [{"name": "a", "sql": "SELECT * FROM b"}, {"name": "b", "sql": "SELECT * FROM a"}]}
    with pytest.raises(PlanError, match="a -> b -> a"):
        build_plan(job)
