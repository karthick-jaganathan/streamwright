"""A shared, bound where-condition language for connector authors."""

import pytest

from streamwright.core.sql.conditions import check_conditions, render_conditions


def test_postgres_conditions_bind_values_and_quote_column_names():
    sql, parameters = render_conditions([
        {"column": 'a"b', "op": "=", "value": "' OR 1=1 --"},
        {"column": "id", "op": "IN", "value": [1, 2]},
        {"column": "deleted_at", "op": "IS NULL"},
    ])
    assert sql == ' WHERE "a""b" = $w0 AND "id" = ANY($w1) AND "deleted_at" IS NULL'
    assert parameters == {"w0": "' OR 1=1 --", "w1": [1, 2]}


def test_rejects_operator_and_type_injection_before_rendering():
    conditions = [{"column": "id", "op": "= 1; DROP TABLE users", "value": 1,
                   "type": "BIGINT); DROP TABLE users; --"}]
    assert len(check_conditions(conditions)) == 2
    with pytest.raises(ValueError, match="unsupported"):
        render_conditions(conditions)


def test_valueless_condition_with_value_is_rejected():
    assert check_conditions([{"column": "id", "op": "IS NULL", "value": None}])


def test_mysql_membership_expands_bound_values_and_handles_empty_list():
    sql, parameters = render_conditions([{"column": "id", "op": "IN", "value": [1, 2]},
                                         {"column": "id", "op": "NOT IN", "value": []}], dialect="mysql")
    assert sql == ' WHERE "id" IN ($p_0_0, $p_0_1) AND TRUE'
    assert parameters == {"p_0_0": 1, "p_0_1": 2}


def test_spark_membership_expands_typed_placeholders_and_scalars_keep_their_cast():
    sql, parameters = render_conditions([{"column": "day", "op": "IN", "value": ["2026-11-01"], "type": "DATE"},
                                         {"column": "n", "op": ">", "value": 3, "type": "INT"},
                                         {"column": "id", "op": "IN", "value": []}], dialect="spark")
    assert sql == ' WHERE "day" IN (CAST($p_0_0 AS DATE)) AND "n" > CAST($p_1 AS INT) AND FALSE'
    assert parameters == {"p_0_0": "2026-11-01", "p_1": 3}
