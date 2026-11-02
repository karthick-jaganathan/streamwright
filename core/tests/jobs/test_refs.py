import datetime
import decimal

import pytest

from streamwright.core.jobs.refs import (
    RefError,
    decode_value,
    encode_value,
    input_references,
    resolve,
    sql_literal,
    substitute,
)


def test_a_value_that_is_one_input_reference_keeps_the_input_type():
    job = {"value": "{{ config.channels }}", "limit": " {{ config.limit }} ", "sql": "IN {{ config.channels }}"}
    resolved, problems = resolve(job, {"channels": ["SEARCH", "SHOPPING"], "limit": 5}, {})
    # a query builder's IN gets the list itself; inside other text the value is written as text
    assert resolved == {"value": ["SEARCH", "SHOPPING"], "limit": 5, "sql": "IN ['SEARCH', 'SHOPPING']"}
    assert problems == []


def test_resolve_replaces_inputs_only_and_reports_missing_values_without_secrets():
    job = {"a": "{{ config.x }}-{{ secrets.s }}", "b": ["{{ step.col }}"], "{{ config.k }}": 1}
    resolved, problems = resolve(job, {"x": 1, "k": "key"}, {"s": "pw"})
    assert resolved == {"a": "1-pw", "b": ["{{ step.col }}"], "key": 1} and problems == []
    _, problems = resolve(job, {}, {})
    assert problems == ["config.k has no value", "config.x has no value", "secrets.s has no value"]
    assert input_references(job, "secrets") == {"s"}


def test_literals_are_standard_sql_and_typed():
    assert sql_literal("it's \\ fine") == "'it''s \\ fine'"
    assert sql_literal(None) == "NULL" and sql_literal(True) == "TRUE"
    assert sql_literal(decimal.Decimal("1.50")) == "1.50"
    assert sql_literal(datetime.date(2026, 11, 1)) == "DATE '2026-11-01'"
    aware = datetime.datetime(2026, 11, 1, 5, 30, tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
    assert sql_literal(aware) == "TIMESTAMP '2026-11-01 00:00:00.000000'"
    for bad in (float("nan"), b"x", "nul\x00"):
        with pytest.raises(RefError):
            sql_literal(bad)


def test_substitute_renders_lists_and_constants_and_refuses_the_rest():
    text = "t.d IN ({{ s.d }}) AND t.b = {{ batch.id }}"
    rendered = substitute(text, {("s", "d"): ["a'b", "c"]}, {"batch.id": "b-1"}, limit=10)
    assert rendered == "t.d IN ('a''b', 'c') AND t.b = 'b-1'"
    assert substitute("x IN ({{ s.d }})", {("s", "d"): []}, {}, 10) == "x IN (NULL)"
    with pytest.raises(RefError, match="exceed"):
        substitute("{{ s.d }}", {("s", "d"): [1, 2]}, {}, 1)
    with pytest.raises(RefError, match="cannot resolve"):
        substitute("{{ other.x }}", {}, {}, 10)


def test_pinned_values_round_trip_with_their_types():
    for value in (datetime.datetime(2026, 1, 2, 3, 4), datetime.date(2026, 1, 2), decimal.Decimal("3.10"), "x", 5):
        assert decode_value(encode_value(value)) == value
