from streamwright.core.jobs.write_rules import (
    OutputSpec,
    WriteTarget,
    check_merge_clauses,
    check_output,
    check_overwrite_scope,
    render_target_predicate,
    warn_merge_scope,
)

TABLE = WriteTarget("table", ("append", "overwrite", "merge"), ("table", "primary_key"),
                    required={"*": ("table",), "merge": ("primary_key",)}, replay_safe=("overwrite", "merge"))
FORMAT = WriteTarget("format", ("append", "overwrite", "merge"), ("path", "replace_where", "whole_table", "condition",
                                                                  "target_predicate", "when_matched",
                                                                  "when_not_matched"),
                     replay_safe=("append", "overwrite", "merge"),
                     checks=(check_overwrite_scope, check_merge_clauses, warn_merge_scope))


def _codes(found):
    return sorted((p.code, p.warning) for p in found)


def test_a_mode_is_required_and_must_be_one_the_writer_has():
    assert _codes(check_output(OutputSpec("o", step="s", table="t"), TABLE)) == [("missing-key", False)]
    assert _codes(check_output(OutputSpec("o", step="s", table="t", mode="error"), TABLE)) == [
        ("unsupported-mode", False)]


def test_keys_the_writer_does_not_read_are_errors_not_ignored():
    found = check_output(OutputSpec("o", step="s", table="t", mode="overwrite", condition="x", path="p"), TABLE)
    assert _codes(found) == [("unsupported-key", False), ("unsupported-key", False)]


def test_merge_needs_its_key_and_append_is_flagged_as_not_replay_safe():
    assert _codes(check_output(OutputSpec("o", step="s", table="t", mode="merge"), TABLE)) == [("missing-key", False)]
    assert _codes(check_output(OutputSpec("o", step="s", table="t", mode="append"), TABLE)) == [
        ("not-idempotent", True)]


def test_a_table_format_overwrite_needs_a_scope():
    assert _codes(check_output(OutputSpec("o", step="s", mode="overwrite"), FORMAT)) == [("write-guard", False)]
    assert check_output(OutputSpec("o", step="s", mode="overwrite", whole_table=True), FORMAT) == []
    assert _codes(check_output(OutputSpec("o", step="s", mode="overwrite", whole_table=True, replace_where="d=1"),
                               FORMAT)) == [("bad-value", False)]


def test_merge_clauses_and_scope():
    plain = OutputSpec("o", step="s", mode="merge", condition="target.id = source.id")
    assert _codes(check_output(plain, FORMAT)) == [("merge-scope", True), ("missing-key", False)]
    scoped = OutputSpec("o", step="s", mode="merge", condition="target.id = source.id",
                        target_predicate="target.d IN ({{ s.d }})", when_not_matched=({"action": "update"},))
    assert _codes(check_output(scoped, FORMAT)) == [("bad-value", False)]


def test_target_predicates_render_pinned_values_as_literals():
    text = render_target_predicate("target.d IN ({{ s.d }}) AND b = {{ batch.id }}", lambda s, c: ["x'); DROP", "y"],
                                   {"batch.id": "b1"}, 10)
    assert text == "target.d IN ('x''); DROP', 'y') AND b = 'b1'"
