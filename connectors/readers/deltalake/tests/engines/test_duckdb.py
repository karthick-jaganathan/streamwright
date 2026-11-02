"""The deltalake DuckDB adapter: delta_scan reads inside the roots, shared write rules, replay-safe writes."""

import os
from types import MappingProxyType

import pytest

from streamwright.connectors.deltalake import CONNECTOR
from streamwright.core.sdk.testing import DuckDBAdapterContract, JobInvalidError, SqlStateStore, open_sqlite, run_job

deltalake = pytest.importorskip("deltalake")


class TestDuckDBAdapter(DuckDBAdapterContract):
    __test__ = True
    definition = CONNECTOR
    read_arguments = MappingProxyType({"path": "cities", "format": "delta"})

    @pytest.fixture(autouse=True)
    def tables(self, lake):
        self.connection = {"roots": [lake[0]]}
        self.expected_records = None


def _job(tmp_path, output, sql="SELECT * FROM (VALUES (1, DATE '2026-11-01', 'a'), (2, DATE '2026-11-02', 'b')) "
                                "t(id, day, v)"):
    return {"kind": "DuckDBJob", "name": "lake_job",
            "connections": {"lake": {"provider": "deltalake", "roots": [str(tmp_path / "lake")]}},
            "steps": [{"name": "rows", "sql": sql}],
            "outputs": [dict({"name": "orders", "connection": "lake", "step": "rows", "path": "orders"}, **output)],
            "runtime": {"state": {"sqlite": str(tmp_path / "state.db")}}}


def _table(tmp_path):
    return deltalake.DeltaTable(str(tmp_path / "lake" / "orders"))


def test_a_step_reads_a_table_inside_the_roots_only(tmp_path, lake):
    inside, outside = lake
    job = {"kind": "DuckDBJob", "name": "read_job",
           "connections": {"lake": {"provider": "deltalake", "roots": [inside]}},
           "steps": [{"name": "cities", "connection": "lake", "read": "delta", "path": "cities"}],
           "outputs": [{"name": "copy", "connection": "lake", "step": "cities", "path": "copy", "mode": "append"}]}
    assert run_job(job).exit_code == 0
    assert deltalake.DeltaTable(os.path.join(inside, "copy")).to_pyarrow_table().num_rows == 3
    job["steps"][0]["path"] = os.path.join(outside, "cities")
    failed = run_job(job)
    assert failed.exit_code == 1 and "outside the roots" in failed.error


def test_an_append_whose_commit_already_happened_is_not_written_again(tmp_path, monkeypatch):
    job = _job(tmp_path, {"mode": "append"})
    real = SqlStateStore.record_receipt

    def lost(self, *args, **kwargs):              # the process dies between the commit and its receipt
        raise ConnectionError("state store unreachable")
    monkeypatch.setattr(SqlStateStore, "record_receipt", lost)
    failed = run_job(job)
    assert failed.exit_code == 1 and _table(tmp_path).to_pyarrow_table().num_rows == 2
    monkeypatch.setattr(SqlStateStore, "record_receipt", real)
    store = open_sqlite(str(tmp_path / "state.db"))
    lease = store.acquire_lease("lake_job", "operator", 60)
    store.retry_batch(lease, failed.batches[0])
    store.release_lease(lease)
    resumed = run_job(job, resume_only=True)
    assert resumed.exit_code == 0 and _table(tmp_path).to_pyarrow_table().num_rows == 2
    assert store.get_receipts("lake_job", failed.batches[0])["orders"]["version"] == 0
    assert run_job(job).exit_code == 0 and _table(tmp_path).to_pyarrow_table().num_rows == 4


def _merge_job(tmp_path, v, *, durable=False):
    job = {"kind": "DuckDBJob", "name": "lake_job",
           "connections": {"lake": {"provider": "deltalake", "roots": [str(tmp_path / "lake")]}},
           "steps": [{"name": "rows", "sql": "SELECT 1 AS id, DATE '2026-11-01' AS day, '%s' AS v" % v}],
           "outputs": [{"name": "orders", "connection": "lake", "step": "rows", "path": "orders", "mode": "merge",
                        "condition": "target.id = source.id",
                        "when_matched": [{"action": "update", "set": {"v": "source.v"}}],
                        "when_not_matched": [{"action": "insert"}]}]}
    if durable:
        job["runtime"] = {"state": {"sqlite": str(tmp_path / "state.db")}}
    return job


def test_two_in_memory_runs_with_changed_data_apply_the_second_merge(tmp_path):
    # Each ephemeral state store issues its own identity, so the second run's app transaction no longer
    # collides with the first run's (both are batch sequence 1) and the changed merge is applied.
    assert run_job(_merge_job(tmp_path, "a")).exit_code == 0
    result = run_job(_merge_job(tmp_path, "b"))
    assert result.exit_code == 0, result.error
    assert [r["v"] for r in _table(tmp_path).to_pyarrow_table().to_pylist()] == ["b"]


def test_a_durable_replay_of_a_committed_write_is_skipped_and_reported(tmp_path, monkeypatch):
    job = _merge_job(tmp_path, "a", durable=True)
    real = SqlStateStore.record_receipt

    def lost(self, *args, **kwargs):                 # the process dies between the commit and its receipt
        raise ConnectionError("state store unreachable")
    monkeypatch.setattr(SqlStateStore, "record_receipt", lost)
    failed = run_job(job)
    assert failed.exit_code == 1
    monkeypatch.setattr(SqlStateStore, "record_receipt", real)
    store = open_sqlite(str(tmp_path / "state.db"))
    lease = store.acquire_lease("lake_job", "operator", 60)
    store.retry_batch(lease, failed.batches[0])
    store.release_lease(lease)
    resumed = run_job(job, resume_only=True)
    assert resumed.exit_code == 0 and "orders" in resumed.skipped
    assert _table(tmp_path).to_pyarrow_table().num_rows == 1
    receipt = open_sqlite(str(tmp_path / "state.db")).get_receipts("lake_job", failed.batches[0])["orders"]
    assert receipt["already_committed"] is True


def test_merge_scopes_the_target_with_rendered_pinned_values(tmp_path):
    assert run_job(_job(tmp_path, {"mode": "append"})).exit_code == 0
    merge = _job(tmp_path, {"mode": "merge", "condition": "target.id = source.id",
                            "target_predicate": "target.v IN ({{ rows.v }})",
                            "when_matched": [{"action": "update", "set": {"v": "source.v"}}],
                            "when_not_matched": [{"action": "insert"}]},
                 "SELECT 2 AS id, DATE '2026-11-02' AS day, 'b' AS v UNION ALL SELECT 3, DATE '2026-11-03', 'it''s'")
    result = run_job(merge)
    assert result.exit_code == 0, result.error
    rows = sorted(_table(tmp_path).to_pyarrow_table().to_pylist(), key=lambda r: r["id"])
    assert [r["v"] for r in rows] == ["a", "b", "it's"]
    receipt = open_sqlite(str(tmp_path / "state.db")).get_receipts("lake_job", result.batches[0])["orders"]
    assert receipt["predicate"] == "target.v IN ('b', 'it''s')"


def test_an_overwrite_replaces_what_replace_where_selects(tmp_path):
    assert run_job(_job(tmp_path, {"mode": "append"})).exit_code == 0
    result = run_job(_job(tmp_path, {"mode": "overwrite", "replace_where": "day = DATE '2026-11-02'"},
                          "SELECT 5 AS id, DATE '2026-11-02' AS day, 'z' AS v"))
    assert result.exit_code == 0, result.error
    assert sorted(row["id"] for row in _table(tmp_path).to_pyarrow_table().to_pylist()) == [1, 5]


def test_schema_changes_need_add_columns(tmp_path):
    assert run_job(_job(tmp_path, {"mode": "append"})).exit_code == 0
    wider = "SELECT 9 AS id, DATE '2026-11-09' AS day, 'z' AS v, 1.5 AS extra"
    failed = run_job(_job(tmp_path, {"mode": "append"}, wider))
    assert failed.exit_code == 1 and "SchemaMismatch" in failed.error
    store = open_sqlite(str(tmp_path / "state.db"))
    lease = store.acquire_lease("lake_job", "operator", 60)
    store.record_batch(lease, failed.batches[0], "abandoned", {})
    store.release_lease(lease)
    assert run_job(_job(tmp_path, {"mode": "append", "schema_evolution": "add_columns"}, wider)).exit_code == 0
    assert "extra" in _table(tmp_path).schema().to_arrow().names


def test_an_unscoped_overwrite_is_rejected_before_anything_runs(tmp_path):
    with pytest.raises(JobInvalidError, match="overwrite needs replace_where, or whole_table"):
        run_job(_job(tmp_path, {"mode": "overwrite"}))
