"""DuckDBJob execution policy (contract v1.2 §6a): retry of transient writes, on_error on steps/outputs, bad keys."""

from streamwright.core.jobs import policy
from streamwright.core.jobs.duckdb.adapter import DuckDBCatalogAdapter
from streamwright.core.jobs.duckdb.engine import run_job, validate_job


def _job(tmp_path, *, name="policy", step_sql="SELECT 1 AS id", step_extra=None, out_sql="SELECT * FROM data",
         out_extra=None):
    step = {"name": "data", "sql": step_sql, **(step_extra or {})}
    output = {"name": "first", "connection": "wh", "sql": out_sql, "table": "a", "mode": "overwrite",
              **(out_extra or {})}
    return {"kind": "DuckDBJob", "name": name,
            "connections": {"wh": {"provider": "duckdb", "path": str(tmp_path / ("%s.duckdb" % name))}},
            "steps": [step], "outputs": [output],
            "runtime": {"state": {"sqlite": str(tmp_path / ("%s-state.db" % name))}}}


def test_an_output_write_retries_a_transient_error_through_the_shared_retry(tmp_path, monkeypatch):
    original, calls = DuckDBCatalogAdapter.write, []

    def flaky(self, context):
        calls.append(1)
        if len(calls) == 1:
            raise policy.TransientError("transient write")
        return original(self, context)

    monkeypatch.setattr(DuckDBCatalogAdapter, "write", flaky)
    result = run_job(_job(tmp_path, out_extra={"retry": {"max_attempts": 3, "backoff": "constant"}}))
    assert result.exit_code == 0, result.error
    assert len(calls) == 2 and result.outputs == {"first": 1}


def test_a_failing_output_is_fatal_but_on_error_skip_lets_the_batch_complete(tmp_path):
    failed = run_job(_job(tmp_path, name="out_fail", out_sql="SELECT * FROM does_not_exist"))
    assert failed.exit_code == 1

    skipped = run_job(_job(tmp_path, name="out_skip", out_sql="SELECT * FROM missing", out_extra={"on_error": "skip"}))
    assert skipped.exit_code == 0, skipped.error


def test_a_failing_step_with_on_error_warn_does_not_fail_the_batch(tmp_path):
    failed = run_job(_job(tmp_path, name="step_fail", step_sql="SELECT * FROM nope", out_sql="SELECT 1 AS id"))
    assert failed.exit_code == 1

    warned = run_job(_job(tmp_path, name="step_warn", step_sql="SELECT * FROM nope",
                          step_extra={"on_error": "warn"}, out_sql="SELECT 1 AS id"))
    assert warned.exit_code == 0, warned.error


def test_an_unsupported_policy_key_names_the_engine(tmp_path):
    report = validate_job(_job(tmp_path, out_extra={"paginator": {"type": "page"}}))
    codes = [(i.code, i.message) for i in report.issues if i.code == "unsupported-argument"]
    assert codes == [("unsupported-argument", "`paginator` is not supported by the duckdb engine on outputs")]

    report = validate_job(_job(tmp_path, step_extra={"rate_limit": {"requests": 1, "per": "1s"}}))
    assert any(i.code == "unsupported-argument" and "duckdb engine on steps" in i.message for i in report.issues)
