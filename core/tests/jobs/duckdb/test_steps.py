"""Call and source steps: records through the source engine (SourceRunner.collect), loaded into DuckDB."""

import os
from types import SimpleNamespace

import duckdb
import pytest
import yaml

from streamwright.core.jobs.duckdb.engine import run_job
from streamwright.core.jobs.duckdb.steps import _load_records
from streamwright.core.runtime import components
from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation


def _job(tmp_path, step, sql):
    return {"kind": "DuckDBJob", "name": "calls",
            "connections": {"wh": {"provider": "duckdb", "path": str(tmp_path / "wh.duckdb")}},
            "steps": [step, {"name": "shaped", "sql": sql}],
            "outputs": [{"name": "out", "connection": "wh", "step": "shaped", "table": "result", "mode": "overwrite"}],
            "runtime": {"state": {"sqlite": str(tmp_path / "state.db")}}}


def _rows(tmp_path):
    with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as con:
        return con.execute("SELECT * FROM result ORDER BY ALL").fetchall()


def test_a_call_step_flattens_records_and_partitions_by_an_earlier_step(tmp_path, api):
    def items(request):
        group = request["params"]["group"]
        return 200, {"data": [{"id": (group[0] if isinstance(group, list) else group) + "-1", "n": 1}]}
    api.routes[("GET", "/items")] = items
    job = _job(tmp_path, {"name": "items", "connection": "api", "request": {
        "path": "/items", "params": {"group": "{{ partition.group }}"}, "records": {"path": "data"}},
        "partitions": [{"from": "groups", "field": "g", "name": "group"}]}, "SELECT id, n FROM items")
    job["connections"]["api"] = {"provider": "restapi", "base_url": api.url}
    job["steps"].insert(0, {"name": "groups", "sql": "SELECT unnest(['a', 'b']) AS g"})
    result = run_job(job)
    assert result.exit_code == 0, result.error
    assert _rows(tmp_path) == [("a-1", 1), ("b-1", 1)]


def test_a_call_step_with_sql_reads_records_and_keeps_its_shape_when_empty(tmp_path, api):
    api.routes[("GET", "/items")] = lambda request: (200, {"data": []})
    job = _job(tmp_path, {"name": "items", "connection": "api",
                          "request": {"path": "/items", "records": {"path": "data"}},
                          "sql": "SELECT record->>'id' AS id FROM records"}, "SELECT id FROM items")
    job["connections"]["api"] = {"provider": "restapi", "base_url": api.url}
    result = run_job(job)
    assert result.exit_code == 0, result.error
    assert _rows(tmp_path) == []


class VaultSession:
    def __init__(self, connection):
        self.connection = connection

    def read(self, operation, arguments, context):
        yield [{"id": 1, "token_seen": self.connection["token"] == "tok-1", "region": self.connection["region"]}]

    def close(self):
        self.connection = None


class VaultSourceAdapter:
    def open(self, connection, context):
        return VaultSession(connection)


def check_region(connection):
    return [] if connection["region"] in ("eu", "us") else ["region %r is not served" % connection["region"]]


# an SDK connector with a credential key and a value check on its resolved region
VAULT_API = ConnectorDefinition(
    name="vault_api", title="Vault API", family="test",
    connection=(Field("token", "string", required=True, secret=True), Field("region", "string", required=True)),
    operations=(Operation(service="items", method="list"),),
    adapters={"source": __name__ + ":VaultSourceAdapter"}, validator=__name__ + ":check_region")


@pytest.fixture
def vault_api():
    components.register(VAULT_API)
    yield
    components.unregister("vault_api")


def test_a_call_step_connection_is_rendered_and_checked_like_a_source_auth_block(tmp_path, vault_api):
    job = _job(tmp_path, {"name": "items", "connection": "vault", "service": "items", "method": "list"},
               "SELECT region, token_seen FROM items")
    job["spec"] = {"config": {"region": {"type": "string"}}, "secrets": {"token": {"type": "string"}}}
    job["connections"]["vault"] = {"provider": "vault_api", "token": "{{ secrets.token }}",
                                   "region": "{{ config.region }}"}
    result = run_job(job, config={"region": "eu"}, secrets={"token": "tok-1"})
    assert result.exit_code == 0, result.error
    assert _rows(tmp_path) == [("eu", True)]
    failed = run_job(dict(job, name="calls_mars"), config={"region": "mars"}, secrets={"token": "tok-1"})
    assert failed.exit_code == 1 and "region 'mars' is not served" in failed.error


def test_a_relative_source_is_found_next_to_the_job_only(tmp_path, monkeypatch, api):
    api.routes[("GET", "/rows")] = lambda request: (200, [{"id": 1}, {"id": 2}])
    source = tmp_path / "jobs" / "sources" / "demo"
    (source / "streams").mkdir(parents=True)
    (source / "source.yaml").write_text(yaml.safe_dump({
        "kind": "source", "name": "demo", "auth": {"provider": "restapi"}, "http": {"base_url": api.url}}))
    (source / "streams" / "rows.yaml").write_text(yaml.safe_dump({
        "requests": [{"name": "raw", "http": {"path": "/rows"}}],
        "transform": {"mode": "page", "steps": [{"name": "rows", "select": "SELECT record->>'id' AS id FROM raw"}]},
        "export": {"rows": {"step": "rows", "primary_key": ["id"]}}}))
    job = _job(tmp_path, {"name": "rows", "source": "sources/demo", "stream": "rows"}, "SELECT id FROM rows")
    (tmp_path / "jobs" / "job.yaml").write_text(yaml.safe_dump(job))
    monkeypatch.chdir(tmp_path)
    assert run_job(str(tmp_path / "jobs" / "job.yaml")).exit_code == 0
    assert _rows(tmp_path) == [("1",), ("2",)]
    elsewhere = tmp_path / "jobs" / "deeper"
    elsewhere.mkdir()
    (elsewhere / "job.yaml").write_text(yaml.safe_dump(job))
    result = run_job(str(elsewhere / "job.yaml"))
    assert result.exit_code == 1 and "relative paths start at the job's folder" in result.error
    assert os.path.isdir(source)


def test_source_without_rows_uses_declared_export_columns(tmp_path, api):
    api.routes[("GET", "/rows")] = lambda request: (200, [])
    source = tmp_path / "source"
    (source / "streams").mkdir(parents=True)
    (source / "source.yaml").write_text(yaml.safe_dump({
        "kind": "source", "name": "demo", "auth": {"provider": "restapi"}, "http": {"base_url": api.url}}))
    (source / "streams" / "rows.yaml").write_text(yaml.safe_dump({
        "requests": [{"name": "raw", "http": {"path": "/rows"}}],
        "transform": {"mode": "page", "steps": [{"name": "rows", "select": "SELECT record->>'id' AS id FROM raw"}]},
        "export": {"rows": {"step": "rows"}}}))
    job = _job(tmp_path, {"name": "rows", "source": str(source), "stream": "rows"}, "SELECT id FROM rows")
    result = run_job(job)
    assert result.exit_code == 0, result.error
    with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as con:
        assert con.execute("DESCRIBE result").fetchall()[0][:2] == ("id", "VARCHAR")
    assert _rows(tmp_path) == []


def test_records_are_inserted_before_source_is_exhausted_and_batches_stay_bounded():
    total = 12017
    produced = [0]
    insert_points = []

    def records():
        for index in range(total):
            produced[0] += 1
            yield {"id": index}

    class TrackingConnection:
        def __init__(self, con):
            self.con = con

        def execute(self, query, parameters=None):
            if query.startswith("INSERT INTO"):
                insert_points.append(produced[0])
            return self.con.execute(query, parameters) if parameters is not None else self.con.execute(query)

    with duckdb.connect() as con:
        run = SimpleNamespace(session=SimpleNamespace(con=TrackingConnection(con)))
        _load_records(run, {"name": "items"}, records())
        assert con.execute("SELECT COUNT(*) FROM items").fetchone()[0] == total
    assert insert_points[0] < total
    previous_points = [0, *insert_points[:-1]]
    assert all(0 < point - previous <= 5000 for previous, point in zip(previous_points, insert_points, strict=True))


def test_an_empty_message_read_keeps_the_columns_of_the_established_schema():
    step = {"name": "arrivals", "service": "message"}
    with duckdb.connect() as con:
        run = SimpleNamespace(session=SimpleNamespace(con=con), carry={}, record_columns=None)
        _load_records(run, step, iter([{"topic": "t", "partition": 0, "offset": 8, "value": "a"}]))
        full = [row[0] for row in con.execute("DESCRIBE arrivals").fetchall()]
        assert "offset" in full and con.execute("SELECT COUNT(*) FROM arrivals").fetchone()[0] == 1
        _load_records(run, step, iter([]))                 # a batch that polled no messages
        empty = [row[0] for row in con.execute("DESCRIBE arrivals").fetchall()]
        assert empty == full and con.execute("SELECT COUNT(*) FROM arrivals").fetchone()[0] == 0
