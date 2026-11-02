"""The sectioned checker, through the DuckDBJob rules (the duckdb built-in, the files connector, a fake writable
adapter)."""

from types import MappingProxyType

from streamwright.connectors.files import CONNECTOR as FILES
from streamwright.core.jobs.check import check_job
from streamwright.core.jobs.duckdb.adapter import BUILT_IN_ADAPTERS, ProviderAdapter
from streamwright.core.jobs.duckdb.definition_adapter import DefinitionDuckDBAdapter
from streamwright.core.jobs.duckdb.rules import DuckDBRules
from streamwright.core.jobs.duckdb.writers import TABLE_TARGET
from streamwright.core.jobs.fields import Field


class VaultAdapter(ProviderAdapter):
    provider = "vault"
    connection_fields = MappingProxyType({"token": Field("string"), "host": Field("string", required=True)})
    secret_keys = ("token",)
    reads = MappingProxyType({"table": ("table",)})

    def write_target(self, connection):
        return TABLE_TARGET


RULES = DuckDBRules(dict({"vault": VaultAdapter(), "files": DefinitionDuckDBAdapter(FILES)},
                         **{name: adapter() for name, adapter in BUILT_IN_ADAPTERS.items()}),
                    connectors={"rest_api"})
def new_job():
    """A valid job (a new copy each time)."""
    return {
        "kind": "DuckDBJob", "name": "orders",
        "spec": {"config": {"day": {"type": "date"}}, "secrets": {"token": {"type": "string"}}},
        "connections": {"wh": {"provider": "duckdb", "path": "wh.duckdb"},
                        "lake": {"provider": "files", "roots": ["data"]},
                        "vault": {"provider": "vault", "host": "h", "token": "{{ secrets.token }}"},
                        "api": {"provider": "rest_api", "base_url": "https://x"}},
        "steps": [{"name": "raw", "connection": "lake", "read": "csv", "path": "in/*.csv"},
                  {"name": "calls", "connection": "api", "request": {"path": "/v1/{{ partition.id }}"},
                   "partitions": [{"from": "raw", "field": "id"}]},
                  {"name": "shaped", "sql": "SELECT * FROM raw JOIN calls USING (id)", "materialize": "table"}],
        "outputs": [{"name": "fact", "connection": "wh", "step": "shaped", "table": "fact", "mode": "merge",
                     "primary_key": ["id"]}],
        "runtime": {"state": {"connection": "wh"}, "timeout_seconds": 60},
    }


def _issues(job):
    return [(i.code, i.path) for i in check_job(job, RULES) if i.severity == "error"]


def _changed(change):
    job = new_job()
    change(job)
    return _issues(job)


def test_the_reference_job_is_valid():
    assert _issues(new_job()) == []


def test_structure_rejects_unknown_and_mistyped_keys():
    assert _changed(lambda j: j["outputs"][0].update(table_name="x")) == [("unknown-key", "outputs[0].table_name")]
    assert _changed(lambda j: j["runtime"].update(timeout="1m")) == [("unknown-key", "runtime.timeout")]
    assert _changed(lambda j: j["connections"]["wh"].update(bucket="b")) == [("unknown-key", "connections.wh.bucket")]
    assert ("bad-type", "steps[0].row_index") in _changed(lambda j: j["steps"][0].update(row_index=3))


def test_steps_and_outputs_share_one_namespace_and_reserved_names():
    assert _changed(lambda j: j["outputs"][0].update(name="shaped")) == [("duplicate-name", "outputs[0].name")]
    assert ("reserved-name", "steps[0].name") in _changed(lambda j: j["steps"][0].update(name="records"))
    assert ("bad-name", "connections.Bad-Name") in _changed(
        lambda j: j["connections"].update({"Bad-Name": {"provider": "duckdb", "path": "x"}}))


def test_secrets_only_in_connection_values_and_never_literal():
    assert _changed(lambda j: j["steps"][2].update(sql="SELECT '{{ secrets.token }}'")) == [
        ("secret-placement", "steps[2].sql")]
    assert _changed(lambda j: j["connections"]["vault"].update(token="plain")) == [
        ("literal-secret", "connections.vault.token")]
    assert ("undeclared-secret", "connections.vault.token") in _changed(
        lambda j: j["connections"]["vault"].update(token="{{ secrets.other }}"))


def test_secrets_only_in_declared_credential_keys():
    assert _changed(lambda j: j["connections"]["vault"].update(host="{{ secrets.token }}")) == [
        ("secret-placement", "connections.vault.host")]
    assert _changed(lambda j: j["connections"]["lake"].update(roots=["{{ secrets.token }}"])) == [
        ("secret-placement", "connections.lake.roots[0]")]



def test_references_must_be_declared_steps_or_constants_and_only_where_rendered():
    assert _changed(lambda j: j["steps"][0].update(path="{{ shaped.uri }}")) == [("dependency", "steps")]
    assert _changed(lambda j: j["steps"][2].update(sql="SELECT {{ raw.id }}")) == [
        ("unsupported-reference", "steps[2].sql")]
    assert _changed(lambda j: j["steps"][0].update(path="{{ config.nope }}")) == [
        ("undeclared-input", "steps[0].path")]


def test_step_kinds_allow_only_their_keys():
    assert _changed(lambda j: j["steps"][0].update(service="x")) == [("unsupported-key", "steps[0].service")]
    assert _changed(lambda j: j["steps"][0].update(table="t")) == [("unsupported-key", "steps[0].table")]
    assert ("bad-value", "steps[0]") in _changed(lambda j: j["steps"][0].update(read="table"))
    assert ("unsupported-key", "steps[2].partitions") in _changed(lambda j: j["steps"][2].update(partitions=[]))


def test_outputs_follow_their_writer():
    assert _changed(lambda j: j["outputs"][0].pop("primary_key")) == [("missing-key", "outputs[0]")]
    assert _changed(lambda j: j["outputs"][0].update(mode="overwrite", path="p")) == [("unsupported-key", "outputs[0]")]
    assert _changed(lambda j: j["outputs"][0].update(connection="api")) == [("not-writable", "outputs[0].connection")]
    appending = dict(new_job(), outputs=[dict(new_job()["outputs"][0], mode="append")])
    found = [(i.code, i.severity) for i in check_job(appending, RULES)]
    assert found == [("not-idempotent", "warning")]


def test_state_and_cycles():
    assert _changed(lambda j: j["runtime"].update(state={"connection": "lake"})) == [("bad-value", "runtime.state")]
    assert _changed(lambda j: j["runtime"].update(state={"sqlite": "s.db", "connection": "wh"})) == [
        ("bad-value", "runtime.state")]
    assert _changed(lambda j: j["steps"][0].update(path="{{ shaped.uri }}"))[0][0] == "dependency"


def test_connections_are_checked_by_their_adapter_or_connector():
    assert _changed(lambda j: j["connections"].update(x={"provider": "nowhere"})) == [
        ("connector-not-installed", "connections.x")]
