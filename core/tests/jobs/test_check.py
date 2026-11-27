#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

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
                        "api": {"provider": "rest_api", "http": {"base_url": "https://x"}}},
        "steps": [{"name": "raw", "connection": "lake", "read": "csv", "path": "in/*.csv"},
                  {"name": "calls", "connection": "api", "operation": {"path": "/v1/{{ partition.id }}"},
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


def test_a_jobs_name_may_have_dotted_parts_but_its_steps_names_may_not():
    assert _changed(lambda j: j.update(name="google_ads.upload_conversions")) == []
    assert _changed(lambda j: j.update(name="a" * 128)) == []
    for bad in ("google_ads.", ".orders", "a..b", "Orders.x", "a-b", "a.1b", "a" * 129):
        assert _changed(lambda j, bad=bad: j.update(name=bad)) == [("bad-value", "name")], bad
    assert ("bad-value", "steps[0].name") in _changed(lambda j: j["steps"][0].update(name="raw.orders"))


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
    assert _changed(lambda j: j["steps"][0].update(service="x")) == [("unknown-key", "steps[0].service")]
    assert set(_changed(lambda j: j["steps"][0].update(operation={"service": "x", "method": "y"}))) == {
        ("unsupported-key", "steps[0].operation.service"), ("unsupported-key", "steps[0].operation.method")}
    assert _changed(lambda j: j["steps"][0].update(table="t")) == [("unsupported-key", "steps[0].table")]
    assert ("bad-value", "steps[0]") in _changed(lambda j: j["steps"][0].update(read="table"))
    assert ("unsupported-key", "steps[2].partitions") in _changed(lambda j: j["steps"][2].update(partitions=[]))


def test_an_operation_is_one_group_whose_kind_the_connector_decides():
    def regroup(name):
        return lambda j: j["steps"][1].update({name: j["steps"][1].pop("operation")})
    assert ("unknown-key", "steps[1].http") in _changed(regroup("http"))
    assert ("unknown-key", "steps[1].call") in _changed(regroup("call"))
    assert _changed(lambda j: j["steps"][1]["operation"].update(type="http")) == [
        ("unknown-key", "steps[1].operation.type")]
    assert _changed(lambda j: j["steps"][1]["operation"].update(service="S")) == [("bad-value", "steps[1].operation")]


def test_an_operation_needs_the_keys_of_its_kind():
    def operation(value):
        return lambda j: j["steps"][1].update(operation=value)
    assert _changed(operation({"method": "POST", "json": {"a": 1}})) == [("missing-key", "steps[1].operation.path")]
    assert _changed(operation({"method": "GET", "records": {"path": "data"}})) == [
        ("missing-key", "steps[1].operation.path")]
    assert {("missing-key", "steps[1].operation.service"), ("missing-key", "steps[1].operation.method")} <= set(
        _changed(operation({"arguments": {"id": 1}})))


def test_a_mixed_operation_is_reported_once():
    assert _changed(lambda j: j["steps"][1].update(operation={"service": 5, "path": 7, "params": 3, "bogus": 1,
                                                              "type": "http"})) == [("bad-value", "steps[1].operation")]


def test_input_constraints_and_defaults_are_checked_as_sources_do():
    def constrain(j):
        j["spec"]["config"].update(code={"type": "string", "pattern": "[a-"}, size={"type": "integer", "minimum": "5"},
                                   kind={"type": "string", "enum": ["x"], "default": "y"})
    assert sorted(_changed(constrain)) == [("bad-type", "spec.config.size.minimum"),
                                           ("bad-value", "spec.config.code.pattern"),
                                           ("bad-value", "spec.config.kind.default")]


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
