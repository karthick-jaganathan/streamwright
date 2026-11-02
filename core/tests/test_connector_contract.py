"""
Connector contract v1: the definition types, the engines' capabilities, generic checks, and how engines discover and
bind definitions.
"""

from types import MappingProxyType, SimpleNamespace

import pytest

from streamwright.core.io.capabilities import DUCKDB_CAPABILITIES as DUCKDB_ENGINE
from streamwright.core.io.capabilities import SOURCE_CAPABILITIES
from streamwright.core.io.file_rules import DUCKDB_CAPABILITIES
from streamwright.core.jobs.duckdb.definition_adapter import DefinitionDuckDBAdapter
from streamwright.core.jobs.duckdb.loading import load_adapters
from streamwright.core.runtime import components, plugins, source_connectors
from streamwright.core.runtime.definitions import definition_names, engine_connectors, load_definition
from streamwright.core.sdk.contract import (
    AdapterLoadError,
    ConnectorDefinition,
    DefinitionError,
    EngineCapabilities,
    EngineLimit,
    Field,
    InvalidConnectionError,
    Operation,
    OperationKind,
    check_arguments,
    check_connection,
    resolve_connection,
    vendor_arguments,
)
from streamwright.core.sdk.operations import files


def validate_sample(connection):
    return ["host: a vendor rule"] if connection.get("host") == "forbidden" else []


SAMPLE = ConnectorDefinition(
    "sample", "Sample", "test",
    connection=(Field("host", "string", required=True), Field("token", "string", secret=True),
                Field("port", "integer", default=443), Field("tls", "boolean", default=True)),
    operations=(Operation(service="item", method="read", arguments=(
        Field("limit", "integer"), Field("mode", "string", choices=("a", "b")))),),
    adapters={"source": "no_such_module_0042:Adapter"},
    validator=f"{__name__}:validate_sample",
)
SOURCE = EngineCapabilities(engine="source", operation_kinds=frozenset({OperationKind.READ}),
                            arguments=MappingProxyType({"item.read": {"limit": None, "mode": ("a",)}}))
FAKE_DISTRIBUTION = SimpleNamespace(name="streamwright-sample", version="0.1",
                                    metadata=MappingProxyType({"StreamWright-SDK-Version": "1.0"}))


def entry(name, value, loaded):
    return SimpleNamespace(name=name, group=plugins.CONNECTORS, value=value, dist=FAKE_DISTRIBUTION,
                           load=lambda: loaded)


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_definitions_are_frozen_and_checked_when_built():
    with pytest.raises(DefinitionError):
        Field("Bad-Name", "string")
    with pytest.raises(DefinitionError):
        Field("token", "integer", secret=True)
    with pytest.raises(DefinitionError):
        Operation(service="item", method="read", arguments=(Field("a", "string"), Field("a", "integer")))
    with pytest.raises(DefinitionError, match="bad kind 'fetch'"):
        Operation(service="item", method="read", kind="fetch")
    with pytest.raises(TypeError):
        Operation("item", "read")  # keyword-only
    with pytest.raises(DefinitionError):
        EngineCapabilities(engine="cli", operation_kinds=frozenset(), arguments={})
    with pytest.raises(DefinitionError):
        ConnectorDefinition("sample", "Sample", "test", connection=(), operations=(), adapters={"cli": "a:B"})
    with pytest.raises(AttributeError):
        SAMPLE.name = "other"
    with pytest.raises(TypeError):
        SAMPLE.adapters["duckdb"] = "a:B"


def test_derived_views_come_from_the_declarations():
    assert [field.name for field in SAMPLE.secret_fields] == ["token"]
    assert [field.name for field in SAMPLE.required_fields] == ["host"]
    assert SAMPLE.engines == {"source"}
    assert set(SAMPLE.json_schema()["properties"]) == {"provider", "host", "token", "port", "tls"}
    operation = SAMPLE.operation("item", "read")
    assert operation.name == "item.read" and operation.kind == OperationKind.READ == "read"
    assert SAMPLE.operations_for(SOURCE) == (operation,) and SAMPLE.operations_for(DUCKDB_CAPABILITIES) == ()
    assert [argument.choices for argument in operation.arguments_for(SOURCE)] == [(), ("a",)]
    assert operation.json_schema(SOURCE)["properties"]["mode"]["anyOf"][0]["enum"] == ["a"]
    assert operation.json_schema()["properties"]["mode"]["anyOf"][0]["enum"] == ["a", "b"]
    description = SAMPLE.describe(SOURCE, DUCKDB_CAPABILITIES)
    assert description["secret_keys"] == ["token"] and description["operations"][0]["kind"] == "read"
    assert description["capabilities"] == {"source": {"item.read": {"limit": None, "mode": ["a"]}}}


def test_a_connector_extends_a_catalog_operation_with_arguments_of_its_own():
    read = files.READ.with_arguments(Field("version_id", "string"), Field("format", "string", choices=("csv",)))
    assert read.extensions == ("version_id",) and files.READ.extensions == ()
    assert read.argument("format").choices == ("csv",) and read.arguments[-1].name == "version_id"
    lake = ConnectorDefinition("lake", "Lake", "test", connection=(), operations=(read,),
                               adapters={"duckdb": "no_such_module_0042:Adapter"})
    assert check_arguments(lake, read, {"path": "a.csv", "version_id": "3"}, DUCKDB_CAPABILITIES) == []
    assert "version_id" in read.json_schema(DUCKDB_CAPABILITIES)["properties"]
    assert codes(check_arguments(lake, read, {"path": "a.csv", "format": "tsv"}, DUCKDB_CAPABILITIES)) == [
        ("bad-value", "arguments.format")]


def test_declared_connections_are_checked_generically():
    assert check_connection(SAMPLE, {"host": "{{ config.host }}", "token": "{{ secrets.t }}", "port": 1}) == []
    # `extra` is now an accepted vendor argument (no near-miss, forwarded verbatim); the rest still fail
    assert codes(check_connection(SAMPLE, {"token": "abc", "port": "x", "tls": "{{ secrets.t }}", "extra": 1})) == [
        ("missing-key", "host"), ("literal-secret", "token"), ("bad-value", "port"), ("secret-placement", "tls")]
    assert codes(check_connection(SAMPLE, ["host"])) == [("bad-type", "")]


def test_vendor_arguments_are_accepted_aliases_and_near_misses():
    vendored = ConnectorDefinition(
        "vend", "Vend", "test",
        connection=(Field("host", "string", required=True), Field("password", "string", secret=True)),
        operations=(Operation(service="item", method="read",
                              arguments=(Field("max_messages", "integer", aliases=("MaxNumberOfMessages",)),)),),
        adapters={"source": "no_such_module_0042:Adapter"},
        secret_key_patterns=("*.password", "sasl.oauthbearer.client.secret"))
    # arbitrary vendor keys forwarded, references allowed; near-miss of a declared field is a warning
    assert check_connection(vendored, {"host": "h", "socket.timeout.ms": 5000, "client.id": "{{ config.id }}"}) == []
    warns = check_connection(vendored, {"host": "h", "hosst": "x"})
    assert [(i.severity, i.code, i.path) for i in warns] == [("warning", "unknown-key", "hosst")]
    # a vendor connection key may bear a secret only under a secret_key_pattern
    assert check_connection(vendored, {"host": "h", "sasl.password": "{{ secrets.p }}"}) == []
    assert codes(check_connection(vendored, {"host": "h", "proxy.token": "{{ secrets.p }}"})) == [
        ("secret-placement", "proxy.token")]
    operation = vendored.operation("item", "read")
    # an operation vendor argument is forwarded; it may not hold a secret
    assert check_arguments(vendored, operation, {"fetch.max.bytes": 1024}) == []
    secret = {"x": "{{ secrets.p }}"}
    assert codes(check_arguments(vendored, operation, secret)) == [("secret-placement", "arguments.x")]
    # an alias is recognised; the alias and its field together is an error
    assert check_arguments(vendored, operation, {"MaxNumberOfMessages": 5}) == []
    assert codes(check_arguments(vendored, operation, {"max_messages": 5, "MaxNumberOfMessages": 5})) == [
        ("alias-conflict", "arguments.max_messages")]
    assert vendor_arguments(operation, {"max_messages": 1, "fetch.max.bytes": 2}) == {"fetch.max.bytes": 2}


def test_vendor_connection_arguments_resolve_and_redact():
    vendored = ConnectorDefinition(
        "vend2", "Vend2", "test",
        connection=(Field("host", "string", required=True),),
        operations=(), adapters={"source": "no_such_module_0042:Adapter"},
        secret_key_patterns=("*.password",))
    connection = resolve_connection(vendored, {"host": "{{ config.host }}", "socket.timeout.ms": "5000",
                                              "sasl.password": "{{ secrets.p }}"},
                                    config={"host": "h"}, secrets={"p": "pw-SECRET"})
    assert connection.vendor == {"socket.timeout.ms": "5000", "sasl.password": "pw-SECRET"}
    assert connection["socket.timeout.ms"] == "5000"
    assert connection.secret_values == ("pw-SECRET",) and connection.redact("x pw-SECRET") == "x ***"


def test_resolving_renders_converts_validates_and_redacts():
    connection = resolve_connection(SAMPLE, {"host": "{{ config.host }}", "token": "{{ secrets.t }}",
                                             "port": "{{ config.port }}"},
                                    config={"host": "db.local", "port": "5432"}, secrets={"t": "tok-SECRET-1"})
    assert dict(connection) == {"host": "db.local", "token": "tok-SECRET-1", "port": 5432, "tls": True}
    assert connection.secret_values == ("tok-SECRET-1",) and connection.redact("x tok-SECRET-1") == "x ***"
    assert "tok-SECRET-1" not in repr(connection)
    with pytest.raises(TypeError):
        connection["host"] = "other"  # read-only
    with pytest.raises(InvalidConnectionError, match="a vendor rule") as caught:
        resolve_connection(SAMPLE, {"host": "{{ config.host }}", "token": "{{ secrets.t }}"},
                           config={"host": "forbidden"}, secrets={"t": "tok-SECRET-1"})
    assert codes(caught.value.issues) == [("bad-value", "")]
    with pytest.raises(InvalidConnectionError, match="tls: expected boolean") as caught:
        resolve_connection(SAMPLE, {"host": "h", "tls": "{{ config.tls }}", "token": "{{ secrets.t }}"},
                           config={"tls": "maybe tok-SECRET-1"}, secrets={"t": "tok-SECRET-1"})
    assert "tok-SECRET-1" not in str(caught.value)


def test_arguments_are_checked_against_an_engines_capabilities():
    operation = SAMPLE.operation("item", "read")
    assert check_arguments(SAMPLE, operation, {"limit": 3, "mode": "a"}, SOURCE) == []
    issues = check_arguments(SAMPLE, operation, {"mode": "b"}, SOURCE)
    assert [(issue.code, issue.path, issue.message) for issue in issues] == [
        ("unsupported-choice", "arguments.mode", "`mode: b` is not supported by the source engine (supported: a)")]
    assert check_arguments(SAMPLE, operation, {"mode": "b"}) == []  # as declared, for any engine
    assert codes(check_arguments(SAMPLE, operation, {"mode": "a"}, DUCKDB_CAPABILITIES)) == [
        ("unsupported-operation", "")]
    narrow = EngineCapabilities(engine="source", operation_kinds=frozenset({OperationKind.READ}),
                                arguments={"item.read": {"limit": None}},
                                checks={"item.read": lambda arguments: ["limit: too high"] * (arguments["limit"] > 9)})
    assert [issue.message for issue in check_arguments(SAMPLE, operation, {"mode": "a"}, narrow)] == [
        "`mode` is not supported by the source engine"]
    assert codes(check_arguments(SAMPLE, operation, {"limit": 10}, narrow)) == [("bad-value", "arguments")]
    # `other` is an accepted vendor argument now, not an unknown-key error
    assert check_arguments(SAMPLE, operation, {"limit": "{{ config.n }}", "other": 1}) == []


def test_load_adapter_reports_unsupported_and_unimportable_engines():
    with pytest.raises(AdapterLoadError, match="has no duckdb adapter"):
        SAMPLE.load_adapter("duckdb")
    with pytest.raises(AdapterLoadError, match="cannot load its source adapter"):
        SAMPLE.load_adapter("source")


def test_entry_points_naming_a_definition_bind_to_the_engines_without_importing_adapters():
    class OldConnector:
        """A plug-in written for the pre-contract connector API."""

    def source():
        return {plugins.CONNECTORS: [entry("sample", "streamwright_sample:CONNECTOR", SAMPLE),
                                     entry("old", "streamwright_old.connector:OldConnector", OldConnector)]}
    assert definition_names(source) == ["old", "sample"]
    assert load_definition("sample", entry_points=source) is SAMPLE
    with pytest.raises(plugins.PluginError, match="not in the allowed list"):
        load_definition("sample", allowed=("old",), entry_points=source)
    with pytest.raises(plugins.PluginError, match=r"old \(.*\) targets the pre-contract connector API; it needs an "
                                                  r"update to connector contract v1 \(docs/design/connector-contract"):
        load_definition("old", entry_points=source)
    adapters, missing = load_adapters(["sample", "old", "absent"], entry_points=source)
    assert adapters == {} and "does not support the duckdb engine" in missing["sample"]
    assert "pre-contract connector API" in missing["old"] and "pip install streamwright-absent" in missing["absent"]
    assert [item["name"] for item in engine_connectors("source", entry_points=source)] == ["sample"]
    # contract v1.2 §6a: a job engine reaches a source-only connector through a call step
    duckdb_reach = engine_connectors("duckdb", entry_points=source)
    assert [(item["name"], item["support"]) for item in duckdb_reach] == [("sample", "call")]


def test_a_registered_definition_is_a_source_connector_with_derived_checks():
    registry = components.ComponentRegistry()
    registry.register(SAMPLE)
    definition = registry.load("sample")
    assert definition is SAMPLE and source_connectors.select_transport(definition) == "sdk"
    assert source_connectors.check_declared_auth(definition, {"host": "h", "token": "written"}) == [
        "auth.token: must be one {{ secrets.NAME }} reference"]
    assert source_connectors.check_call(definition, {"service": "item", "method": "write"}) == [
        "sample: item.write is not supported (supported: item.read)"]
    description = components.describe("sample", registry=registry)
    assert description["secret_keys"] == ["token"] and description["engines"] == ["source"]
    # item.read is no catalog operation: the source engine runs it and passes all its arguments to the adapter
    assert description["transport"] == "sdk" and description["query_builders"] == []
    assert description["capabilities"] == {"source": {"item.read": {"limit": None, "mode": ["a", "b"]}}}


def test_engine_limits_are_declared_once_and_every_check_follows_them():
    store = ConnectorDefinition(
        "store", "Store", "test", connection=(),
        operations=(files.READ, files.WRITE, SAMPLE.operation("item", "read")),
        adapters={"source": "m:A", "duckdb": "m:B"},
        engine_limits=(EngineLimit(engine="duckdb", operation="object.write", choices={"mode": ("overwrite",)}),
                       EngineLimit(engine="duckdb", operation="item.read")))
    write = store.operation("object", "write")
    assert [item.name for item in store.operations_for(DUCKDB_ENGINE)] == ["object.read", "object.write"]
    assert [item.name for item in store.operations_for(SOURCE_CAPABILITIES)] == ["object.read", "item.read"]
    assert codes(check_arguments(store, write, {"path": "p", "format": "csv", "mode": "error"}, DUCKDB_ENGINE)) == [
        ("unsupported-choice", "arguments.mode")]
    assert check_arguments(store, write, {"path": "p", "format": "csv", "mode": "overwrite"}, DUCKDB_ENGINE) == []
    assert codes(check_arguments(store, store.operation("item", "read"), {}, DUCKDB_ENGINE)) == [
        ("unsupported-operation", "")]
    assert store.describe(DUCKDB_ENGINE)["capabilities"]["duckdb"]["object.write"]["mode"] == ["overwrite"]
    with pytest.raises(DefinitionError, match="engine_limits name engines it has an adapter for"):
        ConnectorDefinition("bad", "Bad", "test", connection=(), operations=(files.READ,), adapters={},
                            engine_limits=(EngineLimit(engine="duckdb", operation="object.read"),))
    with pytest.raises(DefinitionError, match="subset of the choices"):
        ConnectorDefinition("bad", "Bad", "test", connection=(), operations=(files.WRITE,), adapters={"duckdb": "m:B"},
                            engine_limits=(EngineLimit(engine="duckdb", operation="object.write",
                                                       choices={"mode": ("merge",)}),))


def test_query_builders_are_listed_by_the_definition_and_imported_on_use():
    definition = ConnectorDefinition("ads", "Ads", "test", connection=(), operations=(), adapters={},
                                     query_builders={"adsql": "no_such_module_0042:Builder"})
    assert definition.describe()["query_builders"] == ["adsql"]
    with pytest.raises(AdapterLoadError, match="cannot load its query builder"):
        definition.load_query_builder("adsql")
    with pytest.raises(DefinitionError, match="query_builders are"):
        ConnectorDefinition("ads", "Ads", "test", connection=(), operations=(), adapters={},
                            query_builders={"Bad Name": "m:B"})


def test_a_definition_with_a_duckdb_adapter_drives_the_duckdb_rules():
    definition = ConnectorDefinition(
        "lake", "Lake", "test", connection=(Field("roots", "list", required=True, item_type="string"),
                                            Field("key", "string", secret=True)),
        operations=(files.READ, files.WRITE), adapters={"duckdb": "no_such_module_0042:Adapter"})
    adapter = DefinitionDuckDBAdapter(definition)
    assert adapter.secret_keys == ("key",)
    assert sorted(adapter.reads) == sorted(files.READ_FORMATS) and "match" not in adapter.reads["csv"]
    target = adapter.write_target({})
    assert (target.modes, target.replay_safe, target.required) == (("error", "overwrite"), ("overwrite",),
                                                                    {"*": ("path", "format")})
    assert "single_file" not in target.keys and "partition_by" in target.keys
    assert adapter.validate("l", {"provider": "lake", "roots": "x"}) == [("bad-value", "roots: expected list, got str",
                                                                          False)]


def _surface_probe(service, method):
    """A stand-in vendor surface: only `get_real` exists (contract v1.3 §6b)."""
    return [] if method == "get_real" else ["%s has no %s method" % (service, method)]


def test_operation_families_bind_to_concrete_names_and_report_as_patterns():
    family = Operation(service="*Service", method="get_*", kind=OperationKind.READ,
                       arguments=(Field("id", "string", required=True),))
    definition = ConnectorDefinition("dyn", "Dyn", "test", connection=(), operations=(family,),
                                     adapters={"source": "m:A"})
    assert family.is_pattern and not definition.operation("CustomerService", "get_user").is_pattern
    bound = definition.operation("CustomerService", "get_user")
    assert (bound.service, bound.method, bound.name) == ("CustomerService", "get_user", "CustomerService.get_user")
    assert definition.runs(bound, SOURCE_CAPABILITIES)
    assert definition.operation("CustomerService", "mutate_user") is None  # no family matches a write method
    assert definition.describe()["operations"][0]["pattern"] is True


def test_the_surface_catches_a_method_the_installed_library_lacks():
    family = Operation(service="*Service", method="get_*", kind=OperationKind.READ,
                       arguments=(Field("id", "string", required=True),))
    definition = ConnectorDefinition("dyn", "Dyn", "test", connection=(), operations=(family,),
                                     adapters={"source": "m:A"}, surface="%s:_surface_probe" % __name__)
    real = definition.operation("CustomerService", "get_real")
    typo = definition.operation("CustomerService", "get_wrong")
    assert check_arguments(definition, real, {"id": "1"}, SOURCE_CAPABILITIES) == []
    issues = check_arguments(definition, typo, {"id": "1"}, SOURCE_CAPABILITIES)
    assert [issue.code for issue in issues] == ["unknown-operation"]
    # a definition whose surface library is not importable is skipped silently (validation stays offline-friendly)
    missing = ConnectorDefinition("dyn", "Dyn", "test", connection=(), operations=(family,),
                                  adapters={"source": "m:A"}, surface="no_such_module_0042:surface")
    assert missing.load_surface() is None
