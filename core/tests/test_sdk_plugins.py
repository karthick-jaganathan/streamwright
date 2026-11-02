"""Public import paths and the metadata-before-import plug-in boundary."""

from types import SimpleNamespace

import pytest

from streamwright.core import sdk
from streamwright.core.config.loader import load_source
from streamwright.core.engine.runner import SourceRunner
from streamwright.core.net.http import Redactor
from streamwright.core.outputs.output import SingerOutput
from streamwright.core.outputs.staging import sql_string
from streamwright.core.runtime import components, plugins
from streamwright.core.runtime.components import ConnectorContext
from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation
from streamwright.core.validation.engine import validate_source


def fake_entry(name, *, sdk_version="1.0", group=plugins.CONNECTORS, target=None):
    calls = []

    def load():
        calls.append(name)
        return object() if target is None else target

    entry = SimpleNamespace(
        name=name, group=group, dist=SimpleNamespace(name="streamwright-example", version="0.2",
                                                      metadata={"StreamWright-SDK-Version": sdk_version}),
        load=load,
    )
    return entry, calls


def definition(name, **fields):
    return ConnectorDefinition(name=name, title=name, family="test", connection=(), operations=(), adapters={},
                               **fields)


def test_sdk_pins_released_paths():
    assert sdk.ConnectorContext is ConnectorContext
    assert sdk.Redactor is Redactor
    assert sdk.sql_string is sql_string
    assert sdk.SDK_VERSION and sdk.PAGE_SIZE > 0
    assert callable(load_source) and callable(validate_source)
    assert SourceRunner and SingerOutput
    assert set(sdk.__all__) == {
        "ConnectorContext", "ConnectorError", "QueryBuilder",
        "BuiltQuery", "QueryError", "InputError", "as_date", "Redactor", "quote_ident",
        "quote_literal", "sql_string", "PAGE_SIZE", "SDK_VERSION", "ConnectorDefinition", "Field", "Operation",
        "ResolvedConnection", "check_arguments", "check_connection", "resolve_connection",
    }


def test_metadata_listing_and_allowlist_do_not_import():
    entry, calls = fake_entry("sample")

    def source():
        return {plugins.CONNECTORS: [entry]}
    assert plugins.discover(plugins.CONNECTORS, entry_points=source) == [
        {"name": "sample", "distribution": "streamwright-example", "version": "0.2"}]
    with pytest.raises(plugins.PluginError, match=r"not in the allowed list.*streamwright-example"):
        plugins.load_plugin(plugins.CONNECTORS, "sample", allowed={"other"}, entry_points=source)
    assert not calls


def test_incompatible_sdk_refused_before_import():
    entry, calls = fake_entry("sample", sdk_version="2.0")
    with pytest.raises(plugins.PluginError, match=r"requires StreamWright SDK 2\.0"):
        plugins.load_plugin(plugins.CONNECTORS, "sample", entry_points=lambda: {plugins.CONNECTORS: [entry]})
    assert not calls


def test_compatible_plugin_loads_and_checks_type():
    entry, calls = fake_entry("sample")

    def source():
        return {plugins.CONNECTORS: [entry]}
    with pytest.raises(plugins.PluginError, match="invalid type"):
        plugins.load_plugin(plugins.CONNECTORS, "sample", expected_type=ConnectorDefinition, entry_points=source)
    assert calls == ["sample"]


def test_core_requirement_handshake_rejects_incompatible_distribution(monkeypatch):
    entry, calls = fake_entry("sample")
    entry.dist.metadata = {}
    entry.dist.requires = ["streamwright>=9.0,<10.0"]
    monkeypatch.setattr(plugins.importlib.metadata, "version", lambda name: "0.1.8")
    with pytest.raises(plugins.PluginError, match="requires streamwright"):
        plugins.load_plugin(plugins.CONNECTORS, "sample", entry_points=lambda: {plugins.CONNECTORS: [entry]})
    assert not calls


def test_a_pre_contract_connector_is_refused_with_what_it_needs(monkeypatch):
    entry, calls = fake_entry("old_style")
    monkeypatch.setattr(components, "_entry_points", lambda: [entry])
    with pytest.raises(components.ComponentLoadError, match=r"old_style \(streamwright-example\) targets the "
                                                            r"pre-contract connector API; it needs an update to "
                                                            r"connector contract v1"):
        components.ComponentRegistry().load("old_style")
    assert calls == ["old_style"]


def test_query_builders_come_from_allowed_definitions_only(monkeypatch):
    trusted = definition("trusted", query_builders={"trusted_query": "trusted.queries:Builder"})
    foreign, calls = fake_entry("foreign", target=definition("foreign", query_builders={
        "trusted_query": "foreign.queries:Builder"}))
    registry = components.ComponentRegistry()
    registry.register(trusted)
    monkeypatch.setattr(components, "_entry_points", lambda: [foreign])
    assert components.query_builders(allowed=["trusted"], registry=registry) == {"trusted_query": trusted}
    assert not calls  # a connector that is not allowed is never imported
    with pytest.raises(components.ComponentLoadError, match="has no query builder 'other'"):
        components.load_query_builder("other", {"other": trusted}, registry=registry)
    with pytest.raises(components.ComponentLoadError, match="not offered by an installed, allowed connector"):
        components.load_query_builder("missing", {}, registry=registry)


def test_registry_instances_are_isolated():
    registry = components.ComponentRegistry()
    registry.register(definition("isolated"))
    assert registry.load("isolated").name == "isolated"
    assert "isolated" in registry.available()
    assert "isolated" not in components.available()
    registry.unregister("isolated")
    assert "isolated" not in registry.available()
    with pytest.raises(TypeError, match="is not a ConnectorDefinition"):
        registry.register(object())


def test_uninstalled_entry_point_is_not_served_from_registry_cache(monkeypatch):
    entry, _ = fake_entry("reloadable", target=definition("reloadable"))
    installed = [entry]
    monkeypatch.setattr(components, "_entry_points", lambda: installed)
    registry = components.ComponentRegistry()
    assert registry.load("reloadable").name == "reloadable"
    installed.clear()
    with pytest.raises(components.ComponentLoadError, match="not installed"):
        registry.load("reloadable")


def test_cached_entry_point_is_rechecked_after_sdk_metadata_changes(monkeypatch):
    entry, _ = fake_entry("reloadable", target=definition("reloadable"))
    monkeypatch.setattr(components, "_entry_points", lambda: [entry])
    registry = components.ComponentRegistry()
    registry.load("reloadable")
    entry.dist.metadata["StreamWright-SDK-Version"] = "2.0"
    with pytest.raises(components.ComponentLoadError, match="requires StreamWright SDK"):
        registry.load("reloadable")


def test_describe_uses_the_declared_contract_without_connecting():
    registry = components.ComponentRegistry()
    registry.register(ConnectorDefinition(
        name="described", title="Described", family="test", connection=(Field("token", "string", required=True,
                                                                               secret=True),),
        operations=(Operation(service="reports", method="list", arguments=(Field("limit", "integer"),)),),
        adapters={"source": "nowhere.at_all:Adapter"}))
    metadata = components.describe("described", registry=registry)
    assert metadata["kind"] == "connector" and metadata["transport"] == "sdk"
    assert metadata["required_keys"] == ["token"] and metadata["secret_keys"] == ["token"]
    assert metadata["capabilities"]["source"] == {"reports.list": {"limit": None}}
    assert components.describe("described", allowed=["other"], registry=registry) is None
