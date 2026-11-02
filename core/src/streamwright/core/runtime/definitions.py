"""
Discovery of connector definitions: the `streamwright.connectors` entry points, whose target is a connector package's
`CONNECTOR` (rule 1 of docs/design/connector-contract.md). Importing a definition imports only the connector's `spec`
(guarantee G3), so engines can list what each connector supports; adapters and query builders load later, through
`ConnectorDefinition.load_adapter(engine)` / `.load_query_builder(name)`. A plug-in whose target is not a definition
was written for the pre-contract connector API and is refused with a message that says so.
"""

from collections.abc import Callable, Collection
from typing import Any, Final

from streamwright.core.runtime import plugins
from streamwright.core.runtime.contract import ConnectorDefinition

__all__ = ["CONTRACT_DOCUMENT", "ENGINE_ORDER", "definition_names", "definitions_for", "engine_connectors",
           "engine_reach", "load_definition"]

CONTRACT_DOCUMENT: Final = "docs/design/connector-contract.md"
# the fixed display order of engines (contract v1.2 §6a): source, then the job engines
ENGINE_ORDER: Final = ("source", "duckdb", "spark")

EntryPoints = Callable[[], Any] | None


def definition_names(entry_points: EntryPoints = None) -> list[str]:
    """The installed connectors (entry-point metadata only, nothing imported)."""
    return sorted(entry.name for entry in plugins.entries(plugins.CONNECTORS, entry_points))


def _distribution(name: str, entry_points: EntryPoints) -> str:
    entry = next((item for item in plugins.discover(plugins.CONNECTORS, entry_points=entry_points)
                  if item["name"] == name), {})
    return entry.get("distribution") or "unknown distribution"


def load_definition(name: str, *, allowed: Collection[str] | None = None,
                    entry_points: EntryPoints = None) -> ConnectorDefinition:
    """The connector's definition (allow-list and SDK compatibility checked before the import); PluginError."""
    definition = plugins.load_plugin(plugins.CONNECTORS, name, allowed=allowed, entry_points=entry_points)
    if not isinstance(definition, ConnectorDefinition):
        raise plugins.PluginError("%s (%s) targets the pre-contract connector API; it needs an update to connector "
                                  "contract v1 (%s)" % (name, _distribution(name, entry_points), CONTRACT_DOCUMENT))
    if definition.name != name:
        raise plugins.PluginError("%s %r defines the connector %r" % (plugins.CONNECTORS, name, definition.name))
    return definition


def definitions_for(engine: str, names: Collection[str], *, allowed: Collection[str] | None = None,
                    entry_points: EntryPoints = None) -> tuple[dict[str, ConnectorDefinition], dict[str, str]]:
    """
    ({name: definition supporting `engine`}, {name: why not}) for the `names` that are installed connectors; names
    that are not installed are in neither.
    """
    found: dict[str, ConnectorDefinition] = {}
    missing: dict[str, str] = {}
    for name in sorted(set(names) & set(definition_names(entry_points))):
        try:
            definition = load_definition(name, allowed=allowed, entry_points=entry_points)
        except plugins.PluginError as exc:
            missing[name] = str(exc)
            continue
        if engine in definition.engines:
            found[name] = definition
        else:
            missing[name] = "connector %r does not support the %s engine (engines: %s)" % (
                name, engine, ", ".join(sorted(definition.engines)) or "none")
    return found, missing


def engine_connectors(engine: str, *, entry_points: EntryPoints = None) -> list[dict[str, Any]]:
    """
    Name, distribution and version of the installed connectors an `engine` (`source`, `duckdb`, `spark`) can use:
    natively (it has an adapter for the engine) OR, for a job engine, through a *call step* that runs the connector's
    source adapter. Each entry's `support` is `"native"` or `"call"`; only specs are imported.
    """
    names = definition_names(entry_points)
    native, _ = definitions_for(engine, names, entry_points=entry_points)
    callable_: dict[str, Any] = {}
    if engine in ("duckdb", "spark"):                       # a job engine runs any source connector as a call step
        found, _ = definitions_for("source", names, entry_points=entry_points)
        callable_ = {name: definition for name, definition in found.items() if name not in native}
    entries = []
    for entry in plugins.discover(plugins.CONNECTORS, entry_points=entry_points):
        name = entry["name"]
        if name in native:
            entries.append({**entry, "support": "native"})
        elif name in callable_:
            entries.append({**entry, "support": "call"})
    return entries


def engine_reach(definition: ConnectorDefinition) -> list[str]:
    """
    How each engine can use a connector, in the fixed order source, duckdb, spark (contract v1.2 §6a): `"duckdb"`
    when native, `"duckdb (call)"` when only through a source-adapter call step, omitted when unreachable.
    `e.g. ["source", "duckdb (call)", "spark (call)"]` for a source-only connector, `["source", "duckdb", "spark"]`
    for one with every adapter.
    """
    source_capable = "source" in definition.engines
    tags = []
    for engine in ENGINE_ORDER:
        if engine in definition.engines:
            tags.append(engine)
        elif engine != "source" and source_capable:
            tags.append("%s (call)" % engine)
    return tags
