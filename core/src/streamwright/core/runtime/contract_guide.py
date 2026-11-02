"""
A connector's user guide (rule 9 of docs/design/connector-contract.md): the `guide.md` its package ships, with the
tables of connection fields and operation arguments generated from its definition after the `## Connection` and
`## Operations` headings - the prose is written once, the tables are never restated. Given the engines'
EngineCapabilities, an operation's table says which engines take each argument (and the choices they support).
"""

from collections.abc import Iterable
from importlib.resources import files
from typing import Final

from streamwright.core.runtime.contract import ConnectorDefinition, EngineCapabilities, Field, Operation, types_of
from streamwright.core.runtime.definitions import engine_reach

__all__ = ["connection_table", "engines_table", "guide_text", "operation_table"]

SECTION_TABLES: Final = ("Connection", "Operations", "Engines")


def _type_text(field: Field) -> str:
    text = " or ".join(types_of(field.type))
    return "%s of %s" % (text, field.item_type) if field.item_type else text


def _rule_text(field: Field) -> str:
    rules = ["required"] if field.required else []
    if field.secret:
        rules.append("a `{{ secrets.NAME }}` reference")
    if field.choices:
        rules.append("one of " + ", ".join("`%s`" % choice for choice in field.choices))
    if field.default is not None:
        rules.append("default `%s`" % (str(field.default).lower() if isinstance(field.default, bool) else
                                       field.default))
    return "; ".join(rules)


def _row(field: Field, extra: tuple[str, ...] = ()) -> str:
    cells = ["`%s`" % field.name, _type_text(field), _rule_text(field), field.description.replace("|", "\\|")]
    return "| %s |" % " | ".join(cells + list(extra))


def _describe_engines(operation: Operation, field: Field, capabilities: tuple[EngineCapabilities, ...]) -> str:
    """The engines that take an argument; `engine (a, b)` where an engine supports only some of its choices."""
    names = []
    for engine in capabilities:
        if engine.supports(operation, field.name):
            choices = engine.choices(operation, field.name)
            narrowed = choices is not None and set(field.choices) - set(choices)
            supported = [choice for choice in field.choices if choice in (choices or ())]
            names.append("%s (%s)" % (engine.engine, ", ".join(map(str, supported))) if narrowed else engine.engine)
    return ", ".join(names)


def connection_table(definition: ConnectorDefinition) -> str:
    rows = [_row(field) for field in definition.connection]
    return "\n".join(["| Key | Type | Rules | Meaning |", "|---|---|---|---|", *rows])


def engines_table(definition: ConnectorDefinition) -> str:
    """How each engine reaches the connector (contract v1.2 §6a): natively (an adapter) or through a call step."""
    native = set(definition.engines)
    rows = []
    for tag in engine_reach(definition):
        engine = tag.split(" ", 1)[0]
        how = "natively (an adapter)" if engine in native else "through a call step (its source adapter)"
        rows.append("| `%s` | %s |" % (engine, how))
    return "\n".join(["| Engine | How |", "|---|---|", *rows])


def operation_table(operation: Operation, capabilities: Iterable[EngineCapabilities] = ()) -> str:
    """The table of an operation's arguments; with the engines' capabilities, an Engines column."""
    engines = tuple(item for item in capabilities if item.runs(operation))
    pattern = "\n\n_`%s` is a pattern: the installed SDK decides which concrete services and methods exist._" % (
        operation.name) if operation.is_pattern else ""
    heading = "### `%s` (%s%s)\n\n%s%s\n" % (operation.name, operation.kind.value, "; engines: %s" % ", ".join(
        item.engine for item in engines) if engines else "", operation.description, pattern)
    if not engines:
        rows = [_row(field) for field in operation.arguments]
        return "\n".join([heading, "| Argument | Type | Rules | Meaning |", "|---|---|---|---|", *rows])
    rows = [_row(field, (_describe_engines(operation, field, engines),)) for field in operation.arguments]
    return "\n".join([heading, "| Argument | Type | Rules | Meaning | Engines |", "|---|---|---|---|---|", *rows])


def _tables(definition: ConnectorDefinition, section: str, capabilities: tuple[EngineCapabilities, ...]) -> str:
    if section == "Connection":
        return connection_table(definition)
    if section == "Engines":
        return engines_table(definition)
    return "\n\n".join(
        operation_table(operation, [definition.capabilities_for(item) for item in capabilities
                                    if operation in definition.operations_for(item)])
        for operation in definition.operations)


def guide_text(definition: ConnectorDefinition, package: str, capabilities: Iterable[EngineCapabilities] = ()) -> str:
    """
    The guide of the connector whose definition lives in `package`, with its generated tables (`capabilities`: the
    engines to show, of those the connector has an adapter for).
    """
    engines = tuple(item for item in capabilities if item.engine in definition.engines)
    text = files(package).joinpath(definition.guide).read_text(encoding="utf-8")
    blocks = text.split("\n## ")
    for index, block in enumerate(blocks[1:], start=1):
        title, _, body = block.partition("\n")
        if title.strip() in SECTION_TABLES:
            blocks[index] = "%s\n%s\n\n%s\n" % (title, body.rstrip("\n"), _tables(definition, title.strip(), engines))
    return "\n## ".join(blocks)
