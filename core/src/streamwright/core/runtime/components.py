#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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


"""
Connectors (docs/design/connector-contract.md) as the source engine and the validators reach them: the connector
definitions registered by connector packages in the `streamwright.connectors` entry-point group, and the query
builders those definitions list. In a connector's pyproject.toml:

    [project.entry-points."streamwright.connectors"]
    google_ads = "streamwright.connectors.google_ads:CONNECTOR"

and in its spec (`{gaql: {...}}` in a request calls that query builder):

    ConnectorDefinition(..., query_builders={"gaql": "streamwright.connectors.google_ads.gaql:GaqlBuilder"})

Source files can only use installed (and allowed) connectors; a source can call the query builders of the connectors
it may use. Nothing is imported before the allow-list admits it.
"""

from streamwright.core.engine import queries
from streamwright.core.runtime import plugins, source_connectors
from streamwright.core.runtime.connector_types import ComponentLoadError, ConnectorContext, ConnectorError, QueryBuilder
from streamwright.core.runtime.contract import AdapterLoadError, ConnectorDefinition
from streamwright.core.runtime.definitions import load_definition

__all__ = [
    "ENTRY_POINT_GROUP",
    "ComponentLoadError",
    "ComponentRegistry",
    "ConnectorContext",
    "ConnectorError",
    "QueryBuilder",
    "available",
    "check_call",
    "check_queries",
    "check_source",
    "component_problems",
    "declared_auth",
    "describe",
    "load",
    "load_query_builder",
    "path_text",
    "poll_call",
    "query_builders",
    "register",
    "request_body",
    "sdk_call",
    "sdk_calls",
    "unregister",
]

ENTRY_POINT_GROUP = plugins.CONNECTORS


# * --------
# * registry
# * --------
class ComponentRegistry:
    """Isolated connector registration and load caches for embedding and tests."""

    def __init__(self):
        self.registered = {}  # name -> ConnectorDefinition registered in this process
        self.loaded = {}      # name -> ConnectorDefinition loaded from its entry point
        self.builders = {}    # (connector, query builder) -> QueryBuilder

    def register(self, definition):
        register(definition, registry=self)

    def unregister(self, name):
        unregister(name, registry=self)

    def available(self):
        return available(registry=self)

    def load(self, name, allowed=None):
        return load(name, allowed=allowed, registry=self)


_DEFAULT_REGISTRY = ComponentRegistry()


def register(definition, registry=None):
    """Registers a connector definition in this process (for tests and embedding; installs use entry points)."""
    if not isinstance(definition, ConnectorDefinition):
        raise TypeError("%r is not a ConnectorDefinition" % (definition,))
    (registry or _DEFAULT_REGISTRY).registered[definition.name] = definition


def unregister(name, registry=None):
    registry = registry or _DEFAULT_REGISTRY
    registry.registered.pop(name, None)
    registry.loaded.pop(name, None)
    for key in [key for key in registry.builders if key[0] == name]:
        registry.builders.pop(key)


def _entry_points():
    return plugins.entries(ENTRY_POINT_GROUP)


def available(registry=None):
    """Names of the connectors that can be loaded (registered or installed); nothing is imported."""
    return sorted(set((registry or _DEFAULT_REGISTRY).registered) | {entry.name for entry in _entry_points()})


def package_name(name):
    return "streamwright-" + name.replace("_", "-")


def _not_allowed(name, allowed):
    return ComponentLoadError("connector %r is not in the allowed list (%s)" % (name, ", ".join(allowed) or "empty"))


def _not_installed(name):
    return ComponentLoadError("connector %r is not installed; install it with: pip install %s" % (
        name, package_name(name)))


def _cached(name, cached):
    """A loaded definition whose distribution is still installed and compatible."""
    current = next((entry for entry in _entry_points() if entry.name == name), None)
    if current is None:
        cached.pop(name, None)
        raise _not_installed(name)
    try:
        plugins.check_compatibility(current)
    except plugins.PluginError as exc:
        cached.pop(name, None)
        raise ComponentLoadError(str(exc)) from exc
    return cached[name]


def load(name, allowed=None, registry=None):
    """The ConnectorDefinition of `name`; ComponentLoadError if it is not allowed, not installed or not a definition."""
    registry = registry or _DEFAULT_REGISTRY
    if allowed is not None and name not in allowed:
        raise _not_allowed(name, allowed)
    if name in registry.registered:
        return registry.registered[name]
    if name in registry.loaded:
        return _cached(name, registry.loaded)
    if not any(entry.name == name for entry in _entry_points()):
        raise _not_installed(name)
    try:
        definition = load_definition(name, entry_points=lambda: {ENTRY_POINT_GROUP: _entry_points()})
    except plugins.PluginError as exc:
        raise ComponentLoadError(str(exc)) from exc
    registry.loaded[name] = definition
    return definition


def query_builders(allowed=None, registry=None):
    """
    {query builder name: the ConnectorDefinition that lists it} of the connectors a source may use (the allowed ones;
    every installed one without an allow-list). Connectors that cannot be loaded offer none.
    """
    names = available(registry) if allowed is None else [name for name in available(registry) if name in allowed]
    found = {}
    for name in names:
        try:
            definition = load(name, allowed, registry=registry)
        except ComponentLoadError:
            continue
        for builder in definition.query_builders:
            found.setdefault(builder, definition)
    return found


def load_query_builder(name, builders, registry=None):
    """The QueryBuilder `name` of the connector that lists it in `builders` (query_builders()); ComponentLoadError."""
    registry = registry or _DEFAULT_REGISTRY
    definition = builders.get(name)
    if definition is None:
        raise ComponentLoadError("query builder %r is not offered by an installed, allowed connector" % name)
    key = (definition.name, name)
    if key not in registry.builders:
        try:
            builder = definition.load_query_builder(name)
        except AdapterLoadError as exc:
            raise ComponentLoadError(str(exc)) from exc
        if not isinstance(builder, QueryBuilder):
            raise ComponentLoadError("connector %s: query builder %r is not a QueryBuilder" % (definition.name, name))
        registry.builders[key] = builder
    return registry.builders[key]


def describe(name, allowed=None, registry=None):
    """The connector's declared contract for the source engine, without connecting; None when it cannot be loaded."""
    try:
        definition = load(name, allowed=allowed, registry=registry)
    except ComponentLoadError:
        return None
    return source_connectors.describe_connector(definition)


# * ------
# * checks
# * ------

def declared_auth(source):
    """The source's auth block as written (secret references, not values), without `provider`."""
    return {key: value for key, value in (source.get("auth") or {}).items() if key != "provider"}


def sdk_call(request):
    """The {service, method, arguments[, headers]} call of an sdk request (`headers` only to refuse them)."""
    call = {key: request.get(key) for key in ("service", "method", "arguments")}
    if request.get("headers") is not None:
        call["headers"] = request["headers"]
    return call


def poll_call(submit, poll):
    """An async job's poll call: the submit's service, the poll's method and arguments."""
    return {"service": submit.get("service"), "method": poll.get("method"), "arguments": poll.get("arguments")}


def request_body(item):
    """What a stream's `requests` item sends: the item without its name, records, paginator and partitions."""
    return {key: value for key, value in item.items() if key not in ("name", "records", "paginator",
                                                                           "partitions")}


def sdk_calls(request):
    """The calls an unrendered request (a request_body) makes through its connector (see sdk_call)."""
    if "sdk" in request:
        return [sdk_call(request)]
    job = request.get("async_job")
    if not job:
        return []
    submit = job.get("submit") or {}
    calls = sdk_calls(submit)
    if calls and job.get("poll"):
        calls.append(poll_call(submit, job["poll"]))
    return calls + sdk_calls(job.get("results") or {})


def check_call(definition, call, builders=None, registry=None):
    """
    Problems with an unrendered call of connector `definition`: the source engine's checks of its operation and
    arguments, which see the calls of query builders (`builders`: names; default: those of the usable connectors) as
    BuiltQuery values. Per-request values are arguments: sdk requests take no `headers`.
    """
    problems = []
    if call.get("headers") is not None:
        problems.append("%s: %s.%s: sdk requests take no `headers`; per-request values are the operation's "
                        "`arguments`" % (definition.name, call.get("service"), call.get("method")))
    names = set(query_builders(registry=registry) if builders is None else builders)
    call = queries.replace_calls({key: value for key, value in call.items() if key != "headers"}, names,
                                 lambda name, spec: queries.BuiltQuery(name))
    return problems + source_connectors.check_call(definition, call)


def check_queries(request, allowed=None, builders=None, registry=None):
    """
    Problems with the query builder calls in an unrendered request, as (path inside the request, message, missing):
    a builder that cannot be loaded, and the builder's own checks. `builders`: query_builders(allowed) by default.
    """
    builders = query_builders(allowed, registry=registry) if builders is None else builders
    problems = []
    for path, name, spec in queries.find_calls(request, set(builders)):
        try:
            builder = load_query_builder(name, builders, registry=registry)
        except ComponentLoadError as exc:
            problems.append(((*path, name), str(exc), False))
            continue
        problems += [((*path, name, *tuple(inner)), "%s: %s" % (name, message), False)
                     for inner, message in builder.check(spec)]
    return problems


def _auth_problems(source, allowed, registry):
    """(connector definition or None, problems) of the source's `auth.provider` and its declared auth block."""
    provider = (source.get("auth") or {}).get("provider")
    if not provider:
        return None, []
    try:
        definition = load(provider, allowed, registry=registry)
    except ComponentLoadError as exc:
        missing = provider not in available(registry=registry) and (allowed is None or provider in allowed)
        return None, [(("auth", "provider"), str(exc), missing)]
    return definition, [(("auth",), problem, False)
                        for problem in source_connectors.check_declared_auth(definition, declared_auth(source))]


def component_problems(source, allowed=None, registry=None):
    """
    What stops a source from running here, before it starts, as (path in the source, message, missing): a connector
    that is not installed (`missing`), not allowed or broken, auth keys or calls its connector does not support, and
    query builder checks, for every `requests` item of every stream (as streamwright run checks them).
    """
    definition, problems = _auth_problems(source, allowed, registry)
    builders = query_builders(allowed, registry=registry)
    for index, stream in enumerate(source.get("streams") or []):
        items = stream.get("requests") if isinstance(stream, dict) else None
        for position, item in enumerate(items if isinstance(items, list) else []):
            if not isinstance(item, dict):
                continue
            request, path = request_body(item), ("streams", index, "requests", position)
            problems += [(path + inner, message, missing) for inner, message, missing in
                         check_queries(request, allowed, builders, registry=registry)]
            if definition is not None:
                problems += [(path, problem, False) for call in sdk_calls(request)
                             for problem in check_call(definition, call, builders, registry=registry)]
    return problems


def path_text(path):
    """("arguments", "where", 0) -> "arguments.where[0]"."""
    text = ""
    for part in path:
        text += "[%d]" % part if isinstance(part, int) else ("." if text else "") + str(part)
    return text


def check_source(source, allowed=None, registry=None):
    """component_problems() as messages, e.g. "stream 'campaigns': requests[0].arguments.query.gaql.limit: ..."."""
    streams = source.get("streams") or []
    messages = []
    for path, message, _ in component_problems(source, allowed, registry=registry):
        if path[:1] == ("streams",):
            inner = path[2:]
            where = " %s:" % path_text(inner) if len(inner) > 1 else ""
            messages.append("stream %r:%s %s" % (streams[path[1]].get("name"), where, message))
        else:
            messages.append(message)
    return messages
