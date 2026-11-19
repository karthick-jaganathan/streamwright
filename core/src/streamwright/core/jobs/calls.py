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

"""
Connector *call steps*, shared by every job engine (connector contract v1.2 §6a, "engine reach").

A call step runs a source connector through the source engine (`SourceRunner.collect`) on the driver and loads the
records it returns. DuckDBJob and SparkJob use the SAME code here — the only difference is where the records land (a
DuckDB table, a Spark DataFrame). The step shape is identical to a DuckDBJob call step: a `connection`, and either a
connector call (internally `service`/`method` with `arguments`) or an HTTP request (internally `request`): a
manifest's `operation:`, optionally paged (`paginator`), throttled (`rate_limit`), retried (`retry`); the
manifest grammar is translated by streamwright.core.jobs.grammar.
"""

import itertools
import json

from streamwright.core.config.inputs import resolve_inputs
from streamwright.core.engine.runner import SourceRunner
from streamwright.core.jobs.grammar import CONNECTION_HTTP_KEYS, CONNECTION_POLICY_KEYS, public_connection_key
from streamwright.core.runtime import components
from streamwright.core.runtime.contract_validation import check_connection
from streamwright.core.runtime.source_connectors import select_transport
from streamwright.core.runtime.text import suggest

__all__ = ["HTTP_DEFAULT_KEYS", "PARTITION_KEYS", "call_problems", "call_records", "connection_problems",
           "ephemeral_source", "partition_dependencies", "partition_problems", "partition_scopes", "record_value",
           "source_inputs"]

# a REST connection's request defaults, flat in the internal shape (a manifest's `http:` and `policy:` groups)
HTTP_DEFAULT_KEYS = (*CONNECTION_HTTP_KEYS, *CONNECTION_POLICY_KEYS)
_ABSOLUTE = ("http://", "https://", "{{")

# a call step's `partitions: [{from, field, name}]`: one call per distinct `field` value of the earlier step `from`
# (crossed when there are several), the value read as `{{ partition.<name> }}` (`name` defaults to `field`)
PARTITION_KEYS = ("from", "field", "name")


def partition_problems(step):
    """Problems with a step's `partitions` as (code, message, is_warning)."""
    return [("unsupported-key", "partitions[%d]: keys are %s" % (index, ", ".join(PARTITION_KEYS)), False)
            for index, part in enumerate(step.get("partitions") or ())
            if isinstance(part, dict) and set(part) - set(PARTITION_KEYS)]


def partition_dependencies(item):
    """The earlier steps a step's `partitions` read their values from."""
    return [part["from"] for part in item.get("partitions") or () if isinstance(part, dict) and "from" in part]


def partition_scopes(step, values):
    """
    The partition values of a call step, crossed: `[{name: value}]` (one empty scope without `partitions`).
    `values(index, part)` is the engine's distinct, non-null values of `part["field"]` in the step `part["from"]`,
    pinned in the batch so a replay calls with the same values.
    """
    dimensions = [[(part.get("name", part["field"]), value) for value in values(index, part)]
                  for index, part in enumerate(step.get("partitions") or ())]
    return [dict(combination) for combination in itertools.product(*dimensions)]


def ephemeral_source(step, connection, scope=None):
    """An ephemeral source with one stream: the step's call, its records as returned by the connector."""
    request = {"name": "raw"}
    if "request" in step:
        spec = step["request"]
        request["http"] = {k: spec[k] for k in ("path", "method", "params", "headers", "json") if k in spec}
        if "records" in spec:
            request["records"] = spec["records"]
    else:
        request.update(sdk=connection["provider"], service=step.get("service"), method=step.get("method"),
                       arguments=step.get("arguments") or {})
    if "paginator" in step:
        request["paginator"] = step["paginator"]
    stream = {"name": step["name"], "requests": [request],
              "transform": {"mode": "page", "steps": [{"name": "rows", "select": "SELECT record FROM raw"}]},
              "export": {step["name"]: {"step": "rows"}}}
    for key in ("retry", "rate_limit", "timeout", "on_error"):
        if key in step:
            stream[key] = step[key]
    if scope:
        stream["partitions"] = [{"name": name, "values": [value]} for name, value in scope.items()]
    http = {k: connection[k] for k in HTTP_DEFAULT_KEYS if k in connection}
    auth = {k: v for k, v in connection.items() if k not in http}
    return {"kind": "source", "name": "step_%s" % step["name"], "auth": auth, "http": http, "streams": [stream]}


def source_inputs(source, provided, secrets):
    """A source folder's resolved (config, secrets) for a `source:` step."""
    spec = source.get("spec") or {}
    declared = spec.get("secrets") or {}
    config = resolve_inputs(spec.get("config") or {}, provided, "config")
    secret_values = resolve_inputs(declared, {n: secrets[n] for n in declared if n in secrets}, "secrets",
                                   hide_values=True)
    return config, secret_values


def record_value(row):
    """A call step's record as the connector returned it (the `record` column of the ephemeral stream)."""
    value = row.get("record") if isinstance(row, dict) else row
    return json.loads(value) if isinstance(value, str) else value


def call_records(step, connection, config, secrets, *, allowed_connectors=None, state=None, scope=None):
    """
    Run a connector call step on the driver for one partition `scope` (`{name: value}`, see partition_scopes) and
    return `(records, collected)`: `records` is a lazy iterator of the connector's records (as returned), `collected`
    is the `CollectedStream` whose `.state`/`.partial` are final once `records` is exhausted. The caller loads
    `records` into its engine in bounded batches.
    """
    runner = SourceRunner(ephemeral_source(step, connection, scope), config, secrets,
                          allowed_connectors=allowed_connectors)
    collected = runner.collect(step["name"], state=state)

    def records():
        for _, record in collected:
            yield record_value(record)

    return records(), collected


def _definition(provider):
    try:
        return components.load(provider)
    except components.ComponentLoadError:
        return None                       # not installed or broken: the engine's rules report that


def connection_problems(connection):
    """
    What the call step connector of `connection` (internal shape, streamwright.core.jobs.grammar) would reject or
    ignore, as [(code, message, is_warning)]: its keys as the connector declares them; for a REST connector (whose HTTP
    engine reads only its declared keys and its `http`/`policy` defaults) any other key; for an SDK connector, any
    `http`/`policy` default.
    """
    definition = _definition(connection.get("provider"))
    if definition is None:
        return []
    declared = {k: v for k, v in connection.items() if k != "provider" and k not in HTTP_DEFAULT_KEYS}
    defaults = sorted(public_connection_key(key) for key in connection if key in HTTP_DEFAULT_KEYS)
    is_http = select_transport(definition) == "http"
    # a REST connection without `type` calls the API unauthenticated, as the HTTP engine does
    issues = [] if is_http and "type" not in declared else check_connection(definition, declared)
    found = [("bad-connection", "%s: %s" % (issue.path, issue.message) if issue.path else issue.message,
              issue.severity != "error") for issue in issues if not (is_http and issue.code == "unknown-key")]
    if not is_http and defaults:
        found.append(("unsupported-key", "%s is an SDK connector: %s are the defaults of REST API connectors' "
                                         "requests" % (definition.name, ", ".join(defaults)), False))
    if is_http:
        known = [name for item in definition.connection for name in (item.name, *item.aliases)]
        for key in declared:
            if key not in known:
                found.append(("unsupported-key", "%s is a REST API connector: `%s` would be ignored%s (keys: %s, "
                                                 "http: {%s}, policy: {%s})" % (
                                                     definition.name, key, suggest(key, known), ", ".join(known),
                                                     ", ".join(CONNECTION_HTTP_KEYS),
                                                     ", ".join(CONNECTION_POLICY_KEYS)), False))
    return found


def call_problems(step, connection):
    """
    What the source engine would reject in a call step before calling, as [(code, message, is_warning)]: a call
    style its connector does not take, the connector's checks of an SDK call, and a relative request path with no
    `base_url` on the connection.
    """
    definition = _definition(connection.get("provider"))
    if definition is None:
        return []
    is_http = select_transport(definition) == "http"
    if "request" in step:
        if not is_http:
            return [("bad-call", "%s is an SDK connector: its operation is a call (service, method, arguments), not "
                                 "an HTTP request (path, ...)" % definition.name, False)]
        path = (step["request"] or {}).get("path") if isinstance(step["request"], dict) else None
        if isinstance(path, str) and not path.startswith(_ABSOLUTE) and not connection.get("base_url"):
            return [("bad-call", "operation.path %r is relative and the connection has no `http.base_url`: give an "
                                 "absolute URL (https://...) or set the connection's http.base_url" % path, False)]
        return []
    if "service" not in step and "method" not in step:
        return []
    if is_http:
        return [("bad-call", "%s is a REST API connector: its operation is an HTTP request (path, method, params, "
                             "...), not a call (service, arguments)" % definition.name, False)]
    call = {key: step.get(key) for key in ("service", "method", "arguments")}
    return [("bad-call", problem, False) for problem in components.check_call(definition, call)]
