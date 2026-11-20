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
The grammar of a job manifest's items (plan §10): the public groups a manifest writes and the engines' internal
shape. One translation, after the public shape is checked; the engines, the runner and the contract types read the
internal shape only.

    steps / outputs                         internal (flat on the item)
      operation:                            what the item invokes on its connection's connector, which decides the
                                            kind (no `type`: an SDK connector takes a call, a REST one a request)
        a call:   {service, method, arguments}
                                            service, method, arguments
        a request: {path, method, params, headers, json, records, paginator}
                                            request: {path, method, params, headers, json, records}, paginator
      policy: {retry, rate_limit, timeout, on_error, page_size}
                                            retry, rate_limit, timeout, on_error, page_size
      apply:  {chunk_size, on_row_error, nulls, on_unknown, allow_remove}
                                            chunk_size, on_row_error, nulls, on_unknown, allow_remove
    connections (REST connectors)
      http:   {base_url, headers, params_encoding, records, paginator}
      policy: {retry, rate_limit}           base_url, headers, params_encoding, records, paginator, retry, rate_limit

The internal (flat) keys are not part of the public grammar: they are unknown keys (`grammar_problems` reports a
connection's, whose other keys are its connector's).
`public_path` maps an internal path (an issue found on the internal shape) back to where the key is written.
"""

import copy
from types import MappingProxyType

from streamwright.core.jobs.fields import Field

__all__ = ["APPLY_FIELD", "APPLY_KEYS", "ASYNC_JOB_FIELD", "CALL_KEYS", "CONNECTION_GROUPS", "CONNECTION_HTTP_KEYS",
           "CONNECTION_POLICY_KEYS", "NULLS", "ON_ROW_ERROR", "ON_UNKNOWN", "OPERATION_FIELD", "POLICY_FIELD",
           "POLICY_KEYS", "REQUEST_KEYS", "grammar_problems", "operation_kind", "public_connection_key", "public_key",
           "public_path", "to_internal"]

CALL_KEYS = ("service", "method", "arguments")
REQUEST_KEYS = ("path", "method", "params", "headers", "json", "records", "paginator")
POLICY_KEYS = ("retry", "rate_limit", "timeout", "on_error", "page_size")
APPLY_KEYS = ("chunk_size", "on_row_error", "nulls", "on_unknown", "allow_remove")
CONNECTION_HTTP_KEYS = ("base_url", "headers", "params_encoding", "records", "paginator")
CONNECTION_POLICY_KEYS = ("retry", "rate_limit")
ON_ROW_ERROR = ("fail", "continue")
NULLS = ("error", "skip_field", "clear")
ON_UNKNOWN = ("hold", "reconcile")
_CALL_ONLY = frozenset(CALL_KEYS) - frozenset(REQUEST_KEYS)          # service, arguments
_REQUEST_ONLY = frozenset(REQUEST_KEYS) - frozenset(CALL_KEYS)       # path, params, headers, json, records, paginator
_REQUEST = "request"                               # the internal name of an item's HTTP request
_TEXT = Field("string")
_OBJECT = Field("object")
OPERATION_FIELD = Field("object", fields={
    "service": Field("string", description="a call: the connector's service"),
    "method": Field("string", description="a call: the service's method; a request: the HTTP method (GET)"),
    "arguments": Field("object", description="a call: the operation's arguments (the connector declares them; "
                                             "others go to the vendor verbatim)"),
    "path": Field("string", description="a request: relative to the connection's http.base_url, or absolute"),
    "params": _OBJECT, "headers": _OBJECT, "json": Field(),
    "records": Field("object", description="a request: where the records are in each response (path, explode)"),
    "paginator": Field("object", description="a request: how the pages are followed")},
    description="what the item invokes on its connection's connector: a call (service, method, arguments) of an SDK "
                "connector, or a request (path, method, params, ...) of a REST API connector")
_JOB_PART = Field("object", fields={"operation": OPERATION_FIELD}, description="a call or a request")
_CONDITION = Field("object", fields={"path": Field("string", required=True), "equals": Field(),
                                     "in": Field("array")})
ASYNC_JOB_FIELD = Field("object", fields={
    "submit": Field("object", required=True, fields={"operation": OPERATION_FIELD},
                    description="starts the job; its response is {{ submit.* }}"),
    "poll": Field("object", required=True, fields={
        "operation": Field("object", fields={**OPERATION_FIELD.fields, "service": Field(
            "string", description="a call: the service (default: the submit's)")}),
        "every": Field("string", required=True, description="how often to check, e.g. 15s"),
        "timeout": Field("string", required=True, description="how long to wait in all, e.g. 30m"),
        "done_when": Field("object", required=True, fields=_CONDITION.fields), "fail_when": _CONDITION},
        description="checks the job until done_when; its response is {{ poll.* }}"),
    "results": _JOB_PART,
    "download": Field("object", fields={"url": Field("string", required=True),
                                        "format": Field("string", required=True, enum=("csv", "jsonl", "json")),
                                        "compression": Field("string", enum=("none", "zip", "gzip"))})},
    description="call steps: a report job (submit, poll, then results or a download) instead of an `operation:`; "
                "the source engine runs it (docs/design/source-format.md, async jobs)")
POLICY_FIELD = Field("object", fields={
    "retry": _OBJECT, "rate_limit": _OBJECT, "page_size": Field("integer", minimum=1),
    "on_error": Field("string", enum=("fail", "skip", "warn")),
    "timeout": Field(("number", "string"), description="per-call timeout, seconds or a duration like `30s`")},
    description="how the call behaves: retries, throttling, timeout, failures, page size")
APPLY_FIELD = Field("object", fields={
    "chunk_size": Field("integer", minimum=1, description="rows per call (at most the operation's max_chunk, its "
                                                          "default)"),
    "on_row_error": Field("string", enum=ON_ROW_ERROR, description="a rejected row stops the run (fail, the "
                                                                   "default) or is only recorded"),
    "nulls": Field("string", enum=NULLS, description="a NULL field fails its row (error, the default), is left out "
                                                     "(skip_field) or clears the field"),
    "on_unknown": Field("string", enum=ON_UNKNOWN, description="a cut-short call of a non-idempotent operation is "
                                                               "held for the operator (hold, the default) or looked "
                                                               "up in the vendor (reconcile)"),
    "allow_remove": Field("boolean", description="rows may use the operation's destructive actions")},
    description="apply outputs: how the rows are sent and judged")
# a REST connector connection's defaults for its calls
CONNECTION_GROUPS = MappingProxyType({
    "http": Field("object", fields={"base_url": _TEXT, "headers": _OBJECT,
                                    "params_encoding": Field("string", enum=("plain", "dotted")),
                                    "records": _OBJECT, "paginator": _OBJECT},
                  description="REST connectors: the defaults of every request (base_url, headers, ...)"),
    "policy": Field("object", fields={"retry": _OBJECT, "rate_limit": _OBJECT},
                    description="REST connectors: the default retry and rate limit of every request")})
_ITEM_SECTIONS = ("steps", "outputs")
_KIND_HINT = "a call (service, method, arguments) or an HTTP request (path, method, params, ...)"
# an item's flat key (internal) -> where it is written
_ITEM_PLACES = MappingProxyType({
    **{key: "operation.%s" % key for key in CALL_KEYS}, _REQUEST: "operation", "paginator": "operation.paginator",
    **{key: "policy.%s" % key for key in POLICY_KEYS}, **{key: "apply.%s" % key for key in APPLY_KEYS}})
_CONNECTION_PLACES = MappingProxyType({**{key: "http.%s" % key for key in CONNECTION_HTTP_KEYS},
                                       **{key: "policy.%s" % key for key in CONNECTION_POLICY_KEYS}})


def _items(job):
    for section in _ITEM_SECTIONS:
        items = job.get(section)
        for index, item in enumerate(items if isinstance(items, list) else ()):
            if isinstance(item, dict):
                yield section, index, item


def _connections(job):
    connections = job.get("connections")
    return connections.items() if isinstance(connections, dict) else ()


def operation_kind(operation):
    """"request" (an HTTP request: it has path, params, ...) or "call" (a connector call) of an operation mapping."""
    return "request" if isinstance(operation, dict) and set(operation) & _REQUEST_ONLY else "call"


def _operation_problems(operation, path):
    call, request = sorted(set(operation) & _CALL_ONLY), sorted(set(operation) & _REQUEST_ONLY)
    if call and request:              # the one problem of a mixed operation: nothing under it is checked
        return [("bad-value", path, "an operation is %s, not both (found %s and %s)" % (
            _KIND_HINT, ", ".join(call), ", ".join(request)))]
    found = []
    if "type" in operation:
        found.append(("unknown-key", (*path, "type"), "`type` is not written: the connection's connector decides "
                                                       "whether the operation is %s" % _KIND_HINT))
    kind = operation_kind(operation)
    what, required = ("an HTTP request", ("path",)) if kind == "request" else ("a call", ("service", "method"))
    found.extend(("missing-key", (*path, key), "%s needs `%s`" % (what, key)) for key in required
                 if key not in operation)
    return found


def grammar_problems(job):
    """
    What a public manifest writes outside the grammar, as (code, path, message): a connection's internal keys
    (unknown-key), a `type` in an operation, an operation that mixes a call's keys and a request's, and the keys its
    kind needs (a call: service, method; a request: path). Each one replaces the structure check's issue at its path.
    """
    found = []
    for section, index, item in _items(job):
        if isinstance(item.get("operation"), dict):
            found.extend(_operation_problems(item["operation"], (section, index, "operation")))
    for name, connection in _connections(job):
        for key in connection if isinstance(connection, dict) else ():
            if key in _CONNECTION_PLACES:
                found.append(("unknown-key", ("connections", name, key), "unknown key %r in a connection; it is "
                                                                         "written as `%s`" % (
                                                                             key, _CONNECTION_PLACES[key])))
    return found


def public_connection_key(key):
    """Where a connection's internal key is written (`base_url` -> `http.base_url`), or the key itself."""
    return _CONNECTION_PLACES.get(key, key)


def public_key(key):
    """Where an item's internal key is written (`retry` -> `policy.retry`), or the key itself."""
    return _ITEM_PLACES.get(key, key)


def _group(mapping, name):
    value = mapping.pop(name, None)
    return value if isinstance(value, dict) else {}


def _item_to_internal(item):
    operation = _group(item, "operation")
    if operation_kind(operation) == "request":
        if "paginator" in operation:
            item["paginator"] = operation.pop("paginator")
        item[_REQUEST] = operation
    else:
        item.update(operation)
    item.update(_group(item, "policy"))
    item.update(_group(item, "apply"))


def to_internal(job):
    """A copy of a public job manifest in the engines' internal shape (groups flattened, a request as `request`)."""
    if not isinstance(job, dict):
        return job
    internal = copy.deepcopy(job)
    for _, _, item in _items(internal):
        _item_to_internal(item)
    for _, connection in _connections(internal):
        if isinstance(connection, dict):
            connection.update(_group(connection, "http"))
            connection.update(_group(connection, "policy"))
    return internal


def _item(job, section, index):
    items = job.get(section) if isinstance(job, dict) else None
    if isinstance(items, list) and isinstance(index, int) and 0 <= index < len(items) \
            and isinstance(items[index], dict):
        return items[index]
    return None


def public_path(path, job):
    """The path of `path` (a tuple found on the internal shape) as the public manifest `job` writes it."""
    path = tuple(path)
    if len(path) >= 3 and path[0] in _ITEM_SECTIONS:
        item = _item(job, path[0], path[1])
        key = path[2]
        if item is not None and key not in item:
            if key == _REQUEST:
                return (*path[:2], "operation", *path[3:])
            for group in ("operation", "policy", "apply"):
                if isinstance(item.get(group), dict) and key in item[group]:
                    return (*path[:2], group, *path[2:])
    if len(path) >= 3 and path[0] == "connections":
        connection = (job.get("connections") or {}).get(path[1]) if isinstance(job, dict) else None
        if isinstance(connection, dict) and path[2] not in connection:
            for group in ("http", "policy"):
                if isinstance(connection.get(group), dict) and path[2] in connection[group]:
                    return (*path[:2], group, *path[2:])
    return path
