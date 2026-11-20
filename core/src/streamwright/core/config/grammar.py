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
The grammar of a source manifest's requests (plan §10): the public groups a source writes and the source engine's
internal shape, translated in place (the validator's line marks follow the mappings, which are kept).

    source.yaml                                   internal
      http:   {base_url, headers, params_encoding, records, paginator}
      policy: {retry, rate_limit}                 http: {..., retry, rate_limit}
    stream
      policy: {retry, rate_limit, timeout, on_error}
                                                  retry, rate_limit, timeout, on_error (flat on the stream)
    request item (and async_job submit / results)
      operation:                                  what the request invokes; auth.provider's connector decides the kind
        a call:    {service, method, arguments, records}
                                                  sdk: <auth.provider>, service, method, arguments, records
        a request: {path, method, params, headers, json, params_encoding, records, paginator}
                                                  http: {path, ...}, records, paginator (flat on the item)
    async_job
      results: {operation: {..., records}}        results: {sdk, service, ...}, records (on the item)
      poll:    {operation: {[service], method, arguments}}
                                                  poll: {service, method, arguments} (service: the submit's)
               {operation: {path, method, ...}}   poll: {http: {path, ...}}
      download (json): records on the item, as before (a download is not an operation)

The internal keys are not part of the public grammar: `problems` reports them as unknown keys; `public_path` maps
an internal path back to where the key is written.
"""

import difflib

__all__ = ["OPERATION_KEYS", "POLL_OPERATION_KEYS", "SOURCE_POLICY_KEYS", "STREAM_POLICY_KEYS", "operation_kind",
           "problems", "public_path", "to_internal"]

OPERATION_KEYS = ("service", "method", "arguments", "records", "path", "params", "headers", "json",
                  "params_encoding", "paginator")
POLL_OPERATION_KEYS = ("service", "method", "arguments", "path", "params", "headers", "json", "params_encoding")
_POLL_CALL_KEYS = ("service", "method", "arguments")
SOURCE_POLICY_KEYS = ("retry", "rate_limit")
STREAM_POLICY_KEYS = ("retry", "rate_limit", "timeout", "on_error")
_CALL_KEYS = ("service", "method", "arguments", "records")
_CALL_ONLY = frozenset(("service", "arguments"))
_REQUEST_ONLY = frozenset(("path", "params", "headers", "json", "params_encoding", "paginator"))
_ITEM_REQUEST_KEYS = ("records", "paginator")    # internal: flat on a request item, public: in its operation
_INTERNAL_ITEM_KEYS = ("sdk", "service", "method", "arguments", "http", *_ITEM_REQUEST_KEYS)  # internal shape only
_GROUPS = ("operation", "policy")
_PARTS = ("submit", "results")
_KIND_HINT = "a call (service, method, arguments) or an HTTP request (path, method, params, ...)"


def _mapping(value):
    return value if isinstance(value, dict) else None


def operation_kind(operation):
    """"request" (an HTTP request: it has path, params, ...) or "call" (a connector call) of an operation mapping."""
    return "request" if isinstance(operation, dict) and set(operation) & _REQUEST_ONLY else "call"


def _streams(source):
    streams = source.get("streams")
    return [(index, stream) for index, stream in enumerate(streams if isinstance(streams, list) else ())
            if isinstance(stream, dict)]


def _requests(stream):
    items = stream.get("requests")
    return [(index, item) for index, item in enumerate(items if isinstance(items, list) else ())
            if isinstance(item, dict)]


def _suggest(word, candidates):
    match = difflib.get_close_matches(word, candidates, n=1, cutoff=0.6) if isinstance(word, str) else []
    return " (did you mean %r?)" % match[0] if match else ""


def _unknown(path, container, key, what, place):
    return "unknown-key", (*path, key), container, key, "unknown key %r in %s; %s" % (key, what, place)


def _group(owner, name, keys, path, what):
    """Problems of the group `name` of `owner`: not a mapping, or a key it does not hold."""
    if name not in owner:
        return []
    group = owner[name]
    if not isinstance(group, dict):
        return [("bad-value", (*path, name), owner, name, "`%s` of %s is a mapping of %s" % (
            name, what, ", ".join(keys)))]
    found = []
    for key in group:
        if key == "type" and name == "operation":
            found.append(("unknown-key", (*path, name, key), group, key, "`type` is not written: the connector of "
                                                                         "auth.provider decides whether the operation "
                                                                         "is %s" % _KIND_HINT))
        elif key not in keys:
            found.append(("unknown-key", (*path, name, key), group, key, "unknown key %r in `%s`%s; it holds %s" % (
                key, name, _suggest(key, keys), ", ".join(keys))))
    return found


def _operation_problems(item, path, what, keys=OPERATION_KEYS):
    found = _group(item, "operation", keys, path, what)
    operation = _mapping(item.get("operation"))
    if operation is not None:
        call, request = sorted(set(operation) & _CALL_ONLY), sorted(set(operation) & _REQUEST_ONLY)
        if call and request:      # the one problem of a mixed operation: nothing under it is checked
            return [("bad-value", (*path, "operation"), item, "operation", "an operation is %s, not both (found %s "
                                                                           "and %s)" % (_KIND_HINT, ", ".join(call),
                                                                                        ", ".join(request)))]
    return found


def _item_problems(item, path, what):
    found = _operation_problems(item, path, what)
    job = item.get("async_job")
    downloads = isinstance(job, dict) and "download" in job
    for key in item:
        # an async job's download reads `records` from the item; a paginator on one is the checks' to explain
        if (key == "records" and downloads) or (key == "paginator" and job is not None):
            continue
        if key in _INTERNAL_ITEM_KEYS:
            found.append(_unknown(path, item, key, what, "what it invokes is written in `operation:`"))
    return found


def _poll_problems(poll, path):
    found = _operation_problems(poll, path, "`poll`", POLL_OPERATION_KEYS)
    found += [_unknown(path, poll, key, "`poll`", "it is written in `poll.operation:`") for key in poll
              if key in (*_POLL_CALL_KEYS, "http")]
    return found


def problems(source):
    """
    The grammar problems of a public source, as (code, path, container, key, message): keys of the internal shape
    (unknown-key), groups that are not mappings or hold unknown keys, an operation that mixes a call's keys and a
    request's. Checked before `to_internal`.
    """
    if not isinstance(source, dict):
        return []
    found = _group(source, "policy", SOURCE_POLICY_KEYS, (), "a source")
    http = _mapping(source.get("http"))
    found += [_unknown(("http",), http, key, "`http`", "it is written in `policy:`") for key in http or ()
              if key in SOURCE_POLICY_KEYS]
    for index, stream in _streams(source):
        spath = ("streams", index)
        found += _group(stream, "policy", STREAM_POLICY_KEYS, spath, "a stream")
        found += [_unknown(spath, stream, key, "a stream", "it is written in `policy:`") for key in stream
                  if key in STREAM_POLICY_KEYS]
        for position, item in _requests(stream):
            path = (*spath, "requests", position)
            found += _item_problems(item, path, "a request")
            job = _mapping(item.get("async_job")) or {}
            for part in _PARTS:
                if _mapping(job.get(part)) is not None:
                    found += _item_problems(job[part], (*path, "async_job", part), "`%s`" % part)
            if _mapping(job.get("poll")) is not None:
                found += _poll_problems(job["poll"], (*path, "async_job", "poll"))
    return found


def _lift(owner, name, keys):
    """Moves the known keys of the group `name` onto `owner`; the group is removed."""
    group = owner.pop(name, None)
    if isinstance(group, dict):
        owner.update((key, value) for key, value in group.items() if key in keys)
    return group


def _request_to_internal(item, provider):
    operation = _mapping(item.get("operation"))
    if operation is None:
        return
    if operation_kind(operation) == "call":
        _lift(item, "operation", _CALL_KEYS)
        item["sdk"] = provider
        return
    item["http"] = item.pop("operation")         # the same mapping: its YAML node keeps the request's line marks
    for key in _ITEM_REQUEST_KEYS:
        if key in operation:
            item[key] = operation.pop(key)


def to_internal(source):
    """Translates a public source manifest (source.yaml with its streams) into the engine's shape, in place."""
    if not isinstance(source, dict):
        return source
    provider = (_mapping(source.get("auth")) or {}).get("provider")
    policy = {key: value for key, value in (_mapping(source.pop("policy", None)) or {}).items()
              if key in SOURCE_POLICY_KEYS}
    if policy:
        if "http" not in source:
            source["http"] = {}
        if isinstance(source["http"], dict):
            source["http"].update(policy)
    for _, stream in _streams(source):
        _lift(stream, "policy", STREAM_POLICY_KEYS)
        for _, item in _requests(stream):
            _request_to_internal(item, provider)
            job = _mapping(item.get("async_job")) or {}
            for part in _PARTS:
                if _mapping(job.get(part)) is not None:
                    _request_to_internal(job[part], provider)
            if _mapping(job.get("results")) is not None and "records" in job["results"]:
                item["records"] = job["results"].pop("records")      # the item's records select the results'
            poll = _mapping(job.get("poll"))
            if poll is not None and operation_kind(poll.get("operation")) == "request":
                poll["http"] = poll.pop("operation")           # the same mapping: it keeps the request's line marks
            elif poll is not None:
                _lift(poll, "operation", _POLL_CALL_KEYS)
    return source


def _request_at(public, path):
    streams = public.get("streams") if isinstance(public, dict) else None
    if not (isinstance(streams, list) and isinstance(path[1], int) and 0 <= path[1] < len(streams)):
        return None
    items = streams[path[1]].get("requests") if isinstance(streams[path[1]], dict) else None
    if not (isinstance(items, list) and isinstance(path[3], int) and 0 <= path[3] < len(items)):
        return None
    return _mapping(items[path[3]])


def public_path(path, public):
    """The path of `path` (found on the internal shape) as the public source `public` writes it."""
    path = tuple(path)
    if len(path) > 1 and path[0] == "http" and path[1] in SOURCE_POLICY_KEYS and isinstance(public, dict):
        http, policy = _mapping(public.get("http")) or {}, _mapping(public.get("policy")) or {}
        if path[1] not in http and path[1] in policy:
            return ("policy", *path[1:])
    if len(path) > 4 and path[0] == "streams" and path[2] == "requests" and path[4] == "records":
        item = _request_at(public, path)
        results = _mapping(_mapping((item or {}).get("async_job") or {}).get("results")) if item else None
        if results is not None and "records" in (_mapping(results.get("operation")) or {}) and "records" not in item:
            return (*path[:4], "async_job", "results", "operation", *path[4:])
    found, node = [], public
    for position, key in enumerate(path):
        container = _mapping(node)
        if container is not None and key not in container and _mapping(container.get("operation")) is not None \
                and (key == "sdk" or (key == "http" and position > 0)):
            found.append("operation")             # sdk: the connector of auth.provider; http: the request itself
            node = None if key == "sdk" else container["operation"]
            continue
        if container is not None and key not in container:
            group = next((name for name in _GROUPS if _mapping(container.get(name)) is not None
                          and key in container[name]), None)
            if group is not None:
                found.append(group)
                container = container[group]
        found.append(key)
        if container is not None:
            node = container.get(key)
        elif isinstance(node, list) and isinstance(key, int) and 0 <= key < len(node):
            node = node[key]
        else:
            node = None
    return tuple(found)
