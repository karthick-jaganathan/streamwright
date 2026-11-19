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
"""The OpenAPI document of a caller's jobs: each job's request schema from its settable spec.config and spec.input."""

from types import MappingProxyType

__all__ = ["input_schema", "openapi"]

_TYPES = MappingProxyType({"string": {"type": "string"}, "integer": {"type": "integer"},
                           "number": {"type": "number"}, "boolean": {"type": "boolean"},
                           "date": {"type": "string", "format": "date"}})
_CONSTRAINTS = MappingProxyType({"enum": "enum", "pattern": "pattern", "minimum": "minimum", "maximum": "maximum",
                                 "max_length": "maxLength"})
_ERROR = MappingProxyType({"type": "object", "required": ["status", "code", "message", "request_id"],
                           "properties": {"status": {"const": "error"}, "code": {"type": "string"},
                                          "message": {"type": "string"}, "request_id": {"type": "string"}}})
_RUN = MappingProxyType({"type": "object", "properties": {
    "run_id": {"type": "string"}, "job": {"type": "string"}, "request_id": {"type": "string"},
    "status": {"enum": ["queued", "running", "succeeded", "failed"]}, "attempts": {"type": "integer"},
    "result": {"type": ["object", "null"]}, "error": {"type": "object"}}})


def input_schema(definition):
    """The JSON schema of one input (a spec.config input or a spec.input column)."""
    definition = definition or {}
    kind = definition.get("type", "string")
    item = dict(_TYPES.get(definition.get("items", "string") if kind == "list" else kind, _TYPES["string"]))
    item.update({target: definition[source] for source, target in _CONSTRAINTS.items() if source in definition})
    schema = {"type": "array", "items": item} if kind == "list" else item
    if definition.get("description"):
        schema["description"] = definition["description"]
    return schema


def _request(served):
    properties = {"set": {"type": "object", "additionalProperties": False, "properties": {
        name: input_schema(served.config[name]) for name in sorted(served.settable)},
        "required": sorted(name for name in served.settable if (served.config[name] or {}).get("required", True)
                           and (served.config[name] or {}).get("default") is None)},
        "options": {"type": "object", "additionalProperties": False, "properties": {"async": {"type": "boolean"}}}}
    required = []
    if served.input is not None:
        columns = served.input["columns"]
        properties["input"] = {"type": "array", "maxItems": int(served.input["max_rows"]), "items": {
            "type": "object", "additionalProperties": served.input.get("unknown_columns") == "drop",
            "properties": {name: input_schema(column) for name, column in sorted(columns.items())},
            "required": sorted(name for name, column in columns.items() if (column or {}).get("required"))}}
        required.append("input")
    return {"type": "object", "additionalProperties": False, "properties": properties, "required": required}


def _responses(*codes):
    found = {"200": {"description": "the run ended", "content": {"application/json": {"schema": dict(_RUN)}}},
             "202": {"description": "the run is queued or running: poll its Location",
                     "content": {"application/json": {"schema": dict(_RUN)}}}}
    shown = {code: found[code] for code in codes if code in found}
    shown["default"] = {"description": "an error", "content": {"application/json": {"schema": dict(_ERROR)}}}
    return shown


def _paths(served):
    name = served.entry.name
    body = {"required": True, "content": {"application/json": {"schema": _request(served)}}}
    run = {"summary": "run %s (duckdb run)" % name, "operationId": "run_%s" % name, "requestBody": body,
           "responses": _responses("200", "202")}
    run["parameters"] = [{"name": "Idempotency-Key", "in": "header", "required": served.applies,
                          "schema": {"type": "string", "maxLength": 128}}]
    return {
        "/v1/jobs/%s/runs" % name: {"post": run},
        "/v1/jobs/%s/validate" % name: {"post": {"summary": "validate %s (duckdb validate)" % name,
                                                 "operationId": "validate_%s" % name, "requestBody": body,
                                                 "responses": _responses()}},
        "/v1/jobs/%s/plan" % name: {"get": {"summary": "plan of %s" % name, "operationId": "plan_%s" % name,
                                            "responses": _responses()}},
        "/v1/jobs/%s/status" % name: {"get": {"summary": "latest runs of %s" % name,
                                              "operationId": "status_%s" % name, "responses": _responses()}},
    }


def openapi(jobs, authenticated):
    """OpenAPI 3.1 of `jobs` (ServedJob)."""
    paths = {"/v1/runs/{run_id}": {"get": {"summary": "a run", "operationId": "get_run", "parameters": [
        {"name": "run_id", "in": "path", "required": True, "schema": {"type": "string"}}],
        "responses": _responses("200")}}}
    for served in jobs:
        paths.update(_paths(served))
    document = {"openapi": "3.1.0", "info": {"title": "StreamWright jobs", "version": "1"}, "paths": paths}
    if authenticated:
        document["components"] = {"securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}}}
        document["security"] = [{"bearer": []}]
    return document
