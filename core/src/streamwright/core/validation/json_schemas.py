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

"""The JSON Schemas (draft-07) of sources and stream files, for editor autocomplete."""

from collections.abc import Mapping
from types import MappingProxyType

from streamwright.core.validation.vocabulary import (
    API_KEY_LOCATIONS,
    AUTH_TYPES,
    BACKOFF_TYPES,
    BATCH_SIZE,
    COMPRESSIONS,
    DOWNLOAD_FORMATS,
    DURATION_PATTERN,
    ERROR_POLICIES,
    HTTP_METHODS,
    INPUT_TYPES,
    KIND,
    LIST_ITEM_TYPES,
    NAME_PATTERN,
    PAGINATOR_IN_LOCATIONS,
    PAGINATOR_TYPES,
    PARAMS_ENCODING_TYPES,
    PARTITION_RECORD_KEYS,
    PARTITION_STREAM_KEYS,
    PARTITION_VALUES_KEYS,
    REQUEST_COLUMNS,
    REQUEST_PARTITION_RECORD_KEYS,
    REQUEST_PARTITION_SOURCE_KEYS,
    REQUEST_PARTITION_VALUES_KEYS,
    REQUIRED_STREAM_KEYS,
    REQUIRED_TOP_LEVEL_KEYS,
    TRANSFORM_KEYS,
    TRANSFORM_MODES,
)

_MERGE_KEY = "<<"  # YAML merge key, as an editor in YAML 1.2 mode sees `<<: *anchor`
_STR = MappingProxyType({"type": "string"})
_DURATION = MappingProxyType({"type": "string", "pattern": DURATION_PATTERN, "description": "e.g. 15s, 30m, 1h, 7d"})
_DAYS = MappingProxyType({"type": "string", "pattern": r"^[0-9]+d$", "description": "whole days, e.g. 0d, 3d"})
_WINDOW = MappingProxyType({"type": "string", "pattern": r"^0*[1-9][0-9]*d$",
                            "description": "whole days, at least 1d, e.g. 1d, 7d"})
_NAME = MappingProxyType({"type": "string", "pattern": NAME_PATTERN})
_FROM = MappingProxyType(dict(_NAME, description="An earlier request of this stream (fields are dotted paths in its "
                                                 "records) or a step of this stream (fields are its columns)."))
_BATCH = MappingProxyType({"type": "integer", "minimum": 1, "description": "One request per list of up to N distinct "
                                                                          "values: the partition's name holds the list "
                                                                          "(e.g. for GAQL `IN`)."})
_START_DATE = MappingProxyType({"type": "string",
                                "pattern": r"^(?:today|-?[0-9]+d|[0-9]{4}-[0-9]{2}-[0-9]{2}|\{\{.*\}\})$"})
_INPUT_DEFAULT_TYPES = MappingProxyType({
    "string": {"type": ["string", "null"]}, "integer": {"type": ["integer", "null"]},
    "number": {"type": ["number", "null"]}, "boolean": {"type": ["boolean", "null"]},
    "date": {"anyOf": [_START_DATE, {"type": "null"}]}, "list": {"type": ["array", "null"]},
})


def _ref(name):
    return {"$ref": "#/definitions/%s" % name}


def _obj(properties, required=(), description=None):
    properties = dict(properties, **{_MERGE_KEY: {}})
    schema = {"type": "object", "properties": properties, "additionalProperties": False}
    if required:
        schema["required"] = list(required)
    if description:
        schema["description"] = description
    return schema


def _enum(values, description=None):
    schema = {"enum": list(values)}
    if description:
        schema["description"] = description
    return schema


def _dispatch(options, key="type"):
    """{key: name} selects options[name]."""
    return {
        "type": "object",
        "required": [key],
        "properties": {key: _enum(options)},
        "allOf": [{"if": {"properties": {key: {"const": name}}, "required": [key]}, "then": schema}
                  for name, schema in options.items()],
    }


def _definitions():
    definitions = {
        "input": dict(_obj({
            "type": _enum(INPUT_TYPES),
            "items": _enum(LIST_ITEM_TYPES, "Item type of a `list` input."),
            "required": {"type": "boolean", "description": "Default true."},
            "default": {"description": "Dates accept offsets from today, e.g. -30d."},
            "description": _STR,
        }, required=("type",)), allOf=[
            {"if": {"properties": {"type": {"const": name}}, "required": ["type"]},
             "then": {"properties": {"default": schema},
                      **({"required": ["items"]} if name == "list" else {})}}
            for name, schema in _INPUT_DEFAULT_TYPES.items()
        ]),
        "inputs": {"type": ["object", "null"], "propertyNames": _NAME, "additionalProperties": _ref("input")},
        "rate_limit": _obj({"requests": {"type": "integer", "minimum": 1}, "per": _DURATION},
                           required=("requests", "per")),
        "retry": _obj({
            "codes": {"type": "array", "items": {"type": ["integer", "string"]},
                   "description": "HTTP status codes or provider error codes."},
            "max_attempts": {"type": "integer", "minimum": 1},
            "backoff": _enum(BACKOFF_TYPES),
            "max_delay": _DURATION,
        }),
        "http_request": dict(_obj({
            "path": _STR, "method": _enum(HTTP_METHODS), "params": {"type": "object"},
            "headers": {"type": "object", "additionalProperties": _STR}, "json": {},
            "params_encoding": _enum(PARAMS_ENCODING_TYPES),
        }, required=("path",)), allOf=[{
            "if": {"properties": {"params_encoding": {"const": "plain"}}, "required": ["params_encoding"]},
            "then": {"properties": {"params": {"additionalProperties": {
                "not": {"type": ["object", "array"]}}}}},
        }]),
        "sdk_request": _obj({"sdk": _STR, "service": _STR, "method": _STR, "arguments": {"type": "object"}},
                            required=("sdk", "method")),
        "poll_condition": _obj({"path": _STR, "equals": {}, "in": {"type": "array"}}, required=("path",)),
        "async_job": {
            "type": "object",
            "properties": {
                "submit": _ref("sdk_request"),
                "poll": _obj({
                    "method": _STR, "arguments": {"type": "object"}, "every": _DURATION, "timeout": _DURATION,
                    "done_when": _ref("poll_condition"), "fail_when": _ref("poll_condition"),
                }, required=("method", "every", "timeout", "done_when")),
                "download": _obj({"url": _STR, "format": _enum(DOWNLOAD_FORMATS), "compression": _enum(COMPRESSIONS)},
                                 required=("url", "format")),
                "results": _ref("sdk_request"),
            },
            "required": ["submit", "poll"],
            "oneOf": [{"required": ["download"]}, {"required": ["results"]}],
            "additionalProperties": False,
        },
        "request_item": {"type": "object", "properties": {
            "name": _NAME,
            "http": _ref("http_request"),
            "sdk": _STR,
            "service": _STR,
            "method": _STR,
            "arguments": {"type": "object"},
            "async_job": _ref("async_job"),
            "paginator": _ref("paginator"),
            "records": _ref("records"),
            "partitions": {"type": "array", "minItems": 1, "items": _ref("request_partition")},
            _MERGE_KEY: {},
        }, "required": ["name"], "additionalProperties": False, "oneOf": [
            {"required": ["http"], "not": {"anyOf": [
                {"required": ["sdk"]}, {"required": ["service"]}, {"required": ["method"]},
                {"required": ["arguments"]}, {"required": ["async_job"]},
            ]}},
            {"required": ["sdk", "method"], "not": {"anyOf": [{"required": ["http"]}, {"required": ["async_job"]}]}},
            {"required": ["async_job"], "not": {"anyOf": [
                {"required": ["http"]}, {"required": ["sdk"]}, {"required": ["service"]},
                {"required": ["method"]}, {"required": ["arguments"]},
            ]}},
        ]},
        "paginator": _dispatch({
            "none": _obj({"type": {}}),
            "offset": _obj({"type": {}, "offset_param": _STR, "limit_param": _STR,
                            "page_size": {"type": "integer", "minimum": 1},
                            "start": {"type": "integer", "minimum": 0},
                            "total_path": _STR,
                            "in": _enum(PAGINATOR_IN_LOCATIONS)},
                           required=PAGINATOR_TYPES["offset"]["required"]),
            "cursor": {"anyOf": [
                _obj({"type": {}, "token_path": _STR, "param": _STR,
                      "has_more_path": _STR, "in": _enum(PAGINATOR_IN_LOCATIONS)},
                     required=("token_path", "param")),
                _obj({"type": {}, "next_url_path": _STR,
                      "has_more_path": _STR, "in": _enum(PAGINATOR_IN_LOCATIONS)},
                     required=("next_url_path",)),
            ]},
            "page_number": _obj({"type": {}, "page_param": _STR, "page_size": {"type": "integer", "minimum": 1},
                                 "size_param": _STR, "start": {"type": "integer", "minimum": 0},
                                 "total_pages_path": _STR,
                                 "in": _enum(PAGINATOR_IN_LOCATIONS)},
                                required=("page_param",)),
            "link_header": _obj({"type": {}}),
        }),
        "partition": {"oneOf": [
            _obj({"name": _NAME, "values": {"type": ["array", "string"]}}, required=PARTITION_VALUES_KEYS),
            _obj({"name": _NAME, "from_stream": _STR, "field": _STR}, required=PARTITION_STREAM_KEYS),
            _obj({"from_stream": _STR, "fields": {"type": "array", "items": _NAME, "minItems": 1, "uniqueItems": True}},
                 required=PARTITION_RECORD_KEYS, description="One partition per distinct combination of these "
                                                             "fields in the parent's records, named after them."),
        ]},
        "request_partition": {"oneOf": [
            _obj({"name": _NAME, "values": {"type": ["array", "string"]}, BATCH_SIZE: _BATCH},
                 required=REQUEST_PARTITION_VALUES_KEYS),
            _obj({"name": _NAME, "from": _FROM, "field": _STR, BATCH_SIZE: _BATCH},
                 required=REQUEST_PARTITION_SOURCE_KEYS),
            _obj({"from": _FROM, "fields": {"type": "array", "items": _NAME, "minItems": 1, "uniqueItems": True},
                  BATCH_SIZE: dict(_BATCH, description="One request per list of up to N distinct values of the one "
                                                       "field that is not a stream partition, which names the list "
                                                       "(the stream partition fields keep the partition's rows).")},
                 required=REQUEST_PARTITION_RECORD_KEYS, description="One request partition per distinct "
                                                                     "combination of these fields."),
        ]},
        "incremental": _obj({"cursor_field": _STR, "start": _START_DATE, "window": _WINDOW, "lookback": _DAYS},
                            required=("cursor_field", "start")),
        "records": _obj({"path": _STR, "explode": _STR}),
        "transform": _obj({
            "mode": _enum(TRANSFORM_MODES, "page: the steps run on each page of the stream's one request; run: the "
                                           "requests and steps run in dependency order over all of the run's rows, "
                                           "then the exports are written."),
            "steps": {"type": "array", "minItems": 1, "items": _ref("step")},
        }, required=TRANSFORM_KEYS, description="Named SQL steps that shape the stream's requests."),
        "step": _obj({
            "name": _NAME,
            "select": {"type": "string", "minLength": 1,
                       "description": "One DuckDB SELECT over the stream's request tables (columns record, "
                                      "partition, config: JSON; window_start, window_end, today: DATE) and earlier "
                                      "steps; `$name` reads spec.config.<name>."},
            "description": _STR,
        }, required=("name", "select")),
        "export": _obj({
            "step": _NAME,
            "primary_key": {"type": "array", "items": _STR, "minItems": 1},
            "description": _STR,
            "file_name": {"type": "string", "minLength": 1,
                          "description": "The export's file inside a file output's directory (folders allowed), a "
                                         "template with {{ export }}, {{ source }}, {{ today }}, {{ timestamp }} and "
                                         "{{ config.NAME }}; --file-name replaces it."},
        }, required=("step",)),
        "stream": _obj({
            "kind": {"type": "string", "enum": ["stream"]},
            "name": _NAME,
            "description": _STR,
            "partitions": {"type": "array", "minItems": 1, "items": _ref("partition")},
            "incremental": _ref("incremental"),
            "requests": {"type": "array", "minItems": 1, "items": _ref("request_item"),
                         "description": "Named requests: the raw rows of each one are a table of the stream's SQL, "
                                        "named after it (columns %s)." % ", ".join(REQUEST_COLUMNS)},
            "transform": _ref("transform"),
            "export": {"type": "object", "minProperties": 1, "propertyNames": _NAME,
                       "additionalProperties": _ref("export"),
                       "description": "The stream's outputs: each export is an output table with the rows of one "
                                      "step."},
            "on_partition_error": _enum(ERROR_POLICIES),
            "retry": _ref("retry"),
            "rate_limit": _ref("rate_limit"),
        }, required=REQUIRED_STREAM_KEYS),
    }
    auth_fields = {"token_url": _STR, "client_id": _STR, "client_secret": _STR, "refresh_token": _STR,
                   "scopes": {"type": "array", "items": _STR}, "name": _STR, "value": _STR,
                   "in": _enum(API_KEY_LOCATIONS), "token": _STR, "username": _STR, "password": _STR}
    builtin_auth = {name: _obj(dict([("type", {})] + [(k, auth_fields[k]) for k in
                                                          keys["required"] + keys["optional"]]),
                                    required=keys["required"])
                        for name, keys in AUTH_TYPES.items()}
    definitions["auth"] = {
        "type": "object",
        "if": {"required": ["provider"]},
        "then": {"properties": {"provider": _STR},
                 "description": "Connector provider; its keys are defined by the connector."},
        "else": _dispatch(builtin_auth),
    }
    return definitions


def _plain(schema):
    """A schema with its shared fragments (read-only mappings) copied into plain dicts, ready for JSON."""
    if isinstance(schema, Mapping):
        return {key: _plain(value) for key, value in schema.items()}
    if isinstance(schema, list):
        return [_plain(value) for value in schema]
    return schema


def json_schema():
    """JSON Schema (draft-07) for `kind: source` documents, for editor autocomplete."""
    return _plain({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "StreamWright source configuration",
        "description": "A source with streams. Generated by `streamwright validate --export-schema`; run "
                       "`streamwright validate` for the full set of checks (references, connectors and each step's "
                       "`select`, compiled by DuckDB).",
        "type": "object",
        "properties": {
            "version": {"enum": [1, "1", "1.0"], "description": "Optional; omitting it means the current format."},
            "kind": {"const": KIND},
            "name": _NAME,
            "description": _STR,
            "spec": _obj({"config": _ref("inputs"), "secrets": _ref("inputs")}),
            "auth": _ref("auth"),
            "http": _obj({
                "base_url": _STR,
                "headers": {"type": "object", "additionalProperties": _STR},
                "rate_limit": _ref("rate_limit"),
                "retry": _ref("retry"),
                "paginator": _ref("paginator"),
                "records": _ref("records"),
                "params_encoding": _enum(PARAMS_ENCODING_TYPES),
            }),
            "export": _obj({
                "file_name": {"type": "string", "minLength": 1,
                              "description": "The default file-name template of every export of the source (an "
                                             "export's own `file_name` replaces it)."},
            }),
            "streams": {"type": "array", "minItems": 1, "items": _ref("stream")},
        },
        # not `streams`: the source.yaml of a source folder has none (`streamwright validate` checks the folder)
        "required": [key for key in REQUIRED_TOP_LEVEL_KEYS if key != "streams"],
        "patternProperties": {"^x-": {}},
        "additionalProperties": False,
        "definitions": _definitions(),
    })


def stream_json_schema():
    """JSON Schema (draft-07) for the stream files of source folders (streams/<name>.yaml), for editor autocomplete."""
    definitions = _definitions()
    stream = definitions["stream"]
    return _plain({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "StreamWright source stream",
        "description": "One stream of a source folder: streams/<name>.yaml, where the file name is the stream name. "
                       "Generated by `streamwright validate --export-schema`; run `streamwright validate` on the "
                       "folder for the full set of checks.",
        "type": "object",
        "properties": {key: value for key, value in stream["properties"].items() if key != "name"},
        "required": [key for key in stream["required"] if key != "name"],
        "additionalProperties": False,
        "definitions": definitions,
    })
