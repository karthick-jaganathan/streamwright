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
The vocabulary of `kind: source` documents (validation.schema describes the format): keys, types, enums and
reference scopes, shared by the checks, the runtime and the JSON Schema.
"""

from types import MappingProxyType

# `version` is optional; omitting it means VERSION. A new version is only introduced for a breaking change.
VERSION = "1"
SUPPORTED_VERSIONS = ("1", "1.0")
KIND = "source"

TOP_LEVEL_KEYS = ("version", "kind", "name", "description", "spec", "auth", "http", "export", "streams")
REQUIRED_TOP_LEVEL_KEYS = ("kind", "name", "streams")
EXTENSION_PREFIX = "x-"  # free-form top-level keys, e.g. to hold YAML anchors

NAME_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*$"
DURATION_PATTERN = r"^[0-9]+[smhd]$"

# * ------
# * inputs
# * ------
INPUT_SECTIONS = ("config", "secrets")
INPUT_TYPES = ("string", "integer", "number", "boolean", "date", "list")
LIST_ITEM_TYPES = ("string", "integer", "number", "boolean", "date")
INPUT_KEYS = ("type", "items", "required", "default", "description")

# * ----
# * auth
# * ----
AUTH_TYPES = MappingProxyType({
    "oauth2_refresh_token": {"required": ("token_url", "client_id", "client_secret", "refresh_token"),
                             "optional": ("scopes",)},
    "api_key": {"required": ("name", "value"), "optional": ("in",)},
    "bearer": {"required": ("token",), "optional": ()},
    "basic": {"required": ("username", "password"), "optional": ()},
})
API_KEY_LOCATIONS = ("header", "query")
SECRET_KEY_HINTS = ("secret", "token", "password")  # auth keys whose literal values should be secrets

# * ----------------------
# * HTTP, retries and limits
# * ----------------------
HTTP_KEYS = ("base_url", "headers", "rate_limit", "retry", "paginator", "records", "params_encoding")
RATE_LIMIT_KEYS = ("requests", "per")
# not `on`: YAML 1.1 reads the key `on` as the boolean true
RETRY_KEYS = ("codes", "max_attempts", "backoff", "max_delay")
BACKOFF_TYPES = ("exponential", "constant")

# * -------
# * streams
# * -------
# a stream reads its own `requests` (each one a table named after it), shapes them with the SQL `steps` of its
# `transform` and writes the steps its `export` names
STREAM_KEYS = ("kind", "name", "description", "partitions", "incremental", "requests", "transform", "export",
               "on_partition_error", "on_error", "retry", "rate_limit", "timeout")
REQUIRED_STREAM_KEYS = ("name", "requests", "transform", "export")
REQUEST_COLUMNS = ("record", "partition", "config", "window_start", "window_end", "today")  # of each request's table
# fail (raise), skip (tolerate and mark the stream partial), warn (tolerate without marking it partial)
ERROR_POLICIES = ("fail", "skip", "warn")
TRANSFORM_KEYS = ("mode", "steps")
TRANSFORM_MODES = ("page", "run")
STEP_KEYS = ("name", "select", "description")
EXPORT_KEYS = ("step", "primary_key", "description", "file_name")
# source.yaml `export`: defaults for every export of the source (a stream's export `file_name` replaces it)
SOURCE_EXPORT_KEYS = ("file_name",)
PARTITION_VALUES_KEYS = ("name", "values")
PARTITION_STREAM_KEYS = ("name", "from_stream", "field")  # one field of each parent record
PARTITION_RECORD_KEYS = ("from_stream", "fields")         # several fields of each parent record, named after them
REQUEST_PARTITION_VALUES_KEYS = PARTITION_VALUES_KEYS
REQUEST_PARTITION_SOURCE_KEYS = ("name", "from", "field")
REQUEST_PARTITION_RECORD_KEYS = ("from", "fields")
BATCH_SIZE = "batch_size"  # on request partitions: lists of up to N values of one field, one request each
INCREMENTAL_KEYS = ("cursor_field", "start", "window", "lookback")

REQUEST_KINDS = ("http", "sdk", "async_job")
HTTP_REQUEST_KEYS = ("path", "method", "params", "headers", "json", "params_encoding")
HTTP_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE")
PARAMS_ENCODING_TYPES = ("plain", "dotted")
PAGINATOR_IN_LOCATIONS = ("query", "body")
# per-request values are the operation's `arguments` (sdk requests take no `headers`)
SDK_REQUEST_KEYS = ("sdk", "service", "method", "arguments")
REQUEST_ITEM_BASE_KEYS = ("name", "paginator", "records", "partitions")
REQUEST_ITEM_KEYS = (*REQUEST_ITEM_BASE_KEYS, "http", "async_job", *SDK_REQUEST_KEYS)
ASYNC_JOB_KEYS = ("submit", "poll", "download", "results")
POLL_KEYS = ("method", "arguments", "every", "timeout", "done_when", "fail_when")
POLL_CONDITION_KEYS = ("path", "equals", "in")
DOWNLOAD_KEYS = ("url", "format", "compression")
DOWNLOAD_FORMATS = ("csv", "jsonl", "json")
COMPRESSIONS = ("zip", "gzip", "none")

PAGINATOR_TYPES = MappingProxyType({
    "none": {"required": (), "optional": ()},
    "offset": {"required": ("offset_param", "limit_param", "page_size"), "optional": ("start", "total_path", "in")},
    "cursor": {"required": (), "optional": ("token_path", "param", "next_url_path", "has_more_path", "in")},
    "page_number": {"required": ("page_param",),
                    "optional": ("page_size", "size_param", "start", "total_pages_path", "in")},
    "link_header": {"required": (), "optional": ()},
})
RECORDS_KEYS = ("path", "explode")

# * ---------------------
# * references: {{ ... }}
# * ---------------------
SCOPES = ("config", "secrets", "partition", "window", "response", "submit", "poll", "today")
WINDOW_ATTRIBUTES = ("start", "end")
# name -> number of arguments
FILTERS = MappingProxyType({"default": 1, "join": 1, "date": 1, "int": 0, "lower": 0, "upper": 0})


def split_outside_quotes(text, separator):
    """Splits on `separator` outside single or double quotes; parts are stripped. Shared by checks and runtime."""
    parts, current, quote = [], "", None
    for char in text:
        if quote:
            quote = None if char == quote else quote
        elif char in "'\"":
            quote = char
        elif char == separator:
            parts.append(current.strip())
            current = ""
            continue
        current += char
    parts.append(current.strip())
    return parts
