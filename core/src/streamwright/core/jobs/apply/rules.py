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
Apply outputs (an output that calls a connector's `apply` operation, see streamwright.core.sdk.apply): what one may
declare and how it is checked, for every job engine.

    - name: upload_conversions
      connection: ads_api                     # a connector whose operation is an apply operation
      step: valid_conversions                 # or sql: the rows; each row is one operation's payload
      operation:
        service: ConversionUploadService
        method: upload_click_conversions
        arguments: {customer_id: "{{ config.customer_id }}"}
      apply:
        chunk_size: 2000                      # rows per call (default and maximum: the operation's max_chunk)
        on_row_error: fail                    # fail | continue: a rejected row stops the run (exit 1) or is recorded
        nulls: error                          # error | skip_field | clear: a NULL field fails its row, is left out,
                                              # or clears the field
        on_unknown: hold                      # hold | reconcile: a call cut short of a non-idempotent operation
        allow_remove: false                   # rows may use the operation's destructive actions (e.g. op: remove)
      policy:
        retry: {max_attempts: 5, backoff: exponential}
        rate_limit: {requests: 5, per: 1s}

A REST API connector that declares `http.send` takes a request instead (streamwright.core.jobs.apply.http_send):

      operation:
        path: /conversions                    # or /campaigns/{{ row.id }}
        method: POST                          # POST | PUT | PATCH | DELETE (PUT and DELETE are idempotent)
        json: {data: "{{ rows }}"}            # optional: the body (default: the row); {{ rows }}: a request per chunk

The checks run on the engines' internal (flat) shape (streamwright.core.jobs.grammar); paths are mapped back.
"""

import dataclasses

from streamwright.core.jobs.apply.http_send import sends_chunk
from streamwright.core.jobs.grammar import APPLY_FIELD, NULLS, ON_ROW_ERROR, ON_UNKNOWN, public_key
from streamwright.core.jobs.policy import APPLY_POLICY_KEYS
from streamwright.core.jobs.refs import references, walk_strings
from streamwright.core.runtime import components
from streamwright.core.runtime.apply import APPLY_CAPABILITIES
from streamwright.core.runtime.contract import OperationKind
from streamwright.core.runtime.contract_validation import check_arguments
from streamwright.core.runtime.operations.http import IDEMPOTENT_METHODS, SEND

__all__ = ["APPLY_FIELD", "APPLY_OUTPUT_KEYS", "APPLY_POLICY_KEYS", "DEFAULT_APPLY_ROWS", "NULLS", "ON_ROW_ERROR",
           "ON_UNKNOWN", "REQUEST", "apply_operation", "check_apply", "is_apply_output"]

DEFAULT_APPLY_ROWS = 100_000                 # runtime.limits.apply_rows: the most rows one apply output sends per batch
REQUEST = "request"                          # an output's request (its `operation:` with a path), internal shape
APPLY_OUTPUT_KEYS = frozenset(("name", "description", "connection", "step", "sql", "service", "method", "arguments",
                               REQUEST, "chunk_size", "on_row_error", "nulls", "on_unknown", "allow_remove", "log",
                               *APPLY_POLICY_KEYS))


def _definition(provider):
    try:
        return components.load(provider)
    except components.ComponentLoadError:
        return None


def is_apply_output(item, connection, connectors):
    """Whether an output calls a connector (no engine adapter of its provider) with service/method or a request."""
    return (isinstance(item, dict) and ("service" in item or "method" in item or REQUEST in item)
            and isinstance(connection, dict) and connection.get("provider") in connectors)


def _called(item):
    """The (service, method) an apply output names: a request is the connector's `http.send`."""
    return (SEND.service, SEND.method) if REQUEST in item else (item.get("service"), item.get("method"))


def apply_operation(item, connection):
    """
    (ConnectorDefinition, Operation) of an apply output's service.method or request, or (definition or None, None).
    A request's operation is idempotent when its verb is (PUT, DELETE).
    """
    definition = _definition(connection.get("provider"))
    if definition is None:
        return None, None
    operation = definition.operation(*_called(item))
    if operation is None or operation.kind != OperationKind.APPLY:
        return definition, None
    if REQUEST in item and isinstance(item[REQUEST], dict):
        verb = str(item[REQUEST].get("method") or "POST").upper()
        operation = dataclasses.replace(operation, idempotent=verb in IDEMPOTENT_METHODS)
    return definition, operation


def _request_problems(request):
    """A request output's row references: `{{ row }}` per row, `{{ rows }}` only in `json` (a request per chunk)."""
    if not isinstance(request, dict):
        return []
    found = []
    chunked = sends_chunk(request)
    for key, value in request.items():
        for text in walk_strings(value):
            for expression in references(text):
                if expression == "rows" and key != "json":
                    found.append(("bad-value", "{{ rows }} is only used in json (the chunk's rows as one body)", False))
                elif chunked and (expression == "row" or expression.startswith("row.")):
                    found.append(("bad-value", "{{ %s }}: with {{ rows }} in json a chunk is one request, so no "
                                               "single row's value can be used" % expression, False))
    return found


def _choice(item, key, choices):
    if key in item and item[key] not in choices:
        return [("bad-value", "%s is one of: %s" % (key, ", ".join(choices)), False)]
    return []


def check_apply(item, connection):
    """Problems of an apply output, as [(code, message, is_warning)] (its connector is installed)."""
    definition, operation = apply_operation(item, connection)
    if operation is None:
        offered = sorted(op.name for op in (definition.operations if definition else ())
                         if op.kind == OperationKind.APPLY)
        what = "an HTTP request output" if REQUEST in item else "apply operation %s.%s" % _called(item)
        return [("bad-value", "%s has no %s (apply operations: %s)" % (
            connection.get("provider"), what, ", ".join(offered) or "none"), False)]
    arguments = item.get(REQUEST) if REQUEST in item else item.get("arguments")
    found = [("bad-call", "%s%s" % ("%s: " % issue.path if issue.path else "", issue.message),
              issue.severity != "error")
             for issue in check_arguments(definition, operation, arguments, APPLY_CAPABILITIES)]
    if REQUEST in item:
        found += _request_problems(item[REQUEST])
    if ("sql" in item) == ("step" in item):
        found.append(("bad-value", "an apply output needs `sql` or `step` (its rows are the operations)", False))
    extra = sorted(set(item) - APPLY_OUTPUT_KEYS)
    if extra:
        found.append(("unsupported-key", "an apply output takes no %s (keys: %s)" % (
            ", ".join(public_key(key) for key in extra),
            ", ".join(sorted(public_key(key) for key in APPLY_OUTPUT_KEYS))), False))
    size = item.get("chunk_size")
    if size is not None and (isinstance(size, bool) or not isinstance(size, int)
                             or not 1 <= size <= operation.max_chunk):
        found.append(("bad-value", "chunk_size is an integer from 1 to %d (%s's max_chunk)" % (
            operation.max_chunk, operation.name), False))
    found += (_choice(item, "on_row_error", ON_ROW_ERROR) + _choice(item, "nulls", NULLS)
              + _choice(item, "on_unknown", ON_UNKNOWN))
    if "allow_remove" in item and not isinstance(item["allow_remove"], bool):
        found.append(("bad-value", "allow_remove is true or false", False))
    elif item.get("allow_remove") and not operation.destructive_actions:
        found.append(("bad-value", "allow_remove: %s has no destructive action" % operation.name, True))
    if "retry" in item and not operation.idempotent:
        found.append(("bad-value", "retry: %s is not idempotent, so a call is sent once (a call cut short is held "
                      "or reconciled per on_unknown)" % operation.name, True))
    if item.get("on_unknown") == "reconcile" and operation.idempotent:
        found.append(("bad-value", "on_unknown: %s is idempotent: a cut-short call is simply sent again" %
                      operation.name, True))
    return found
