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
A request's body (plan §5.6): what a caller may give a run, checked as `--set` and `--input` are, plus the service's
bindings. `{"set": {NAME: VALUE}, "input": [ROW, ...], "options": {"async": true}}`; nothing else.

- `set` is `--set`: typed and constrained by spec.config. An input the service binds (from a claim, or fixed), an
  input with a `lookup:`, a secret or an undeclared name is refused (422); a claim the caller's token lacks is 403.
- `input` is `--input`: only for a job with spec.input, which then needs it; rows are checked against spec.input.
- `options.async`: answer 202 at once instead of waiting for the run.
The body is canonical JSON-hashed (its fingerprint binds an Idempotency-Key to it); its values are never logged.
"""

import hashlib
import json
from dataclasses import dataclass

from streamwright.core.config.inputs import InputError, resolve_inputs
from streamwright.core.jobs.payload import PayloadError, input_rows, strict_json
from streamwright.core.jobs.refs import resolve

__all__ = ["BODY_KEYS", "Envelope", "RequestError", "read_envelope", "scope_key"]

BODY_KEYS = ("set", "input", "options")
OPTION_KEYS = ("async",)


class RequestError(Exception):
    """A request the service refuses: an HTTP status, an error code and a message safe to return."""

    def __init__(self, status, code, message, headers=None):
        super().__init__(message)
        self.status, self.code, self.message, self.headers = status, code, message, dict(headers or {})


@dataclass(frozen=True)
class Envelope:
    config: dict                 # the run's config values: the caller's `set` and the service's bindings
    rows: tuple | None           # the input rows, typed (None: the job takes none)
    run_async: bool
    fingerprint: str             # SHA-256 of the canonical body


def _invalid(message, code="invalid-request"):
    return RequestError(422, code, message)


def _body(raw):
    if not raw.strip():
        return {}
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise _invalid("the request body is not UTF-8 text") from None
    try:
        body = strict_json(text, "the request body")
    except PayloadError as exc:
        raise _invalid(str(exc)) from None
    if not isinstance(body, dict):
        raise _invalid("the request body is a JSON object: {\"set\": {...}, \"input\": [...], \"options\": {...}}")
    unknown = sorted(set(body) - set(BODY_KEYS))
    if unknown:
        raise _invalid("unknown key%s %s in the request body (keys: %s); server options such as allow_lookup_exec "
                       "or outputs are the service's, never a request's" % (
                           "s" if len(unknown) > 1 else "", ", ".join(map(repr, unknown)), ", ".join(BODY_KEYS)))
    return body


def _options(body):
    options = body.get("options", {})
    if not isinstance(options, dict):
        raise _invalid("`options` is a mapping, e.g. {\"async\": true}")
    unknown = sorted(set(options) - set(OPTION_KEYS))
    if unknown:
        raise _invalid("unknown option%s %s (a request's only option is `async`)" % (
            "s" if len(unknown) > 1 else "", ", ".join(map(repr, unknown))))
    if not isinstance(options.get("async", False), bool):
        raise _invalid("`options.async` is true or false")
    return bool(options.get("async", False))


def _refused(name, served, secrets):
    if name in served.entry.bound:
        source = served.entry.bound[name]
        return "config input %r is bound by the service (%s): a request does not set it" % (
            name, "from the caller's claim %r" % source[1] if source[0] == "claim" else "fixed")
    if name in secrets:
        return "%r is a secret: secrets are never set by a request" % name
    if name in served.config:
        return "config input %r has a lookup: it is filled by the lookup, never set by a request" % name
    return "unknown config input %r (settable: %s)" % (name, ", ".join(sorted(served.settable)) or "none")


def _config(body, served, principal, secrets):
    given = body.get("set", {})
    if not isinstance(given, dict):
        raise _invalid("`set` is a mapping of config inputs to values, as --set NAME=VALUE")
    refused = [_refused(name, served, secrets) for name in given if name not in served.settable]
    if refused:
        raise _invalid("; ".join(refused), "input-not-settable")
    values = dict(given)
    for name, (kind, source) in served.entry.bound.items():
        if kind == "value":
            values[name] = source
        elif source in principal.claims:
            values[name] = principal.claims[source]
        else:
            raise RequestError(403, "missing-claim", "the caller's credentials carry no claim %r, which config input "
                                                     "%r is bound to" % (source, name))
    # lookups fill their inputs at run time: here they are neither required nor checked
    checked = {name: definition for name, definition in served.config.items()
               if not (definition or {}).get("lookup") or name in values}
    try:
        resolve_inputs(checked, values, "config")
    except InputError as exc:
        raise _invalid(str(exc), "invalid-input") from None
    return values


def _rows(body, served, max_bytes):
    declared = served.input
    if declared is None:
        if "input" in body:
            raise _invalid("`input`: the job declares no spec.input (it takes no rows)")
        return None
    if "input" not in body:
        raise _invalid("the job takes input rows (spec.input): give them as `input`: [{...}, ...]")
    rows = body["input"]
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise _invalid("`input` is a list of objects (rows)")
    if max_bytes is not None and len(json.dumps(rows, separators=(",", ":"))) > max_bytes:
        raise RequestError(413, "too-large", "the input rows are larger than the job's limit (%d bytes)" % max_bytes)
    try:
        return input_rows(rows, declared).rows
    except PayloadError as exc:
        raise _invalid(str(exc), "invalid-input") from None


def read_envelope(raw, served, principal, secrets=(), max_bytes=None):
    """The Envelope of the request body `raw` (bytes) to run `served` (a ServedJob) for `principal`; RequestError."""
    body = _body(raw)
    run_async = _options(body)
    config = _config(body, served, principal, frozenset(secrets))
    rows = _rows(body, served, max_bytes)
    fingerprint = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"),
                                            ensure_ascii=False).encode()).hexdigest()
    return Envelope(config, rows, run_async, fingerprint)


def scope_key(served, config):
    """What a run works on: its runtime.state.scope with the request's config values ("" for a job without one)."""
    if not served.scope:
        return ""
    try:
        values = resolve_inputs({name: dict(definition or {}, required=False)
                                 for name, definition in served.config.items()}, dict(config), "config")
    except InputError:
        values = dict(config)
    rendered, _ = resolve(served.scope, values)
    return str(rendered)[:191]
