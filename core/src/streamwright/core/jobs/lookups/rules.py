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

"""The rule tables of spec.lookups, an input's `lookup:` and runtime.fan_out (so the JSON schema has them), and the
checks the tables cannot express (what a binding names, fan_out's list and scope)."""


from streamwright.core.jobs.fields import Field
from streamwright.core.jobs.lookups.extract import FORMATS
from streamwright.core.jobs.lookups.providers import KINDS, TRANSFORM_KINDS
from streamwright.core.jobs.lookups.resolver import FAN_OUT_DEFAULTS
from streamwright.core.jobs.refs import MOUSTACHE

__all__ = ["FAN_OUT_FIELD", "INPUT_LOOKUP_FIELD", "LOOKUPS_FIELD", "check_lookups"]

_TEXT = Field("string")
_FORMAT = Field("string", enum=FORMATS, default="auto",
                description="auto: JSON when it is JSON (a JSON string holding JSON is unwrapped), else text; text: "
                            "never parsed; json; json_string: JSON serialized in a JSON string")
_TIMEOUT = Field(("number", "string"), default="30s", description="seconds, or 30s, 5m: the command is killed after")
_ARGV = Field("array", min_items=1, items=_TEXT,
              description="the program and its arguments (no shell); {{ config.* }}, {{ secrets.* }} and the fan_out "
                          "item's {{ <as>.* }} are rendered; pass sensitive values on stdin, not here")

INPUT_LOOKUP_FIELD = Field("object", fields={
    "from": Field("string", required=True, description="a lookup of spec.lookups, or the fan_out item (its `as`)"),
    "field": Field("string", description="a dot path or column in its value (credentials.clientId, items[0].id); "
                                         "default: the whole value"),
}, description="where the input's value comes from when it is not given explicitly (--set, --config, --secrets, "
               "STREAMWRIGHT_SECRET_*)")

_TRANSFORM = Field("object", fields={
    "type": Field("string", required=True, enum=TRANSFORM_KINDS),
    "format": _FORMAT,
}, discriminator="type", variants={
    "function": {"function": Field("string", required=True, description="module:function"),
                 "arguments": Field("object", description="keyword arguments; {{ row.COLUMN }} is the row's column")},
    "command": {"command": _ARGV, "stdin": Field("string", description="written to the command's stdin, e.g. "
                                                                       "{{ row.encrypted_token }}"),
                "timeout": _TIMEOUT},
    "builtin": {"apply": Field("string", required=True, enum=("base64", "base64url", "hex", "json")),
                "value": Field("string", required=True, description="e.g. {{ row.b64_secret }}")},
}, description="a value computed from each row (decrypt, decode); runs when the row is used, its input and output "
               "masked")

LOOKUPS_FIELD = Field("object", values=Field("object", fields={
    "type": Field("string", required=True, enum=KINDS),
    "format": _FORMAT,
}, discriminator="type", variants={
    "database": {
        "connection": Field("string", required=True, description="a postgres, mysql, duckdb or sqlite connection"),
        "query": Field("string", required=True,
                       description=":name parameters are bound; a list expands to (?, ?, ...) for IN, a missing one "
                                   "binds NULL"),
        "params": Field("object", description="name -> value, e.g. {{ config.client_id }}"),
        "rows": Field("string", enum=("one", "many"), default="one",
                      description="one: exactly one row; many: a list of rows (for runtime.fan_out.over)"),
        "secret_fields": Field("array", items=_TEXT, description="columns masked as soon as they are read"),
        "transforms": Field("object", values=_TRANSFORM),
    },
    "command": {"command": _ARGV, "stdin": _TEXT, "timeout": _TIMEOUT},
    "function": {"function": Field("string", required=True, description="module:function, called with arguments"),
                 "arguments": Field("object")},
    "aws_secretsmanager": {"secret_id": Field("string", required=True), "region": _TEXT},
}), description="values fetched at run time, before anything connects; command and function lookups and transforms "
                "need --allow-lookup-exec")

FAN_OUT_FIELD = Field("object", fields={
    "over": Field("string", required=True, description="a rows: many lookup, or a list input of spec.config"),
    "as": Field("string", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$", default=FAN_OUT_DEFAULTS["as"],
                description="the item's name: inputs bind it with lookup: {from: <as>, field: COLUMN}"),
    "key": Field("string", description="the row's column that names a pass (default: its first column)"),
    "concurrency": Field("integer", minimum=1, default=1, description="passes at once (--fan-out-concurrency)"),
    "on_failure": Field("string", enum=("continue", "stop"), default="continue",
                        description="continue: the other items still run; stop: no new pass after a failure"),
    "on_empty": Field("string", enum=("fail", "skip"), default="fail",
                      description="no item: fail (exit 1) or skip (exit 0, a warning)"),
}, description="run the whole job once per item (account): its lookups, connections, steps, outputs and state; "
               "runtime.state.scope must name the item, so each keeps its own history")



def check_lookups(job):
    """Problems of spec.lookups, inputs' `lookup:` and runtime.fan_out, as (path, message)."""
    spec = job.get("spec") if isinstance(job.get("spec"), dict) else {}
    lookups = spec.get("lookups") if isinstance(spec.get("lookups"), dict) else {}
    runtime = job.get("runtime") if isinstance(job.get("runtime"), dict) else {}
    fan_out = runtime.get("fan_out") if isinstance(runtime.get("fan_out"), dict) else None
    item = (fan_out or {}).get("as") or FAN_OUT_DEFAULTS["as"]
    config = spec.get("config") if isinstance(spec.get("config"), dict) else {}
    found, bound_to_item = [], set()
    for section in ("config", "secrets"):
        for name, definition in (spec.get(section) or {}).items():
            binding = definition.get("lookup") if isinstance(definition, dict) else None
            if not isinstance(binding, dict):
                continue
            source = binding.get("from")
            if fan_out and source == item:
                bound_to_item.add(name)
            elif source == item and source not in lookups:
                found.append(((section, name, "lookup", "from"), "%r is the fan_out item: declare runtime.fan_out"
                              % source))
            elif source not in lookups:
                found.append(((section, name, "lookup", "from"), "no lookup %r in spec.lookups" % source))
            elif (lookups[source] or {}).get("rows") == "many":
                found.append(((section, name, "lookup", "from"), "lookup %r has rows: many: only runtime.fan_out "
                                                                 "can use it" % source))
    if fan_out:
        over = fan_out.get("over")
        if fan_out.get("key") and fan_out["key"] in ((lookups.get(over) or {}).get("secret_fields") or ()):
            found.append((("runtime", "fan_out", "key"), "%r is a secret field of lookup %r: it would show in "
                                                         "every log line" % (fan_out["key"], over)))
        is_list = (config.get(over) or {}).get("type") == "list" if isinstance(config.get(over), dict) else False
        if over in lookups and (lookups[over] or {}).get("rows") != "many":
            found.append((("runtime", "fan_out", "over"), "lookup %r needs rows: many" % over))
        elif over not in lookups and not is_list:
            found.append((("runtime", "fan_out", "over"), "%r is neither a lookup nor a list input" % over))
        scope = str(((runtime.get("state") or {}).get("scope")) or "")
        named = {expr.split(".", 1)[1] for expr in MOUSTACHE.findall(scope) if expr.startswith("config.")}
        if not named & bound_to_item:
            found.append((("runtime", "state", "scope"), "with fan_out, the scope must name the item (e.g. "
                          "account:{{ config.account_id }} with account_id bound by lookup: {from: %s, field: ...}): "
                          "else every pass shares one scope" % item))
    return found
