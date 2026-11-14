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
The lookup kinds: database (a query on a SQL connection of the job), command (an argument list, never a shell),
function (`module:function`) and aws_secretsmanager; and the per-row transforms of a database lookup (function,
command with stdin, builtin decoders). A command or function, as a lookup or a transform, runs only with
--allow-lookup-exec. Errors name the lookup, never a value.
"""

import base64
import binascii
import importlib
import json
import logging
import os
import re
import signal
import subprocess
import urllib.parse
from types import MappingProxyType

from streamwright.core.config.inputs import InputError, parse_duration
from streamwright.core.jobs.lookups.extract import parse_output
from streamwright.core.runtime.logs.context import log_context

__all__ = [
    "EXEC_KINDS",
    "KINDS",
    "SQL_PROVIDERS",
    "TRANSFORM_KINDS",
    "LookupExecNotAllowedError",
    "LookupFailedError",
    "fetch",
    "transform_row",
]

LOG = logging.getLogger("streamwright.lookups")
KINDS = ("database", "command", "function", "aws_secretsmanager")
EXEC_KINDS = frozenset({"command", "function"})
TRANSFORM_KINDS = ("function", "command", "builtin")
SQL_PROVIDERS = ("postgres", "mysql", "duckdb", "sqlite")
_PARAM = re.compile(r"(\bIN\s*)?(?<![:\w]):([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)
_DEFAULT_TIMEOUT = 30.0


class LookupFailedError(RuntimeError):
    """A lookup could not get its value (the run, or this account's pass, fails)."""


class LookupExecNotAllowedError(InputError):
    """A command or function lookup without --allow-lookup-exec (a usage error)."""


def _allowed(name, kind, context):
    if not context.allow_exec:
        raise LookupExecNotAllowedError("lookup %r runs a %s: allow it with --allow-lookup-exec" % (name, kind))


def fetch(name, spec, context):
    """
    The value of lookup `name`: a row (rows: one) or a list of rows (rows: many) for a database lookup, else the
    parsed output. `context` gives render(node), job, allow_exec and enrol(value) (masks a secret value).
    """
    kind = spec.get("type")
    if kind in EXEC_KINDS:
        _allowed(name, kind, context)
    if kind == "database":
        return _database(name, spec, context)
    if kind == "command":
        out = _run_command(name, context.render(spec["command"]), context.render(spec.get("stdin")),
                           spec.get("timeout"))
        return parse_output(out, spec.get("format"))
    if kind == "function":
        result = _function(spec["function"], name)(**(context.render(spec.get("arguments")) or {}))
        return parse_output(result, spec.get("format"))
    if kind == "aws_secretsmanager":
        return parse_output(_aws_secret(name, context.render(spec)), spec.get("format"))
    raise LookupFailedError("lookup %r: unknown type %r (%s)" % (name, kind, ", ".join(KINDS)))


# --- command and function -------------------------------------------------------------------------------------

def _run_command(name, argv, stdin, timeout):
    argv = [str(part) for part in argv]
    seconds = _DEFAULT_TIMEOUT if timeout is None else parse_duration(str(timeout)).total_seconds() \
        if not isinstance(timeout, (int, float)) else float(timeout)
    try:
        process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,  # argv, no shell
                                   stderr=subprocess.PIPE, text=True, start_new_session=True)
    except OSError as exc:
        raise LookupFailedError("lookup %r: cannot run %s: %s" % (name, argv[0], exc.strerror or exc)) from None
    try:
        out, err = process.communicate(None if stdin is None else str(stdin), timeout=seconds)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise LookupFailedError("lookup %r: %s timed out after %gs" % (name, argv[0], seconds)) from None
    if process.returncode:
        LOG.debug("lookup %r: %s stderr: %s", name, argv[0], (err or "").strip().split("\n", 1)[0][:200])
        raise LookupFailedError("lookup %r: %s failed with exit code %d" % (name, argv[0], process.returncode))
    return out


def _function(path, name):
    module, _, attribute = str(path).partition(":")
    if not module or not attribute:
        raise LookupFailedError("lookup %r: function must be module:function, not %r" % (name, path))
    try:
        found = getattr(importlib.import_module(module), attribute)
    except (ImportError, AttributeError) as exc:
        raise LookupFailedError("lookup %r: cannot load %s: %s" % (name, path, exc)) from None
    if not callable(found):
        raise LookupFailedError("lookup %r: %s is not callable" % (name, path))
    return found


# --- aws secrets manager --------------------------------------------------------------------------------------

def _aws_secret(name, spec):
    try:
        boto3 = importlib.import_module("boto3")
    except ImportError:
        raise LookupFailedError("lookup %r: boto3 is not installed (pip install boto3)" % name) from None
    try:
        reply = boto3.client("secretsmanager", region_name=spec.get("region")).get_secret_value(
            SecretId=spec["secret_id"])
    except Exception as exc:
        raise LookupFailedError("lookup %r: AWS Secrets Manager: %s" % (name, type(exc).__name__)) from None
    raw = reply.get("SecretString")
    return raw if raw is not None else (reply.get("SecretBinary") or b"").decode("utf-8")


# --- database -------------------------------------------------------------------------------------------------

def _database(name, spec, context):
    connections = context.job.get("connections") or {}
    connection = connections.get(spec.get("connection"))
    if not isinstance(connection, dict) or connection.get("provider") not in SQL_PROVIDERS:
        raise LookupFailedError("lookup %r: connection %r is not a %s connection of the job" % (
            name, spec.get("connection"), ", ".join(SQL_PROVIDERS)))
    connection = context.render(connection)
    resolved = _state_url(connection, context.base_dir)
    statement, args = _bind(spec["query"], context.render(spec.get("params")) or {})
    conn = resolved.connect()
    try:
        cursor = conn.execute(resolved.dialect.placeholders(statement), tuple(args))
        columns = [column[0] for column in cursor.description or ()]
        rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    except Exception as exc:
        raise LookupFailedError("lookup %r: the query failed: %s: %s" % (name, type(exc).__name__, exc)) from None
    finally:
        conn.close()
    for row in rows:
        for column in spec.get("secret_fields") or ():
            context.enrol(row.get(column))
    if spec.get("rows", "one") == "many":
        return rows
    if len(rows) != 1:
        raise LookupFailedError("lookup %r: the query returned %d rows (rows: one needs exactly one)"
                                % (name, len(rows)))
    return transform_row(name, spec, rows[0], context)


def _state_url(connection, base_dir):
    url_module = importlib.import_module("streamwright.core.jobs.state.url")
    provider = connection["provider"]
    if provider in ("duckdb", "sqlite"):
        path = str(connection.get("path") or connection.get("database") or ":memory:")
        if path != ":memory:" and not os.path.isabs(path) and base_dir:
            path = os.path.join(base_dir, path)
        return url_module.resolve_state_url("%s:///%s" % (provider, path))
    user = urllib.parse.quote(str(connection.get("user") or connection.get("username") or ""), safe="")
    password = connection.get("password")
    auth = user + (":" + urllib.parse.quote(str(password), safe="") if password else "")
    port = ":%s" % connection["port"] if connection.get("port") else ""
    query = "?sslmode=%s" % connection["sslmode"] if provider == "postgres" and connection.get("sslmode") else ""
    scheme = "postgresql" if provider == "postgres" else "mysql"
    return url_module.resolve_state_url("%s://%s@%s%s/%s%s" % (scheme, auth, connection.get("host") or "localhost",
                                                               port, connection.get("database") or "", query))


def _bind(query, params):
    """
    `:name` parameters as `?` placeholders. After IN, a list expands to (?, ?, ...) and an empty or missing one to
    (NULL); elsewhere (e.g. `:ids IS NULL`) a list binds as its comma-joined text, an empty one as NULL.
    """
    args = []

    def one(match):
        value = params.get(match.group(2))
        if match.group(1):
            values = list(value) if isinstance(value, (list, tuple)) else [] if value is None else [value]
            args.extend(values)
            return match.group(1) + ("(%s)" % ", ".join("?" * len(values)) if values else "(NULL)")
        if isinstance(value, (list, tuple)):
            value = ",".join(map(str, value)) or None
        args.append(value)
        return "?"

    return _PARAM.sub(one, query), args


# --- transforms -----------------------------------------------------------------------------------------------

_BUILTINS = MappingProxyType({
    "base64": lambda text: base64.b64decode(text).decode("utf-8"),
    "base64url": lambda text: base64.urlsafe_b64decode(text).decode("utf-8"),
    "hex": lambda text: bytes.fromhex(text).decode("utf-8"),
    "json": json.loads,
})


def transform_row(name, spec, row, context):
    """The row with its `transforms` added (decrypt, decode); every transform input and output is masked."""
    transforms = spec.get("transforms") or {}
    if not transforms:
        return row
    result = dict(row)
    for column, transform in transforms.items():
        with log_context(lookup="%s.transforms.%s" % (name, column)):
            result[column] = _transform(name, column, transform, row, context)
        context.enrol(result[column])
    return result


def _transform(name, column, transform, row, context):
    render = context.with_row(row).render
    kind = transform.get("type")
    if kind in EXEC_KINDS:
        _allowed("%s.transforms.%s" % (name, column), kind, context)
    if kind == "function":
        arguments = render(transform.get("arguments")) or {}
        for value in arguments.values():
            context.enrol(value)
        value = _function(transform["function"], name)(**arguments)
    elif kind == "command":
        stdin = render(transform.get("stdin"))
        context.enrol(stdin)
        value = _run_command("%s.transforms.%s" % (name, column), render(transform["command"]), stdin,
                             transform.get("timeout"))
    elif kind == "builtin":
        source = render(transform.get("value"))
        context.enrol(source)
        try:
            value = _BUILTINS[transform["apply"]](str(source))
        except KeyError:
            raise LookupFailedError("lookup %r: transform %r: unknown builtin %r (%s)" % (
                name, column, transform.get("apply"), ", ".join(_BUILTINS))) from None
        except (ValueError, binascii.Error, UnicodeDecodeError):
            raise LookupFailedError("lookup %r: transform %r: the value is not valid %s" % (
                name, column, transform["apply"])) from None
    else:
        raise LookupFailedError("lookup %r: transform %r: unknown type %r" % (name, column, kind))
    return parse_output(value, transform.get("format", "text" if kind == "command" else "auto"))
