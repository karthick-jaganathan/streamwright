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
The log context: who a log line is about (job, account, batch, step, ...), set by the engines around their work and
copied onto every record (the record factory), so lines of runs and passes that run side by side tell themselves
apart. The keys are fixed (CONTEXT_FIELDS); a manifest adds only `labels` (runtime.log.labels), its own namespace.

Text lines show `[job label=value ... item step=... | output=... | lookup=...]` after the logger name; JSON lines carry
every key (labels as one object). Values are text, without newlines, at most 120 characters, and scrubbed like the
message. A thread started for a run's work takes the context with it: `bind(function)`.
"""

import contextlib
import contextvars
import re
from types import MappingProxyType

__all__ = ["CONTEXT_FIELDS", "LABEL_PATTERN", "MAX_LABELS", "bind", "context_tag", "context_value", "current_context",
           "log_context", "stamp"]

CONTEXT_FIELDS = ("job", "item", "scope", "job_id", "step", "output", "lookup")
LABEL_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
MAX_LABELS = 8
_MAX_VALUE = 120
_TAGGED = ("step", "output", "lookup")       # what the text tag shows after job, labels and item
_EMPTY = MappingProxyType({})
_CONTEXT = contextvars.ContextVar("streamwright_log_context", default=_EMPTY)


def _clean(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        value = ",".join(map(str, value))
    text = " ".join(str(value).split())
    return (text if len(text) <= _MAX_VALUE else text[:_MAX_VALUE - 3] + "...") or None


@contextlib.contextmanager
def log_context(labels=None, **values):
    """Adds `values` (keys of CONTEXT_FIELDS) and `labels` to the log context for the block; the outer ones return."""
    unknown = sorted(set(values) - set(CONTEXT_FIELDS))
    if unknown:
        raise ValueError("unknown log context key(s) %s (keys: %s)" % (", ".join(unknown), ", ".join(CONTEXT_FIELDS)))
    merged = dict(_CONTEXT.get())
    for key, value in values.items():
        cleaned = _clean(value)
        if cleaned is None:
            merged.pop(key, None)
        else:
            merged[key] = cleaned
    if labels:
        found = dict(merged.get("labels") or {})
        for name, value in labels.items():
            if not LABEL_PATTERN.match(str(name)) or name in CONTEXT_FIELDS:
                raise ValueError("invalid log label name %r" % (name,))
            cleaned = _clean(value)
            if cleaned is not None:
                found[name] = cleaned
        merged["labels"] = MappingProxyType(found)
    token = _CONTEXT.set(MappingProxyType(merged))
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def current_context():
    """The log context in effect (a read-only mapping; empty outside a run)."""
    return _CONTEXT.get()


def bind(function):
    """`function` run in a copy of the caller's context: a thread started for a run's work keeps its log context."""
    context = contextvars.copy_context()
    return lambda *args, **kwargs: context.run(function, *args, **kwargs)


def stamp(record):
    """
    Attaches the context to a record where it is made (the emitting thread): `log_context` (the mapping) and
    `context_tag` (its text tag). Not as the keys themselves: a log call's extra= may name them (job_id), and wins.
    """
    record.log_context = context = _CONTEXT.get()
    record.context_tag = context_tag(context)


def context_value(record, key):
    """A context key of a record: the field the log call gave (extra=), else the record's context."""
    value = getattr(record, key, None)
    if value is None:
        value = (getattr(record, "log_context", None) or {}).get(key)
    return dict(value) if key == "labels" and value is not None else value


def context_tag(context):
    """` [job label=value item step=name]` for a text line, or "" without a context."""
    parts = [context["job"]] if context.get("job") else []
    parts += ["%s=%s" % item for item in (context.get("labels") or {}).items()]
    if context.get("item"):
        parts.append(context["item"])
    parts += ["%s=%s" % (key, context[key]) for key in _TAGGED if context.get(key)]
    return " [%s]" % " ".join(parts) if parts else ""
