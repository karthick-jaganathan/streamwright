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
File-name and folder templates for split exports (`split_by`: one file per distinct split value combination),
modelled on the Linux `date` command.

    {today}                      -> 20261017          the batch's UTC date (default format %Y%m%d)
    {today:%Y-%m-%d}             -> 2026-10-17
    {today-1d:%Y%m%d}            -> 20261016          offsets like `date -d "yesterday"`: +/-N then s m h d w
    {now:%Y%m%dT%H%M%SZ}         -> 20261017T031500Z  the batch's UTC time
    {event_date:%Y%m%d}          -> 20261015          a split column holding a date, timestamp or ISO text
    {client_id}                  -> acme              a split column, sanitized
    {batch_id}                   -> the batch id
    {seq}                        -> 00001             the split's 5 digit number (splits ordered by their values)
    {<constant>}                 -> an engine run constant (e.g. Spark's driver_host), sanitized

`today` and `now` are the *batch* time, pinned in the manifest when the batch is first received, so a replay after
midnight renders the same names and overwrites the same objects. Formats are strftime directives, always UTC and
English. A `-` flag drops zero padding (`%-d`), as in GNU date.

Folders (`layout`) may contain `/` and Hive-style parts; a `file_name` may not contain a path separator. Values
from data are reduced to [A-Za-z0-9._-]; the result may not be empty, `.` or `..`.
"""

import datetime
import re
from types import MappingProxyType

__all__ = ["RESERVED", "NamingError", "Template", "parse_template", "plan_names", "sanitize"]

RESERVED = frozenset(["now", "today", "batch_ts", "batch_id", "seq"])
_TIME_SOURCES = frozenset(["now", "today", "batch_ts"])
_PLACEHOLDER = re.compile(r"\{([^{}:]*)(?::([^{}]*))?\}")
_REF = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)((?:[+-][0-9]{1,6}[smhdw])*)$")
_OFFSET = re.compile(r"([+-])([0-9]{1,6})([smhdw])")
_OFFSET_SECONDS = MappingProxyType({"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800})
_LITERAL = re.compile(r"^[A-Za-z0-9._=,@+\-]*$")
_SAFE = re.compile(r"[^A-Za-z0-9._-]")
_DIRECTIVE = re.compile(r"%(-?)([A-Za-z%])")
_DAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
_MONTHS = ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
           "November", "December")
_DEFAULT_FORMAT = MappingProxyType({"today": "%Y%m%d", "now": "%Y%m%dT%H%M%SZ", "batch_ts": "%Y%m%dT%H%M%SZ"})
_MAX_NAME = 255


class NamingError(ValueError):
    pass


def _directive(directive, when):
    """One strftime directive with GNU `date` semantics: (padded text, unpadded text or None)."""
    iso = when.isocalendar()
    day_of_year = when.timetuple().tm_yday
    table = {
        "Y": ("%04d" % when.year, None), "y": ("%02d" % (when.year % 100), None),
        "m": ("%02d" % when.month, str(when.month)), "d": ("%02d" % when.day, str(when.day)),
        "e": ("%2d" % when.day, str(when.day)),
        "H": ("%02d" % when.hour, str(when.hour)), "M": ("%02d" % when.minute, str(when.minute)),
        "S": ("%02d" % when.second, str(when.second)),
        "I": ("%02d" % (when.hour % 12 or 12), str(when.hour % 12 or 12)),
        "j": ("%03d" % day_of_year, str(day_of_year)),
        "u": (str(iso[2]), None), "w": (str(when.isoweekday() % 7), None),
        "U": ("%02d" % int((day_of_year + 6 - (when.isoweekday() % 7)) // 7), None),
        "G": ("%04d" % iso[0], None), "V": ("%02d" % iso[1], str(iso[1])),
        "a": (_DAYS[when.weekday()][:3], None), "A": (_DAYS[when.weekday()], None),
        "b": (_MONTHS[when.month - 1][:3], None), "B": (_MONTHS[when.month - 1], None),
        "p": ("AM" if when.hour < 12 else "PM", None),
        "Z": ("UTC", None), "z": ("+0000", None),
        "s": (str(int(when.timestamp())), None),
        "F": ("%04d-%02d-%02d" % (when.year, when.month, when.day), None),
        # no %T: it renders colons, which are unsafe in object names and Windows paths (use %H%M%S)
        "%": ("%", None),
    }
    return table.get(directive)


def _check_format(fmt, where):
    if fmt == "":
        raise NamingError("%s: empty format" % where)
    for match in _DIRECTIVE.finditer(fmt):
        flag, directive = match.groups()
        entry = _directive(directive, datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC))
        if entry is None:
            raise NamingError("%s: unsupported directive %%%s (supported: Y y m d e H M S I j u w U G V a A b B p Z z "
                              "s F %%)" % (where, directive))
        if flag and entry[1] is None:
            raise NamingError("%s: the - flag does not apply to %%%s" % (where, directive))
    stripped = _DIRECTIVE.sub("", fmt)
    if "%" in stripped:
        raise NamingError("%s: stray %% in format %r" % (where, fmt))
    leftovers = re.sub(r"[A-Za-z0-9._=,@+\-/]", "", stripped)
    if leftovers:
        raise NamingError("%s: characters not allowed in a format: %r" % (where, leftovers))


def _render_format(fmt, when):
    def one(match):
        flag, directive = match.groups()
        padded, plain = _directive(directive, when)
        return plain if (flag and plain is not None) else padded
    return _DIRECTIVE.sub(one, fmt)


def _as_datetime(value, where):
    if isinstance(value, datetime.datetime):
        when = value
    elif isinstance(value, datetime.date):
        when = datetime.datetime(value.year, value.month, value.day)
    elif isinstance(value, str):
        text = value.strip()
        try:
            when = datetime.datetime.fromisoformat(text.replace("Z", "+00:00")[:32])
        except ValueError:
            raise NamingError("%s: %r is not a date or an ISO timestamp" % (where, text[:40])) from None
    else:
        raise NamingError("%s: %s is not a date or a timestamp" % (where, type(value).__name__))
    if when.tzinfo is None:
        return when.replace(tzinfo=datetime.UTC)
    return when.astimezone(datetime.UTC)


def sanitize(value):
    """Reduces a data value to [A-Za-z0-9._-]; raises when nothing usable remains."""
    text = _SAFE.sub("_", str(value).strip())
    if text in ("", ".", ".."):
        raise NamingError("value %r cannot be used in a file name" % (str(value)[:40],))
    return text


class _Part:
    def __init__(self, kind, text="", name="", offset=0, fmt=None):
        self.kind, self.text, self.name, self.offset, self.fmt = kind, text, name, offset, fmt


class _Run:
    """The values one render uses."""

    def __init__(self, values, batch_time, batch_id, seq, constants):
        self.values, self.batch_time, self.batch_id, self.seq, self.constants = (
            values, batch_time, batch_id, seq, constants)


def _format(part, when):
    return _render_format(part.fmt or _DEFAULT_FORMAT.get(part.name, "%Y%m%d"), when)


def _render_column(part, run):
    if part.name not in run.values:
        raise NamingError("split value for %r is missing" % part.name)
    value = run.values[part.name]
    if part.fmt is None and part.offset == 0:
        return sanitize("" if value is None else value)
    return _format(part, _as_datetime(value, "{%s}" % part.name) + datetime.timedelta(seconds=part.offset))


def _render_constant(part, run):
    if part.name not in run.constants:
        raise NamingError("run constant %r has no value" % part.name)
    return sanitize(run.constants[part.name])


_RENDERERS = MappingProxyType({
    "literal": lambda part, run: part.text,
    "batch_id": lambda part, run: sanitize(run.batch_id),
    "seq": lambda part, run: "%05d" % run.seq,
    "time": lambda part, run: _format(part, run.batch_time + datetime.timedelta(seconds=part.offset)),
    "column": _render_column,
    "constant": _render_constant,
})


class Template:
    """A parsed template. `columns` is the set of split columns it uses."""

    def __init__(self, text, parts, allow_slash):
        self.text, self.parts, self.allow_slash = text, parts, allow_slash
        self.columns = sorted({p.name for p in parts if p.kind == "column"})
        self.uses_seq = any(p.kind == "seq" for p in parts)

    def render(self, values, batch_ts, batch_id="", seq=0, constants=None):
        """`values`: split column -> value for one split; `batch_ts`: the pinned batch time (datetime or ISO text)."""
        run = _Run(values, _as_datetime(batch_ts, "batch_ts"), batch_id, seq, constants or {})
        return self._finish("".join(_RENDERERS[part.kind](part, run) for part in self.parts))

    def _finish(self, text):
        if not self.allow_slash and "/" in text:
            raise NamingError("a file name may not contain '/': %r" % text)
        pieces = text.split("/") if self.allow_slash else [text]
        for piece in pieces:
            if piece in ("", ".", ".."):
                raise NamingError("empty or relative path part in %r" % text)
            if _SAFE.search(piece.replace("=", "").replace(",", "").replace("@", "").replace("+", "")):
                raise NamingError("unsafe characters in %r" % piece)
            if len(piece) > _MAX_NAME:
                raise NamingError("a path part is longer than %d characters" % _MAX_NAME)
        return "/".join(pieces)


def parse_template(text, split_columns=(), allow_slash=False, name="template", constants=()):
    """Parses and checks a template; `constants` are the engine's run constants usable as `{name}`."""
    if not isinstance(text, str) or text == "":
        raise NamingError("%s must be non-empty text" % name)
    split_columns, constants = set(split_columns), set(constants)
    clash = split_columns & (RESERVED | constants)
    if clash:
        raise NamingError("split column(s) %s clash with reserved names (%s)" % (
            ", ".join(sorted(clash)), ", ".join(sorted(RESERVED | constants))))
    parts, position = [], 0
    for match in _PLACEHOLDER.finditer(text):
        _add_literal(parts, text[position:match.start()], allow_slash, name)
        position = match.end()
        parts.append(_placeholder(match.group(1), match.group(2), split_columns, constants, allow_slash=allow_slash,
                                  name=name))
    _add_literal(parts, text[position:], allow_slash, name)
    if not any(p.kind != "literal" for p in parts) and not allow_slash:
        raise NamingError("%s has no placeholder: every split would get the same name" % name)
    return Template(text, parts, allow_slash)


def _add_literal(parts, literal, allow_slash, name):
    if literal == "":
        return
    if "/" in literal and not allow_slash:
        raise NamingError("%s: a file name may not contain '/' (use layout for folders)" % name)
    if not _LITERAL.match(literal.replace("/", "") if allow_slash else literal):
        raise NamingError("%s: literal text %r may only use letters, digits and . _ - = , @ +" % (name, literal))
    if ".." in literal:
        raise NamingError("%s: '..' is not allowed" % name)
    parts.append(_Part("literal", text=literal))


def _placeholder(ref, fmt, split_columns, constants, *, allow_slash, name):
    where = "%s {%s}" % (name, ref + (":" + fmt if fmt is not None else ""))
    match = _REF.match(ref)
    if not match:
        raise NamingError("%s: expected {name}, {name:FORMAT} or {name+1d:FORMAT}" % where)
    source, offsets = match.group(1), match.group(2)
    seconds = sum((1 if sign == "+" else -1) * int(count) * _OFFSET_SECONDS[unit]
                  for sign, count, unit in _OFFSET.findall(offsets))
    if fmt is not None:
        _check_format(fmt, where)
        if "/" in fmt and not allow_slash:
            raise NamingError("%s: '/' in a format is only allowed in layout" % where)
    if source in ("batch_id", "seq") or source in constants:
        if offsets or fmt is not None:
            raise NamingError("%s takes no offset or format" % where)
        return _Part(source if source in ("batch_id", "seq") else "constant", name=source)
    if source in _TIME_SOURCES:
        return _Part("time", name=source, offset=seconds, fmt=fmt)
    if source not in split_columns:
        raise NamingError("%s: %r is not a split_by column (split_by: %s)" % (
            where, source, ", ".join(sorted(split_columns)) or "none"))
    if offsets or fmt is not None:
        return _Part("column", name=source, offset=seconds, fmt=fmt or "%Y%m%d")
    return _Part("column", name=source)


def plan_names(file_template, layout_template, splits, batch_ts, batch_id, *, constants=None):
    """
    Renders every split before any write: {split key: relative path}. `splits` is [(key, values)]; they are numbered
    1, 2, ... in key order for `{seq}`, so a replay gives every split the same number. Raises when two splits render
    the same path (including values that only differ by sanitizing).
    """
    names, seen = {}, {}
    for seq, (key, values) in enumerate(sorted(splits, key=lambda item: tuple(str(v) for v in item[0])), start=1):
        folder = layout_template.render(values, batch_ts, batch_id, seq, constants) if layout_template else ""
        base = file_template.render(values, batch_ts, batch_id, seq, constants)
        path = folder + "/" + base if folder else base
        if len(path) > 1000:
            raise NamingError("rendered path is too long for split %r" % (key,))
        if path in seen:
            raise NamingError("splits %r and %r both render %r; add a column or {seq} to file_name" % (
                seen[path], key, path))
        seen[path] = key
        names[key] = path
    return names
