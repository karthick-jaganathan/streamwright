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
Quoting for values assembled into SQL text (ANSI / DuckDB): identifiers in double quotes, literals in single quotes,
the quote character doubled. NUL can't be represented in either, so it is rejected.
"""

__all__ = ["quote_ident", "quote_literal", "quote_qualified", "sql_string"]


def sql_string(value):
    """Legacy SQL literal quoting; unlike quote_literal, accepts any str-convertible value."""
    return "'" + str(value).replace("'", "''") + "'"


def _text(value, what):
    if value is None or isinstance(value, bool):
        raise ValueError("cannot quote %r as an SQL %s" % (value, what))
    text = value if isinstance(value, str) else str(value)
    if "\x00" in text:
        raise ValueError("an SQL %s cannot contain a NUL character" % what)
    return text


def quote_ident(name):
    """`name` as a double-quoted identifier, embedded double quotes doubled."""
    text = _text(name, "identifier")
    if not text:
        raise ValueError("an SQL identifier cannot be empty")
    return '"%s"' % text.replace('"', '""')


def quote_literal(value):
    """`value` as a single-quoted string literal: it's -> 'it''s'."""
    return "'%s'" % _text(value, "literal").replace("'", "''")


def quote_qualified(name):
    """A dotted name (catalog.schema.table) with each part quoted."""
    return ".".join(quote_ident(part) for part in _text(name, "identifier").split("."))
