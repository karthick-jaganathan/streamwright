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
What a state database is, in words for logs and messages (`target`): its dialect and location, never a password.

    sqlite file /var/lib/sw/state.db                  sqlite in memory
    duckdb connection 'wh' (/data/wh.duckdb)          postgres sw@db.internal:5432/etl, schema streamwright
    postgres connection 'ops_db' (sw@db.internal:5432/etl), schema sw_ops
"""

import os
import urllib.parse
from collections.abc import Mapping
from typing import Any

__all__ = ["describe_connection", "describe_location", "describe_url"]

_IN_MEMORY = ("", ":memory:")


def describe_location(dialect: str, *, path: str | None = None, host: str | None = None, port: Any = None,
                      database: str | None = None, user: str | None = None, schema: str | None = None,
                      catalog: str | None = None) -> str:
    """A database's dialect and where it is: a file (or memory), or user@host:port/database, and its schema."""
    if dialect in ("sqlite", "duckdb") and host is None:
        where = "in memory" if (path or "") in _IN_MEMORY else "file %s" % os.path.abspath(path)
        return "%s %s%s" % (dialect, where, ", catalog %s" % catalog if catalog else "")
    server = "%s%s%s%s" % ("%s@" % user if user else "", host or "", ":%s" % port if port else "",
                           "/%s" % database if database else "")
    return "%s %s%s" % (dialect, server, ", schema %s" % schema if schema and dialect != "sqlite" else "")


def describe_connection(name: str, provider: str, values: Mapping[str, Any], schema: str | None = None) -> str:
    """A job connection that holds the state: its provider, name and location (its non-secret keys only)."""
    location = describe_location(provider, path=values.get("path"), host=values.get("host"), port=values.get("port"),
                                 database=values.get("database"), user=values.get("user") or values.get("username"),
                                 schema=None)
    detail = location.split(" ", 1)[1] if " " in location else ""
    return "%s connection %r (%s)%s" % (provider, name, detail, ", schema %s" % schema if schema else "")


def describe_url(url: str, dialect: str, schema: str | None = None, catalog: str | None = None) -> str:
    """A state database URL without its password and query parameters (which can carry credentials)."""
    parsed = urllib.parse.urlsplit(url)
    if dialect in ("sqlite", "duckdb"):
        path = urllib.parse.unquote(parsed.netloc + parsed.path)
        path = path[1:] if path.startswith("/") and not parsed.netloc else path
        return describe_location(dialect, path=path, catalog=catalog)
    return describe_location(dialect, host=parsed.hostname, port=parsed.port,
                             database=urllib.parse.unquote(parsed.path.lstrip("/")) or None,
                             user=urllib.parse.unquote(parsed.username) if parsed.username else None, schema=schema)
