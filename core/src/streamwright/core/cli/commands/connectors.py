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

"""`streamwright connectors`: the installed connectors, and the catalog's (list, search, install)."""

import re
import sys
from importlib import metadata

import duckdb

from streamwright.core import catalog
from streamwright.core.runtime import components, plugins
from streamwright.core.runtime.logs.lines import NETWORK

_REQUIREMENT_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")
# a connector that logs on streamwright.network calls through StreamWright's HTTP client, which is built on requests
HTTP_CLIENT_LIBRARY = "requests"


def _installed(name):
    try:
        return "%s==%s" % (name, metadata.version(name))
    except metadata.PackageNotFoundError:
        return "%s (not installed)" % name


def libraries(distribution, loggers=()):
    """
    The third-party libraries a connector uses, as "name==version" (or "name (not installed)"): what its distribution
    requires (not its extras', not StreamWright's), and requests when it calls through StreamWright's HTTP client.
    """
    try:
        requires = (metadata.requires(distribution) or []) if distribution else []
    except metadata.PackageNotFoundError:
        requires = []
    names = []
    for requirement in requires:
        spec, _, marker = requirement.partition(";")
        match = _REQUIREMENT_NAME.match(spec)
        if "extra" not in marker and match and not match.group(1).lower().startswith("streamwright"):
            names.append(match.group(1))
    if NETWORK.name in loggers and HTTP_CLIENT_LIBRARY not in names:
        names.append(HTTP_CLIENT_LIBRARY)
    return [_installed(name) for name in names]


class DuckDBExtensions:
    """The installed DuckDB's extensions (one in-memory query, made when a connector names one): name@version."""

    def __init__(self):
        self._installed = None

    def _load(self):
        self._installed = {}
        rows = duckdb.connect().execute(
            "SELECT extension_name, aliases, installed, extension_version FROM duckdb_extensions()").fetchall()
        for name, aliases, installed, version in rows:
            for key in (name, *(aliases or ())):
                self._installed[key] = (name, version if installed else None)
        self._installed[None] = ("duckdb", duckdb.__version__)

    def describe(self, names):
        """("duckdb==VERSION", ["EXTENSION@VERSION" or "EXTENSION (not installed)", ...]); (None, []) for no names."""
        if not names:
            return None, []
        if self._installed is None:
            self._load()
        found = []
        for name in names:
            extension, version = self._installed.get(name, (name, None))
            found.append("%s@%s" % (extension, version) if version else "%s (not installed)" % extension)
        return "duckdb==%s" % self._installed[None][1], found


def _fields(name, definition, entry, extensions):
    """A connector's `pip show`-style fields."""
    used = libraries(entry.get("distribution"), definition.loggers)
    duckdb_library, loaded = extensions.describe(definition.duckdb_extensions)
    return (("Name", name), ("Title", definition.title), ("Family", definition.family),
            ("Summary", definition.summary), ("Package", entry.get("distribution") or "(registered in-process)"),
            ("Version", entry.get("version") or ""), ("Libraries", ", ".join(used + [duckdb_library] * bool(loaded))),
            ("DuckDB-Extensions", ", ".join(loaded)), ("Loggers", ", ".join(definition.loggers)))


def _connector_lines(names=None):
    """
    The installed (and registered) connectors, sorted by family and name, as `pip show` shows packages (`---` between
    them): name, title, family, summary, distribution and version, the third-party libraries and DuckDB extensions it
    uses (installed versions) and its loggers (for --log). `names`: only these. Only specs are imported.
    """
    metadata_of = {entry["name"]: entry for entry in plugins.discover(plugins.CONNECTORS)}
    blocks, extensions = [], DuckDBExtensions()
    for name in components.available():
        if names is not None and name not in names:
            continue
        try:
            definition = components.load(name)
        except components.ComponentLoadError as exc:
            blocks.append(("~", name, (("Name", name), ("Error", str(exc)))))  # (after the families)
            continue
        blocks.append((definition.family, name, _fields(name, definition, metadata_of.get(name, {}), extensions)))
    lines = []
    for _, _, fields in sorted(blocks, key=lambda block: block[:2]):
        if lines:
            lines.append("---")
        lines.extend(("%s: %s" % (key, value)).rstrip() for key, value in fields)
    return lines


def run_connectors_command(args):
    """`streamwright connectors`: bare or installed names = installed connectors; ls/i/find use the catalog (hub)."""
    if getattr(args, "flag_list", False):
        action = "ls"
        query = []
    elif getattr(args, "flag_install", None):
        action = "install"
        query = args.flag_install
    elif getattr(args, "flag_search", None):
        action = "find"
        query = args.flag_search
    else:
        action = getattr(args, "action", None)
        query = getattr(args, "query", [])

    if action is None:
        lines = _connector_lines()
        print("\n".join(lines) if lines else "no connectors installed (use streamwright connectors list to find one)")
        return 0

    norm_action = action.lower()
    if norm_action in ("ls", "list", "-ls", "--list"):
        lines = catalog.listing(args.hub_url)
        print("\n".join(lines) if lines else "the catalog is empty")
        return 0

    if norm_action in ("i", "install", "-i", "--install"):
        if not query:
            sys.stderr.write("streamwright: 'connectors -i' needs at least one connector KEY\n")
            return 2
        return catalog.install_many(query, assume_yes=args.yes, hub_url=args.hub_url)

    words = [action, *query]
    if norm_action in ("find", "search", "s", "-s", "-f", "--find", "--search"):
        words = query
    elif set(words) <= set(components.available()):
        # installed connector names show those connectors, as `pip show NAME` does (`sw connectors google_ads s3`)
        print("\n".join(_connector_lines(set(words))))
        return 0
    return _search(" ".join(words), args.hub_url)  # any other words search the catalog (`sw connectors ads`)


def _search(text, hub_url):
    hits = catalog.search(text, hub_url)
    if not hits:
        print("no connectors match %r" % text)
    for key, entry in sorted(hits.items()):
        print("  %-16s %-10s %s" % (key, entry.get("trust", "community"), entry.get("summary", "")))
    return 0
