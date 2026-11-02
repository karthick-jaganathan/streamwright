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

import sys

from streamwright.core import catalog
from streamwright.core.runtime import components, plugins
from streamwright.core.runtime.definitions import engine_reach


def _connector_lines():
    """
    The installed (and registered) connectors by family: name, title, distribution and version, and how each engine
    reaches them — native or via a call step, in the fixed order source, duckdb, spark (only specs are imported).
    """
    metadata = {entry["name"]: entry for entry in plugins.discover(plugins.CONNECTORS)}
    groups = {}
    for name in components.available():
        entry = metadata.get(name, {})
        package = "%s %s" % (entry.get("distribution") or "registered in-process", entry.get("version") or "")
        try:
            definition = components.load(name)
        except components.ComponentLoadError as exc:
            groups.setdefault("unavailable", []).append("  %-15s%s" % (name, exc))
            continue
        groups.setdefault(definition.family, []).append("  %-15s%-23s%-34s[%s]" % (
            name, definition.title, package.strip(), ", ".join(engine_reach(definition))))
    lines = []
    for family in sorted(groups):
        if lines:
            lines.append("")
        lines.append("%s:" % family)
        lines.extend(sorted(groups[family]))
    return lines


def run_connectors_command(args):
    """`streamwright connectors`: bare = installed list; ls/i/find use the catalog (the hub)."""
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

    if norm_action in ("find", "search", "s", "-s", "-f", "--find", "--search"):
        text = " ".join(query)
        hits = catalog.search(text, args.hub_url)
        if not hits:
            print("no connectors match %r" % text)
            return 0
        for key, entry in sorted(hits.items()):
            print("  %-16s %-10s %s" % (key, entry.get("trust", "community"), entry.get("summary", "")))
        return 0

    # If action was not a reserved verb, treat it as a direct search query (e.g. `sw connectors ads`)
    text = " ".join([action, *query])
    hits = catalog.search(text, args.hub_url)
    if not hits:
        print("no connectors match %r" % text)
        return 0
    for key, entry in sorted(hits.items()):
        print("  %-16s %-10s %s" % (key, entry.get("trust", "community"), entry.get("summary", "")))
    return 0
