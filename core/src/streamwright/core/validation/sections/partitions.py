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

"""Stream-level `partitions`: names, parents and partition sources."""


from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.checker import describe, suggest
from streamwright.core.validation.sections.stream_context import lower_name
from streamwright.core.validation.sections.tables import TableSection


class PartitionSection(TableSection):
    """Stream partitions and parent dependencies."""

    def check_partitions(self, partitions, path, stream, info):
        if not self._expect(isinstance(partitions, list) and len(partitions) > 0, "a non-empty list of partitions",
                            partitions, path, container=stream, key="partitions"):
            return
        for i, item in enumerate(partitions):
            ppath = (*path, i)
            if not self._expect(isinstance(item, dict), "a mapping", item, ppath, container=partitions, key=i):
                continue
            for name, npath, container, key in self._partition_names(item, ppath, partitions, i, stream):
                if not isinstance(name, str):
                    continue
                if name in info.partitions:
                    self.error("duplicate-partition", npath, "partition %r is defined twice" % name, container, key)
                else:
                    info.partitions.append(name)

    def _partition_names(self, item, ppath, partitions, index, stream):
        """Checks one stream partition; returns (name, path, container, key) of the partitions it defines."""
        names = []
        batched = (spec.BATCH_SIZE,)  # reported on its own below, not as an unknown key
        if spec.BATCH_SIZE in item:
            self.error("bad-value", (*ppath, spec.BATCH_SIZE), "`batch_size` is not supported on %s: each "
                       "stream partition is one value, with its own state; batch a request partition of a run-mode "
                       "stream instead ({name, values, batch_size} or {name, from, field, batch_size})" % (
                           "a `from_stream` partition" if "from_stream" in item else "stream partitions"),
                       item, spec.BATCH_SIZE)
        if "from_stream" in item:
            names.extend(self._check_parent_partition(item, ppath, partitions, index, stream))
        else:
            self.check_mapping(item, spec.PARTITION_VALUES_KEYS + batched, ppath, container=partitions, key=index,
                               what="a partition", required=spec.PARTITION_VALUES_KEYS)
            values = item.get("values")
            if "values" in item and not (isinstance(values, list) or (isinstance(values, str) and "{{" in values)):
                self.error("bad-value", (*ppath, "values"), "expected a list or a reference such as "
                           "\"{{ config.ids }}\", got %s" % describe(values), item, "values")
            self.check_templates(values, (*ppath, "values"), container=item, key="values", scopes=self.scopes())
        if "name" in item and "fields" not in item:
            self.check_name(item["name"], (*ppath, "name"), item, "name")
            names.append((item["name"], (*ppath, "name"), item, "name"))
        return names

    def _check_parent_partition(self, item, ppath, partitions, index, stream):
        """A `from_stream` partition; returns the (name, path, container, key) of its `fields`."""
        names = []
        batched = (spec.BATCH_SIZE,)
        if "fields" in item:
            self.check_mapping(item, spec.PARTITION_RECORD_KEYS + batched, ppath, container=partitions, key=index,
                               what="a partition with `fields` (named after the fields)",
                               required=spec.PARTITION_RECORD_KEYS)
            fields = item["fields"]
            if self._expect(isinstance(fields, list) and len(fields) > 0, "a non-empty list of field names",
                            fields, (*ppath, "fields"), container=item, key="fields"):
                for j, field in enumerate(fields):
                    self.check_name(field, (*ppath, "fields", j), fields, j)
                    names.append((field, (*ppath, "fields", j), fields, j))
        else:
            self.check_mapping(item, spec.PARTITION_STREAM_KEYS + batched, ppath, container=partitions, key=index,
                               what="a stream partition", required=spec.PARTITION_STREAM_KEYS)
            self.check_strings(item, ("field",), ppath, "a name")
        self.check_strings(item, ("from_stream",), ppath, "a name")
        self.check_parent(item.get("from_stream"), (*ppath, "from_stream"), item, stream)
        return names

    def check_parent(self, parent, path, item, stream):
        """A `from_stream` parent: another stream of the source, with exactly one export (its rows are the values)."""
        if not isinstance(parent, str):
            return
        key = parent.lower()
        if key not in self.stream_names:
            owner = self.export_owners.get(key)
            if owner is not None:
                owner_name = self.streams[owner[1]].get("name")
                hint = "; %r is an export of stream %r: use `from_stream: %s`" % (parent, owner_name, owner_name)
            else:
                hint = suggest(parent, [self.streams[i]["name"] for i in self.stream_names.values()])
            self.error("unknown-stream", path, "unknown stream %r%s" % (parent, hint), item, "from_stream")
        elif key == lower_name(stream.get("name")):
            self.error("bad-value", path, "a stream cannot partition on itself", item, "from_stream")
        else:
            exports = self.streams[self.stream_names[key]].get("export")
            if isinstance(exports, dict) and len(exports) > 1:
                self.error("bad-value", path, "stream %r has %d exports (%s): `from_stream` reads a stream with "
                           "exactly one export" % (parent, len(exports), ", ".join(str(name) for name in exports)),
                           item, "from_stream")

    def check_partition_sources(self, checked):
        """`from_stream` parents run first, so they cannot form a cycle (names ignore case)."""
        streams, names = self.streams, self.stream_names
        parents = {}
        for i in checked:
            partitions = streams[i].get("partitions")
            if not (isinstance(partitions, list) and isinstance(streams[i].get("name"), str)):
                continue
            for item in partitions:
                parent = item.get("from_stream") if isinstance(item, dict) else None
                # the parent's export columns are checked when DuckDB compiles its steps; itself: reported already
                if isinstance(parent, str) and parent.lower() in names and parent.lower() != streams[i]["name"].lower():
                    parents.setdefault(streams[i]["name"].lower(), []).append(parent.lower())
        for start in sorted(parents):
            cycle = self._find_cycle(parents, start)
            if cycle:
                index = names[cycle[0]]
                self.error("partition-cycle", ("streams", index, "partitions"), "partitions form a cycle: %s" %
                           " -> ".join(streams[names[name]]["name"] for name in cycle), streams[index], "partitions")
                return

    @staticmethod
    def _find_cycle(parents, start):
        """Depth-first search for a path from `start` back to itself."""
        stack = [(start, [start])]
        while stack:
            node, trail = stack.pop()
            for parent in parents.get(node, ()):
                if parent == start:
                    return [*trail, start]
                if parent not in trail:
                    stack.append((parent, [*trail, parent]))
        return None
