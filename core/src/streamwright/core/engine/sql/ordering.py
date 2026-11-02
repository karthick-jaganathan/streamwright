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

"""The order a source runs in: a run-mode stream's requests and steps, `from_stream` parents and streams."""


from streamwright.core.engine.sql.stream_plan import exports_of, mapping_entries, partition_fields
from streamwright.core.graph import toposort


class RunOrdering:
    """SourcePlan's order checks: a run-mode stream's requests and steps, and `from_stream` parents."""

    def _run_order(self, plan):
        """
        The order a run-mode stream runs its requests and steps in: a request after the request or step its
        partitions come `from:`, a step after the requests and earlier steps it reads; requests first, then list
        order, break ties. A request partitioned from a step that needs the request first is a cycle (reported).
        """
        found = {}
        for kind, entries in (("request", plan.requests), ("step", plan.steps)):
            for position, item in entries:
                found.setdefault(item["name"].lower(), (kind, position, item))
        nodes = [node for node, (_, _, item) in found.items()]  # requests first, then steps, in list order
        positions = {step["name"].lower(): at for at, (_, step) in enumerate(plan.steps)}
        needs = {}
        for node in nodes:
            kind, _, item = found[node]
            if kind == "request":
                needs[node] = []
                for _, part in mapping_entries(item.get("partitions")):
                    source = part["from"].lower() if isinstance(part.get("from"), str) else None
                    if source in found and source != node and source not in needs[node]:
                        needs[node].append(source)
            else:
                needs[node] = [read for read in sorted(plan.reads.get(item["name"], ())) if read in found and
                               read != node and (found[read][0] == "request" or
                                                 positions.get(read, -1) < positions[node])]
        order, cycles = toposort(nodes, needs)
        for cycle in cycles:
            ring = list(cycle[:-1])
            if not any(found[node][0] == "request" for node in ring):
                continue
            first = next(at for at, node in enumerate(ring) if found[node][0] == "request")
            ring = ring[first:] + ring[:first]
            _, position, item = found[ring[0]]
            source = ring[1] if len(ring) > 1 else ring[0]
            part = next((k for k, part in mapping_entries(item.get("partitions"))
                         if isinstance(part.get("from"), str) and part["from"].lower() == source), 0)
            self._problem(("streams", plan.index, "requests", position, "partitions", part, "from"),
                          "requests and steps in a cycle: %s (a request partitioned `from:` a step runs after it, and "
                          "a step after the requests it reads)" % " -> ".join(
                              found[node][2]["name"] for node in ring + ring[:1]))
        return [(found[node][0], found[node][2]) for node in order]

    def _parents(self):
        """`from_stream` parents: another stream of the source with exactly one export, whose columns the fields are."""
        for plan in self.streams.values():
            for k, item in mapping_entries(plan.stream.get("partitions")):
                parent = item.get("from_stream")
                if not isinstance(parent, str):
                    continue
                path = ("streams", plan.index, "partitions", k)
                name = self.names.get(parent.lower())  # names ignore case, as `streamwright validate` compares them
                if name is None:
                    self._problem((*path, "from_stream"), "%r is not a stream of this source" % parent)
                    continue
                if name == plan.name:
                    self._problem((*path, "from_stream"), "a stream cannot take its partitions from itself")
                    continue
                plan.parents.append((k, item, name))
                exports = exports_of(self.streams[name].stream)
                if len(exports) != 1:
                    self._problem((*path, "from_stream"), "`from_stream` takes values from the export of stream %r, "
                                                           "which has %d exports: it needs exactly one" % (
                                                               name, len(exports)))
                    continue
                columns = self.streams[name].export_columns(exports[0])
                if columns is None:
                    continue  # its step has problems, reported where they are
                names = [column for column, _ in columns]
                several = isinstance(item.get("fields"), list)
                for f, field in enumerate(partition_fields(item)[0]):
                    if field not in names:
                        self._problem(path + (("fields", f) if several else ("field",)),
                                      "%r is not a column of export %r of stream %r (columns: %s)" % (
                                          field, exports[0], name, ", ".join(names)))

    def _order(self):
        """The stream names, each after its `from_stream` parents (ties keep the source order); cycles reported."""
        dependencies = {name: [parent for _, _, parent in plan.parents]
                        for name, plan in self.streams.items()}
        ordered, cycles = toposort(self.streams, dependencies)
        for cycle in cycles:
            plan = self.streams[cycle[-2]]
            index = next(k for k, _, parent in plan.parents if parent == cycle[-1])
            self._problem(("streams", plan.index, "partitions", index, "from_stream"),
                          "streams in a cycle: %s (a stream runs after its `from_stream` parents)" %
                          " -> ".join(cycle))
        # Keep cyclic streams selectable so closure() can surface their diagnostics before any request.
        return ordered + [name for name in self.streams if name not in ordered]
