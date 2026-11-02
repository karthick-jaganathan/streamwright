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
The dependency graph of a job: steps and outputs, the order steps run in, and which outputs each step feeds.

Dependencies are found by a pluggable `reads_of(sql)`: by default DuckDB's own parser (`json_serialize_sql`) lists
the tables a query reads, so a column or alias named like a step is not a dependency and a quoted step name is one.
`{{ step }}` / `{{ step.column }}` references anywhere else in an item are dependencies too. Ordering is
`streamwright.core.graph.toposort` (stable, input order on ties); a cycle is a PlanError.
"""

from streamwright.core.graph import toposort
from streamwright.core.jobs.refs import STEP_REF, references, walk_strings
from streamwright.core.sql.analysis import SqlAnalysisError as PlanError
from streamwright.core.sql.analysis import sql_table_reads

__all__ = ["Node", "Plan", "PlanError", "build_plan", "item_dependencies", "sql_table_reads"]


class Node:
    def __init__(self, name, kind, item):
        self.name, self.kind, self.item = name, kind, item       # kind: step | output
        self.uses = []                                            # step names this node reads
        self.used_by = []                                         # nodes that read this step
        self.outputs = set()                                      # outputs this step feeds

    def __repr__(self):
        return "<Node %s (%s)>" % (self.name, self.kind)


class Plan:
    def __init__(self, nodes, order):
        self.nodes, self.order = nodes, order                     # order: step names in run order
        self.by_name = {n.name: n for n in nodes}

    @property
    def outputs(self):
        return [n for n in self.nodes if n.kind == "output"]

    def describe(self):
        lines = ["%-24s %-7s %-32s %s" % ("Name", "Type", "Reads", "Feeds outputs"), "-" * 90]
        for name in self.order + [n.name for n in self.outputs]:
            node = self.by_name[name]
            lines.append("%-24s %-7s %-32s %s" % (node.name, node.kind, ", ".join(node.uses) or "-",
                                                  ", ".join(sorted(node.outputs)) or "-"))
        return "\n".join(lines)


def item_dependencies(item, reads_of=sql_table_reads):
    """What a step or output reads: its `sql` (by `reads_of`), its `step`, and `{{ step[.column] }}` elsewhere."""
    found = []
    if isinstance(item.get("sql"), str):
        found.extend(sorted(reads_of(item["sql"])))
    if isinstance(item.get("step"), str):
        found.append(item["step"])
    for key, value in item.items():
        if key == "sql":
            continue
        for text in walk_strings(value):
            for expr in references(text):
                hit = STEP_REF.match(expr)
                if hit:
                    found.append(hit.group(1))
    return found


def build_plan(job, dependencies_of=item_dependencies):
    """The Plan of a (well-formed) job; raises PlanError on a cycle among its steps."""
    steps = [s for s in job.get("steps") or () if isinstance(s, dict)]
    outputs = [o for o in job.get("outputs") or () if isinstance(o, dict)]
    nodes = [Node(s["name"], "step", s) for s in steps] + [Node(o["name"], "output", o) for o in outputs]
    step_names = {s["name"] for s in steps}
    by_name = {n.name: n for n in nodes}
    for node in nodes:
        for name in dependencies_of(node.item):
            dependency = name.lower()
            if dependency in step_names and dependency != node.name and dependency not in node.uses:
                node.uses.append(dependency)
                by_name[dependency].used_by.append(node.name)
    order, cycles = toposort([s["name"] for s in steps], {n.name: n.uses for n in nodes if n.kind == "step"})
    if cycles:
        raise PlanError("steps depend on each other in a cycle: %s" % "; ".join(" -> ".join(c) for c in cycles))
    for output in [n for n in nodes if n.kind == "output"]:
        pending, seen = list(output.uses), set()
        while pending:
            name = pending.pop()
            if name not in seen:
                seen.add(name)
                by_name[name].outputs.add(output.name)
                pending.extend(by_name[name].uses)
    return Plan(nodes, order)
