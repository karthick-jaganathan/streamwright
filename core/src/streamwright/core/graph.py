"""Stable dependency ordering shared by source and job planners."""

import heapq


def toposort(nodes, dependencies):
    """Return ``(order, cycles)`` with ties resolved in input order.

    Dependencies maps each node to its prerequisites. Unknown prerequisites are
    ignored; cycles are closed paths (first node repeated last). Nodes blocked
    by a cycle are omitted from the order, but are not themselves a cycle.
    """
    nodes = list(dict.fromkeys(nodes))
    positions = {node: index for index, node in enumerate(nodes)}
    needs = {
        node: {dependency for dependency in dependencies.get(node, ()) if dependency in positions}
        for node in nodes
    }
    followers = {node: [] for node in nodes}
    for node, required in needs.items():
        for prerequisite in required:
            followers[prerequisite].append(node)
    remaining = {node: len(required) for node, required in needs.items()}
    ready = [positions[node] for node in nodes if remaining[node] == 0]
    heapq.heapify(ready)
    order = []
    while ready:
        node = nodes[heapq.heappop(ready)]
        order.append(node)
        for follower in followers[node]:
            remaining[follower] -= 1
            if remaining[follower] == 0:
                heapq.heappush(ready, positions[follower])

    pending = {node for node in nodes if remaining[node]}
    visited, stack, active, cycles, seen = set(), [], {}, [], set()

    def visit(node):
        visited.add(node)
        active[node] = len(stack)
        stack.append(node)
        for prerequisite in sorted(needs[node] & pending, key=positions.__getitem__):
            if prerequisite in active:
                ring = stack[active[prerequisite]:]
                first = min(range(len(ring)), key=lambda index: positions[ring[index]])
                ring = ring[first:] + ring[:first]
                key = tuple(ring)
                if key not in seen:
                    seen.add(key)
                    cycles.append(tuple(ring + ring[:1]))
            elif prerequisite not in visited:
                visit(prerequisite)
        stack.pop()
        del active[node]

    for node in nodes:
        if node in pending and node not in visited:
            visit(node)
    cycles.sort(key=lambda cycle: tuple(positions[node] for node in cycle))
    return order, cycles
