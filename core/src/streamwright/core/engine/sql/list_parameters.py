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

"""List `$name` parameters in a parsed (serialized) query: compared as one value, or in the list of ANY or ALL."""





def _nodes(tree):
    """Every node (mapping) of a serialized query."""
    pending = [tree]
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            yield node
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)


def _is_list(node, types):
    """Whether an expression of a serialized query is a list for sure: a list parameter or literal, or a cast to one."""
    if not isinstance(node, dict):
        return False
    if node.get("class") == "PARAMETER":
        return (types.get(node.get("identifier")) or "").endswith("]")
    if node.get("class") == "CAST":
        return (node.get("cast_type") or {}).get("id") in ("LIST", "ARRAY")
    return node.get("type") == "ARRAY_CONSTRUCTOR" or str(node.get("function_name")).lower() == "list_value"


def compared_lists(tree, types):
    """
    The list parameters (`types`: name -> DuckDB type) that a serialized query compares as one value with something
    that is not a list, `x IN ($ids)` or `x = $ids`: DuckDB casts x to a list, which compiles against empty tables
    but fails on every row of a run.
    """
    found = set()
    for node in _nodes(tree):
        if node.get("class") == "COMPARISON":
            pairs = [(node.get("left"), node.get("right"))]
        elif node.get("class") == "OPERATOR" and node.get("type") in ("COMPARE_IN", "COMPARE_NOT_IN"):
            children = node.get("children") or [None]
            pairs = [(children[0], item) for item in children[1:]]
        else:
            continue
        for left, right in pairs:
            for operand, other in ((left, right), (right, left)):
                if isinstance(operand, dict) and operand.get("class") == "PARAMETER" and \
                        _is_list(operand, types) and not _is_list(other, types):
                    found.add(operand.get("identifier"))
    return sorted(found)


def quantified(tree):
    """Whether a `$name` parameter is in the list of an ANY or ALL comparison, `x = ANY($ids)`, of a parsed query."""
    for node in _nodes(tree):
        if node.get("class") != "SUBQUERY" or node.get("subquery_type") != "ANY":
            continue
        select = (node.get("subquery") or {}).get("node") or {}
        items = select.get("select_list") or []
        # DuckDB reads `x = ANY(list)` as `x = ANY(SELECT unnest(list))`, with no FROM
        if (select.get("from_table") or {}).get("type") == "EMPTY" and len(items) == 1 and \
                str(items[0].get("function_name")).lower() == "unnest" and \
                any(item.get("class") == "PARAMETER" for item in _nodes(items[0])):
            return True
    return False
