from streamwright.core.graph import toposort


def test_stable_order_with_unknown_dependencies():
    assert toposort(["later", "independent", "first"], {"later": ["first", "missing"]}) == (
        ["independent", "first", "later"], [])


def test_cycles_exclude_blocked_nodes():
    assert toposort(["blocked", "a", "b", "ready"], {
        "blocked": ["a"], "a": ["b"], "b": ["a"],
    }) == (["ready"], [("a", "b", "a")])
