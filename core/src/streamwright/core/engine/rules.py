"""Pure rules shared by source validation and execution."""

import re

_WINDOW_REFERENCE = re.compile(r"\{\{\s*window\.")


def uses_window(value):
    """Whether a nested request references a window in a template."""
    if isinstance(value, str):
        return _WINDOW_REFERENCE.search(value) is not None
    if isinstance(value, dict):
        return any(uses_window(item) for item in value.values())
    if isinstance(value, list):
        return any(uses_window(item) for item in value)
    return False


def valid_batch_size(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def transport_problem(kind, transport, provider):
    """Describe a request/connector transport mismatch, or return None."""
    if kind == "sdk" and transport == "http":
        return "%s is a REST API connector: use an http request (http: {path, method, ...}), not sdk" % provider
    if kind == "http" and transport == "sdk":
        return "http requests need a built-in `auth.type`; auth provider %r only authenticates sdk requests" % provider
    return None
