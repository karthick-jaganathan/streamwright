"""Metadata-first discovery and guarded loading of StreamWright plug-ins."""

import importlib.metadata
import re

from streamwright.core.runtime.constants import SDK_VERSION

CONNECTORS = "streamwright.connectors"
SPARK_LAUNCHERS = "streamwright.spark.launchers"
COMMANDS = "streamwright.commands"
JOB_KINDS = "streamwright.job_kinds"

# Replace this callable in embedding applications and tests; no plug-in is imported to inspect metadata.
entry_point_source = importlib.metadata.entry_points


class PluginError(Exception):
    """A missing, disallowed, incompatible or broken plug-in."""


def entries(group, source=None):
    """The installed entry points of a group (from `source`, default `entry_point_source`); nothing is imported."""
    found = (source or entry_point_source)()
    return list(found.select(group=group)) if hasattr(found, "select") else list(found.get(group, ()))


def discover(group, *, entry_points=None):
    """Return name, distribution and version from installed entry-point metadata without loading code."""
    return [{"name": ep.name, "distribution": getattr(getattr(ep, "dist", None), "name", None),
             "version": getattr(getattr(ep, "dist", None), "version", None)}
            for ep in entries(group, entry_points)]


def _version_parts(version):
    match = re.match(r"^(\d+(?:\.\d+)*)", str(version))
    return tuple(map(int, match.group(1).split("."))) if match else ()


def _admits(requirement, version):
    """Check the common PEP 440 numeric requirement operators without an undeclared runtime dependency."""
    clauses = re.findall(r"(~=|==|!=|>=|<=|>|<)\s*(\d+(?:\.\d+)*)", requirement)
    if not clauses:
        return False
    current = _version_parts(version)
    for operator, bound in clauses:
        target = _version_parts(bound)
        size = max(len(current), len(target))
        value = current + (0,) * (size - len(current))
        limit = target + (0,) * (size - len(target))
        if operator == "~=":
            upper_prefix = target[:-1] if len(target) > 1 else target
            upper = (*upper_prefix[:-1], upper_prefix[-1] + 1)
            upper += (0,) * (size - len(upper))
            valid = value >= limit and value < upper
        else:
            valid = {"==": value == limit, "!=": value != limit, ">=": value >= limit,
                     "<=": value <= limit, ">": value > limit, "<": value < limit}[operator]
        if not valid:
            return False
    return True


def check_compatibility(entry):
    dist = getattr(entry, "dist", None)
    if dist is None:  # in-process entry points used by embedders have no distribution
        return
    declared = (getattr(dist, "metadata", {}) or {}).get("StreamWright-SDK-Version")
    if declared is not None:
        if _version_parts(declared)[:1] != _version_parts(SDK_VERSION)[:1]:
            raise PluginError("%s requires StreamWright SDK %s; running SDK %s" %
                              (entry.name, declared, SDK_VERSION))
        return
    if str(getattr(dist, "name", "")).lower().replace("_", "-") == "streamwright":
        return
    requirements = [value for value in (getattr(dist, "requires", None) or ())
                    if re.match(r"^\s*streamwright\s*([<>=!~(;\[]|$)", value, re.I)]
    if not requirements:
        raise PluginError("%s (%s) does not declare a StreamWright SDK version or core dependency" %
                          (entry.name, dist.name))
    version = importlib.metadata.version("streamwright")
    if not any(_admits(value.split(";", 1)[0], version) for value in requirements):
        raise PluginError("%s (%s) requires %s; running streamwright %s (SDK %s)" %
                          (entry.name, dist.name, ", ".join(requirements), version, SDK_VERSION))


def load_plugin(group, name, *, allowed=None, expected_type=None, entry_points=None):
    """Authorize from metadata before import, check compatibility and validate the loaded type."""
    matches = [entry for entry in entries(group, entry_points) if entry.name == name]
    hint = getattr(getattr(matches[0], "dist", None), "name", None) if matches else None
    hint = hint or "streamwright-" + name.replace("_", "-")
    if allowed is not None and name not in allowed:
        raise PluginError("%s %r is not in the allowed list (%s); install with: pip install %s" %
                          (group, name, ", ".join(sorted(allowed)) or "empty", hint))
    if not matches:
        raise PluginError("%s %r is not installed; install with: pip install %s" % (group, name, hint))
    if len(matches) != 1:
        raise PluginError("%s %r has multiple installed entry points" % (group, name))
    entry = matches[0]
    check_compatibility(entry)
    try:
        loaded = entry.load()
        component = loaded() if isinstance(loaded, type) and expected_type is not None else loaded
    except Exception as exc:
        raise PluginError("%s %r could not be loaded (pip install %s): %s" %
                          (group, name, hint, type(exc).__name__)) from None
    if expected_type is not None and not isinstance(component, expected_type):
        raise PluginError("%s %r (pip install %s) has an invalid type" % (group, name, hint))
    return component
