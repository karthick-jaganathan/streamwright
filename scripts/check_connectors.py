#!/usr/bin/env python3
"""Packaging check of every connector package under connectors/: its pyproject skeleton, LICENSE, README, Makefile and
entry points, and the contract v1.3 §6b certification (no hardcoded vendor catalogs). Run from any working directory;
stdlib only (tomllib)."""

import ast
import importlib
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONNECTORS = ROOT / "connectors"
ALLOWLIST = ROOT / "scripts/vendor_catalog_allowlist.txt"
PYTHONS = ("3.11", "3.12", "3.13")
VENDOR_CATALOG_LITERALS = 5  # a module-level mapping/tuple/frozenset of this many string literals is a vendor catalog


def defines_connector(path):
    """Whether a module assigns the module-level name CONNECTOR (the connector definition of spec.py)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return any(isinstance(node, ast.Assign) and any(getattr(target, "id", None) == "CONNECTOR"
                                                    for target in node.targets) for node in tree.body)


def spec_targets(spec, name):
    """The `"module:Class"` targets of the package's own modules in its spec (adapters, query builders, validator)."""
    pattern = re.compile(r"[\"']streamwright\.connectors\.%s\.([\w.]+):(\w+)[\"']" % re.escape(name))
    return pattern.findall(spec.read_text(encoding="utf-8"))


def check_targets(source, spec, name, problems):
    """Every target of the spec is a module of the package; an `engines` package only when an adapter lives there."""
    modules = {module for module, _ in spec_targets(spec, name)}
    for module in sorted(modules):
        if not (source / (module.replace(".", "/") + ".py")).exists():
            problems.append("spec.py names streamwright.connectors.%s.%s, which does not exist" % (name, module))
    engines = any(module.startswith("engines.") for module in modules)
    if engines and not (source / "engines/__init__.py").exists():
        problems.append("missing engines/__init__.py")
    if not engines and (source / "engines").exists():
        problems.append("engines/ holds no adapter of the spec (an HTTP connector's adapter is core's io.http)")
    return engines


def check_definition_package(package, name, project, data, problems):
    """One entry point to `<package>:CONNECTOR`, spec.py, client.py, guide.md, and the modules the spec names."""
    source = package / "src/streamwright/connectors" / name
    groups = sorted(project.get("entry-points", {}))
    if groups != ["streamwright.connectors"]:
        problems.append("a connector has exactly one entry point group, streamwright.connectors (has %s)" %
                        ", ".join(groups))
    for required in ("spec.py", "client.py", "guide.md"):
        if not (source / required).exists():
            problems.append("missing %s" % required)
    for legacy in ("connector.py", "reader.py", "duckdb_adapter.py", "spark_adapter.py", "spark.py", "_spark.py"):
        if (source / legacy).exists():
            problems.append("%s is replaced by spec.py, client.py and engines/ (connector contract v1)" % legacy)
    engines = False
    if (source / "spec.py").exists():
        if not defines_connector(source / "spec.py"):
            problems.append("spec.py must define CONNECTOR")
        engines = check_targets(source, source / "spec.py", name, problems)
    setuptools = data.get("tool", {}).get("setuptools", {})
    packages = ["streamwright.connectors.%s" % name]
    if engines:
        packages.append("streamwright.connectors.%s.engines" % name)
    if setuptools.get("packages") != packages:
        problems.append("tool.setuptools.packages must be %s" % ", ".join(packages))
    if "guide.md" not in setuptools.get("package-data", {}).get("streamwright.connectors.%s" % name, ()):
        problems.append("guide.md must be package data")
    if "spark" in project.get("optional-dependencies", {}) and not (source / "engines/spark.py").exists():
        problems.append("the `spark` extra needs engines/spark.py")


_CONTAINER_CALLS = ("MappingProxyType", "frozenset", "tuple", "set", "dict", "list")


def _string_literals(node):
    """
    The string literals that are direct elements of a container (a mapping's keys and values, a sequence's items),
    recursing through nested containers but never into a domain object (Field, Operation, re.compile, ...): those
    hold the connector's own vocabulary, not a vendor catalog.
    """
    if isinstance(node, ast.Call):
        name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
        return [text for arg in node.args for text in _string_literals(arg)] if name in _CONTAINER_CALLS else []
    if isinstance(node, ast.Dict):
        children = [child for child in node.keys + node.values if child is not None]
    elif isinstance(node, (ast.Tuple, ast.Set, ast.List)):
        children = list(node.elts)
    elif isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    else:
        return []
    return [text for child in children for text in _string_literals(child)]


def _is_container(value):
    """Whether a module-level value is a mapping/tuple/frozenset/list literal (or one wrapped by a builtin call)."""
    if isinstance(value, (ast.Dict, ast.Tuple, ast.Set, ast.List)):
        return True
    if isinstance(value, ast.Call):
        name = getattr(value.func, "id", None) or getattr(value.func, "attr", None)
        return name in _CONTAINER_CALLS
    return False


def load_allowlist():
    """`path:NAME` entries allowed to hold a vendor vocabulary (each line carries a justification after the key)."""
    if not ALLOWLIST.exists():
        return set()
    allowed = set()
    for line in ALLOWLIST.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if text and not text.startswith("#"):
            allowed.add(text.split()[0])
    return allowed


def check_vendor_catalogs(package, name, allowlist, problems):
    """
    No module of a connector package holds a module-level mapping/tuple/frozenset of >= VENDOR_CATALOG_LITERALS string
    literals unless it is allowlisted (contract v1.3 §6b: the vendor's surface is read from the installed library,
    never hardcoded). The allowlist keeps the connector's own vocabulary (error-retry policy, declared field names).
    """
    source = package / "src/streamwright/connectors" / name
    for path in sorted(source.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            targets = node.targets if isinstance(node, ast.Assign) else (
                [node.target] if isinstance(node, ast.AnnAssign) and node.value is not None else [])
            if not (targets and _is_container(node.value)):
                continue
            if getattr(targets[0], "id", "").startswith("__"):
                continue  # module dunders such as __all__ are not vendor data
            literals = _string_literals(node.value)
            key = "%s/%s:%s" % (name, path.relative_to(source).as_posix(), getattr(targets[0], "id", "?"))
            if len(literals) >= VENDOR_CATALOG_LITERALS and key not in allowlist:
                problems.append("%s is a module-level vendor catalog (%d string literals); resolve it against the "
                                "installed library or add it to %s with a justification (§6b)" % (
                                    key, len(literals), ALLOWLIST.relative_to(ROOT)))


def check_operation_families(name, problems):
    """
    No spec declares more than 3 concrete operations on one service with identical arguments (an operation family
    would do; contract v1.3 §6b). Imports the spec only (pure data, no vendor libraries); skipped if it cannot import.
    """
    try:
        connector = importlib.import_module("streamwright.connectors.%s.spec" % name).CONNECTOR
    except Exception:  # the import-time health of a spec is checked by the test suite, not here
        return
    groups = {}
    for operation in connector.operations:
        if operation.is_pattern:
            continue
        signature = (operation.service, tuple(sorted(field.name for field in operation.arguments)))
        groups.setdefault(signature, []).append(operation.method)
    for (service, _signature), methods in sorted(groups.items()):
        if len(methods) > 3:
            problems.append("service %s declares %d concrete operations with identical arguments (%s); declare an "
                            "operation family instead (§6b)" % (service, len(methods), ", ".join(sorted(methods))))


def check_package(package, allowlist):
    name, problems = package.name, []
    for required in ("README.md", "LICENSE", "Makefile", "pyproject.toml"):
        if not (package / required).exists():
            problems.append("missing %s" % required)
    if (package / "LICENSE").exists() and (package / "LICENSE").read_text() != (ROOT / "LICENSE").read_text():
        problems.append("LICENSE differs from the repository's")
    if (package / "setup.py").exists():
        problems.append("setup.py is a no-op: configuration lives in pyproject.toml")
    makefile = package / "Makefile"
    if makefile.exists() and "include ../../connector.mk" not in makefile.read_text():
        problems.append("Makefile must include ../../connector.mk")
    check_vendor_catalogs(package, name, allowlist, problems)
    check_operation_families(name, problems)
    if not (package / "pyproject.toml").exists():
        return problems
    data = tomllib.loads((package / "pyproject.toml").read_text(encoding="utf-8"))
    check_project(name, data, problems)
    check_definition_package(package, name, data.get("project", {}), data, problems)
    return problems


def check_project(name, data, problems):
    """The pyproject's metadata, dependency pin and single `<name>:CONNECTOR` entry point."""
    project = data.get("project", {})
    expected = "streamwright-" + name.replace("_", "-")
    if project.get("name") != expected:
        problems.append("project name must be %s (is %r)" % (expected, project.get("name")))
    if data.get("build-system", {}).get("build-backend") != "setuptools.build_meta":
        problems.append("build-backend must be setuptools.build_meta")
    if project.get("license") != "Apache-2.0":
        problems.append("license must be Apache-2.0")
    for key in ("version", "description", "readme", "requires-python", "authors", "urls"):
        if not project.get(key):
            problems.append("project.%s is missing" % key)
    classifiers = project.get("classifiers", [])
    for python in PYTHONS:
        if "Programming Language :: Python :: %s" % python not in classifiers:
            problems.append("classifier for Python %s is missing" % python)
    if not any(str(dep).replace(" ", "").startswith("streamwright~=") for dep in project.get("dependencies", [])):
        problems.append("dependencies must pin streamwright~=<minor>")
    target = project.get("entry-points", {}).get("streamwright.connectors", {}).get(name)
    if target != "streamwright.connectors.%s:CONNECTOR" % name:
        problems.append("the entry point must be `%s = \"streamwright.connectors.%s:CONNECTOR\"` in "
                        "streamwright.connectors (connector contract v1)" % (name, name))


def main():
    failures = 0
    allowlist = load_allowlist()
    packages = sorted(path.parent for path in CONNECTORS.glob("*/*/pyproject.toml"))
    for package in packages:
        for problem in check_package(package, allowlist):
            print("%s: %s" % (package.relative_to(ROOT), problem))
            failures += 1
    print("connector packaging: %d package(s), %d problem(s)" % (len(packages), failures))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
