#!/usr/bin/env python3
"""
AST-only G1/G2 boundary check; run from any working directory, no third-party imports. G1: engines name no connector
or vendor argument. G2: connector modules import only streamwright.core.sdk and streamwright.core.io (and, in their
Spark adapter, streamwright.spark.sdk, .io and the legacy .adapters).
"""

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIBLINGS = (
    ROOT,
    ROOT.parent / "streamwright-spark" if (ROOT.parent / "streamwright-spark").exists()
    else ROOT / ".contract-repos/streamwright-spark",
    ROOT.parent / "streamwright-mcp" if (ROOT.parent / "streamwright-mcp").exists()
    else ROOT / ".contract-repos/streamwright-mcp",
)
CORE = ROOT / "core/src/streamwright/core"
CONNECTORS = ROOT / "connectors"
ALLOWLIST = ROOT / "scripts/plugin_contract_allowlist.txt"
ARGUMENT_KEYS = frozenset({
    "developer_token", "login_customer_id", "customer_id", "ad_account_id", "business_account_id",
    "spreadsheet_id", "sheet_id", "bucket_name", "aws_access_key_id", "aws_secret_access_key",
    "azure_connection_string",
})
GENERIC_AUTH_KEYS = frozenset({
    "token", "password", "username", "user", "host", "port", "database", "schema",
    "client_id", "client_secret", "refresh_token", "api_key", "url", "base_url",
    "region", "account", "headers", "path", "endpoint", "timeout", "type",
    "local", "credentials", "environment", "dsn", "uri", "tenant", "app_id",
    "app_secret", "account_id", "azure", "gcs", "s3", "sslmode", "options", "session_token",
})
# connector names that are also ordinary words: flagged only where a literal identifies a provider
GENERIC_CONNECTOR_NAMES = frozenset({"files"})


def ignored_constants(tree):
    """Module/class/function docstrings describe examples, not executable engine coupling."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and \
                    isinstance(first.value.value, str):
                yield first.value


def allowlisted(path, token, allowed):
    return (path, token) in allowed


def parents_of(tree):
    return {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}


def names_provider(node, parents):
    """Whether a string literal is used as a provider identifier: compared, or the value of a `provider`."""
    parent = parents.get(node)
    while isinstance(parent, (ast.Tuple, ast.List, ast.Set)):
        node, parent = parent, parents.get(parent)
    if isinstance(parent, ast.Compare):
        return True
    if isinstance(parent, (ast.Assign, ast.AnnAssign)):
        targets = parent.targets if isinstance(parent, ast.Assign) else [parent.target]
        return any(getattr(target, "id", getattr(target, "attr", None)) == "provider" for target in targets)
    if isinstance(parent, ast.keyword):
        return parent.arg == "provider"
    if isinstance(parent, ast.Dict) and node in parent.values:
        key = parent.keys[parent.values.index(node)]
        return isinstance(key, ast.Constant) and key.value == "provider"
    return False


def mentions(token, node, parents):
    if token in GENERIC_CONNECTOR_NAMES:
        return node.value == token and names_provider(node, parents)
    return re.search(r"(?<![A-Za-z_0-9])" + re.escape(token) + r"(?![A-Za-z_0-9])", node.value, re.I) is not None


def vendor_argument_keys():
    """Derive connector-specific auth keys from declarations, not just a maintained list."""
    uses = {}
    for family in CONNECTORS.iterdir():
        if not family.is_dir():
            continue
        for package in family.iterdir():
            src = package / "src"
            if not src.exists():
                continue
            for file in src.rglob("connector.py"):
                tree = ast.parse(file.read_text(encoding="utf-8"), filename=str(file))
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Assign) or not any(
                            isinstance(target, ast.Name) and target.id in (
                                "auth_required", "auth_optional", "secret_keys", "request_headers")
                            for target in node.targets):
                        continue
                    try:
                        names = ast.literal_eval(node.value)
                    except (ValueError, TypeError, SyntaxError):
                        continue
                    if isinstance(names, (list, tuple, set)):
                        for name in names:
                            if isinstance(name, str) and name.isidentifier():
                                uses.setdefault(name, set()).add(package.name)
    return {name for name, packages in uses.items() if len(packages) == 1 and name not in GENERIC_AUTH_KEYS}


# what every connector module may import, and what its Spark adapter may import besides (docs/design/
# connector-contract.md section 6); `adapters` is legacy
CORE_PUBLIC_APIS = ("streamwright.core.sdk", "streamwright.core.io")
SPARK_PUBLIC_APIS = ("streamwright.spark.sdk", "streamwright.spark.io", "streamwright.spark.adapters")


def is_public(imported, apis):
    return any(imported == api or imported.startswith(api + ".") for api in apis)


def read_allow_list():
    allowed = set()
    for row in ALLOWLIST.read_text().splitlines():
        line = row.strip()
        if line and not line.startswith("#"):
            path, separator, token = line.partition(":")
            if not separator or not token:
                raise ValueError("invalid allow-list row: %s" % line)
            allowed.add((path, token))
    return allowed


def engine_files():
    """(repository, file) of every engine module: core outside its SDK, and the sibling engine packages."""
    for repository in SIBLINGS:
        for root in ([CORE] if repository == ROOT else [repository / "src"]):
            if not root.exists():
                continue
            for file in root.rglob("*.py"):
                if not (root == CORE and "sdk" in file.relative_to(CORE).parts):
                    yield repository, file


def engine_violations(tokens, allowed):
    """G1: an engine string literal that names a connector or a vendor argument."""
    problems = []
    for repository, file in engine_files():
        relative = file.relative_to(repository).as_posix()
        tree = ast.parse(file.read_text(encoding="utf-8"), filename=str(file))
        docs, parents = set(ignored_constants(tree)), parents_of(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str) or node in docs:
                continue
            problems += ["%s:%d: G1 engine literal contains connector/vendor token %r" % (relative, node.lineno, token)
                         for token in tokens if mentions(token, node, parents)
                         and not allowlisted(relative, token, allowed)]
    return problems


def imported_modules(node):
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if isinstance(node, ast.ImportFrom):
        return [node.module or ""] if node.level == 0 else []
    return []


def connector_violations():
    """G2: a connector module that imports engine code beyond the public APIs."""
    problems = []
    for file in CONNECTORS.glob("*/*/src/**/*.py"):
        relative = file.relative_to(ROOT).as_posix()
        adapter = file.name in {"spark_adapter.py", "_spark.py", "spark.py"}
        for node in ast.walk(ast.parse(file.read_text(encoding="utf-8"), filename=str(file))):
            for imported in imported_modules(node):
                permitted = is_public(imported, CORE_PUBLIC_APIS) or (adapter and
                                                                      is_public(imported, SPARK_PUBLIC_APIS))
                if imported.startswith(("streamwright.core.", "streamwright.spark.")) and not permitted:
                    problems.append("%s:%d: G2 connector imports private API %s" % (relative, node.lineno, imported))
    return problems


def violations():
    connector_names = {folder.name for family in CONNECTORS.iterdir() if family.is_dir()
                       for folder in family.iterdir() if (folder / "pyproject.toml").exists()}
    tokens = connector_names | ARGUMENT_KEYS | vendor_argument_keys()
    return sorted(set(engine_violations(tokens, read_allow_list()) + connector_violations()))


def main():
    problems = violations()
    for problem in problems[:150]:
        print(problem, file=sys.stderr)
    if len(problems) > 150:
        print("... and %d more violations" % (len(problems) - 150), file=sys.stderr)
    print("plugin contracts: %d violation(s) (G1 engine literals, G2 SDK imports)" % len(problems))
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
