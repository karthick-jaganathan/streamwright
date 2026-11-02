# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the deltalake connector knows about Delta Lake tables and the storage they live in, shared by every engine
adapter: the roots every table path must be inside (local folders and object storage URL prefixes; `..` refused), the
object storage credentials as DuckDB secret parameters and delta-rs storage options, the query policy of the source
engine's `table.query` (delta_scan only on a literal path inside the roots) and the rules a resolved connection must
meet. No engine library is imported here.
"""

import functools
import os
import posixpath
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final

from streamwright.core.io.queries import QueryPolicy, select_query
from streamwright.core.sdk import quote_literal

__all__ = ["AZURE_PARAMETERS", "S3_PARAMETERS", "VIEW", "StorageSecret", "gcs_settings", "masked_values",
           "normalize_roots", "query_policy", "resolve_path", "scan_query", "storage_options", "storage_secrets",
           "validate_connection"]

VIEW: Final = "delta_table"  # the view of a `table.query` call's `path`
S3_PARAMETERS: Final = MappingProxyType({
    "aws_access_key_id": "KEY_ID", "aws_secret_access_key": "SECRET", "aws_session_token": "SESSION_TOKEN",
    "aws_region": "REGION", "s3_endpoint": "ENDPOINT", "s3_url_style": "URL_STYLE", "s3_use_ssl": "USE_SSL"})
AZURE_PARAMETERS: Final = MappingProxyType({
    "azure_connection_string": "CONNECTION_STRING", "azure_account_name": "ACCOUNT_NAME",
    "azure_account_key": "ACCOUNT_KEY"})
GCS_SETTINGS: Final = ("gcs_key_file", "gcs_project_id")
S3_SCHEMES: Final = ("s3://",)
AZURE_SCHEMES: Final = ("abfss://", "azure://", "wasb://")
DELTA_RS_OPTIONS: Final = (
    ("aws_access_key_id", "AWS_ACCESS_KEY_ID"), ("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY"),
    ("aws_session_token", "AWS_SESSION_TOKEN"), ("aws_region", "AWS_REGION"),
    ("gcs_key_file", "GOOGLE_SERVICE_ACCOUNT"),
    ("azure_account_name", "AZURE_STORAGE_ACCOUNT_NAME"), ("azure_account_key", "AZURE_STORAGE_ACCOUNT_KEY"))


@dataclass(frozen=True)
class StorageSecret:
    """One temporary DuckDB secret: its type and extension, (PARAMETER, value) pairs, the roots it is scoped to."""
    secret_type: str
    extension: str
    parameters: tuple[tuple[str, Any], ...]
    scopes: tuple[str, ...]


def _normalized(path: str) -> str:
    if "://" in path:
        scheme, _, rest = path.partition("://")
        return "%s://%s" % (scheme.lower(), posixpath.normpath(rest))
    return os.path.abspath(os.path.expanduser(path))


def normalize_roots(roots: Iterable[str], base: str | None = None) -> tuple[str, ...]:
    """The roots: local folders absolute (relative ones below `base`, default the working folder), URLs normalized."""
    return tuple(_normalized(root if "://" in root or base is None or os.path.isabs(root)
                             else os.path.join(base, root)) for root in roots)


def _inside(path: str, roots: Sequence[str]) -> bool:
    separator = "/" if "://" in path else os.sep
    return any(path == root or path.startswith(root.rstrip(separator) + separator) for root in roots)


def resolve_path(path: str, roots: Sequence[str]) -> str:
    """A table path (a URL, an absolute path or one relative to the first root) inside `roots`; ValueError if not."""
    if not isinstance(path, str) or not path.strip() or "\x00" in path:
        raise ValueError("the table path is empty")
    if ".." in path.replace("\\", "/").split("/"):
        raise ValueError("path %r goes up a folder (..)" % path)
    location = path if "://" in path or os.path.isabs(path) else roots[0].rstrip("/") + "/" + path
    location = _normalized(location)
    if not _inside(location, roots):
        raise ValueError("path %r is outside the roots: %s" % (path, ", ".join(roots)))
    return location


def _delta_scan_problem(roots: tuple[str, ...], name: str, call: Mapping[str, Any]) -> str | None:
    """delta_scan only on a literal path inside the roots (no parameter or expression)."""
    if name != "delta_scan":
        return None
    arguments = [item for item in (call.get("arguments") or []) if item.get("name") in ("", "path")]
    literal = ((arguments[0].get("expression") or {}).get("literal") or {}) if arguments else {}
    if literal.get("kind") != "STRING":
        return "the query reads delta_scan only on a literal path inside the roots (no parameter or expression)"
    if not _inside(_normalized(str(literal.get("text"))), roots):
        return "the query reads path %r which is outside the roots (%s)" % (literal.get("text"), ", ".join(roots))
    return None


def query_policy(roots: tuple[str, ...]) -> QueryPolicy:
    """What a `table.query` may use: delta_scan inside the roots, the call's `delta_table` view, no other catalog."""
    return QueryPolicy(table_functions=("delta_scan", "unnest", "range", "generate_series"),
                       denied_prefixes=tuple(prefix for prefix in QueryPolicy.denied_prefixes if prefix != "delta_"),
                       catalog=None, views=(VIEW,), check_table_function=functools.partial(_delta_scan_problem, roots))


def scan_query(path: str, columns: Sequence[str] | None = None, where: Sequence[Mapping[str, Any]] | None = None,
               limit: int | None = None) -> tuple[str, dict[str, Any]]:
    """(query, parameters) of a `table.read`: the columns FROM delta_scan(path) WHERE every condition holds, LIMIT."""
    return select_query("delta_scan(%s)" % quote_literal(path), columns, where, limit)


def _given(connection: Mapping[str, Any], keys: Iterable[str]) -> bool:
    return any(connection.get(key) not in (None, "") for key in keys)


def storage_secrets(connection: Mapping[str, Any], roots: Sequence[str]) -> list[StorageSecret]:
    """The DuckDB secrets of the connection's object storage keys, each scoped to the roots on its schemes."""
    secrets = []
    for secret_type, extension, keys, schemes in (("s3", "httpfs", S3_PARAMETERS, S3_SCHEMES),
                                                  ("azure", "azure", AZURE_PARAMETERS, AZURE_SCHEMES)):
        if _given(connection, keys):
            parameters = tuple((parameter, connection[key]) for key, parameter in keys.items()
                               if connection.get(key) not in (None, ""))
            secrets.append(StorageSecret(secret_type, extension, parameters,
                                         tuple(root for root in roots if root.startswith(schemes))))
    return secrets


def gcs_settings(connection: Mapping[str, Any]) -> list[tuple[str, str]]:
    """The httpfs settings of Google Cloud Storage: a service account key file and a project."""
    return [(key, str(connection[key])) for key in GCS_SETTINGS if connection.get(key) not in (None, "")]


def masked_values(connection: Mapping[str, Any], secret_values: Iterable[Any]) -> list[str]:
    """The credentials to mask: the secret fields' values and the account key inside an Azure connection string."""
    return [*map(str, secret_values), *re.findall(r"AccountKey=([^;]+)", str(
        connection.get("azure_connection_string") or ""))]


def storage_options(connection: Mapping[str, Any]) -> dict[str, str]:
    """delta-rs storage options of a connection (credentials included: never logged); plain HTTP only if asked."""
    options = {option: str(connection[key]) for key, option in DELTA_RS_OPTIONS if connection.get(key)}
    if connection.get("s3_endpoint"):
        plain = connection.get("s3_use_ssl") is False
        options["AWS_ENDPOINT_URL"] = ("http://" if plain else "https://") + str(connection["s3_endpoint"])
        if plain:
            options["AWS_ALLOW_HTTP"] = "true"
    if connection.get("s3_url_style") == "path":
        options["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "false"
    options.update(dict(getattr(connection, "vendor", {}) or {}))  # undeclared delta-rs storage options (vendor)
    return options


def validate_connection(connection: Mapping[str, Any]) -> list[str]:
    """`roots`: a non-empty list of folders and URL prefixes (no `..`, no empty text): the sandbox is fail-closed."""
    roots = connection.get("roots") or []
    if not roots:
        return ["`roots` must name at least one folder or URL prefix"]
    problems = []
    for index, root in enumerate(roots):
        if not str(root).strip() or "\x00" in str(root):
            problems.append("roots[%d]: must be a folder or URL prefix (a non-empty text)" % index)
        elif ".." in str(root).replace("\\", "/").split("/"):
            problems.append("roots[%d]: %r goes up a folder (..)" % (index, root))
    return problems
