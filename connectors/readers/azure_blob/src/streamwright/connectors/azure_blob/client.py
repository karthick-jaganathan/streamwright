# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the azure_blob connector knows about Azure Blob Storage, shared by every engine adapter: the store's facts (URL
schemes, DuckDB's azure secret, its credential combinations), the connection string's parts, the endpoint and
credential rules a resolved connection must meet, and the Hadoop (ABFS) URL of a blob. No engine library is imported.
"""

import re
from collections.abc import Mapping
from typing import Any, Final
from urllib.parse import urlsplit

from streamwright.core.io.locations import ObjectStore, root_problems
from streamwright.core.sdk.contract import ResolvedConnection

__all__ = ["ABFS_PATTERN", "DEFAULT_ENDPOINT_SUFFIX", "SCHEMES", "SOURCE_REPLACEMENT", "STORE", "account_key",
           "account_name", "build_secret", "connection_parts", "connection_string", "endpoint_problem", "hadoop_url",
           "has_credentials", "secret_parts", "validate_connection"]

SCHEMES: Final = ("azure", "abfss")
DEFAULT_ENDPOINT_SUFFIX: Final = "core.windows.net"
_ENDPOINT: Final = re.compile(r"^[A-Za-z0-9._\-]+(:[0-9]+)?$")
_CONTROL: Final = re.compile(r"[\x00-\x1f\x7f\s]")
_SECRET_PARTS: Final = ("accountkey", "sharedaccesssignature")  # the parts of a connection string that are secrets
_BLOB_URL: Final = re.compile(r"^(?:azure|abfss)://([^/@]+)/?(.*)$")
_CREDENTIAL_KEYS: Final = ("connection_string", "account_key", "client_secret")
# an ABFS URL back to the connector's (what Spark reports as a file's name), as a Spark regexp_replace
ABFS_PATTERN: Final = r"^abfss://([^@]+)@[^/]+/"
SOURCE_REPLACEMENT: Final = "azure://$1/"


def connection_parts(text: Any) -> dict[str, str]:
    """The `Name=value` parts of a connection string, names in lower case (a value may hold `=`: keys end in ==)."""
    parts = {}
    for item in str(text or "").split(";"):
        name, separator, value = item.partition("=")
        if separator and name.strip():
            parts[name.strip().lower()] = value
    return parts


def account_name(connection: Mapping[str, Any]) -> str | None:
    """The storage account: `account_name`, else the connection string's AccountName."""
    return connection.get("account_name") or connection_parts(connection.get("connection_string")).get(
        "accountname") or None


def account_key(connection: Mapping[str, Any]) -> str | None:
    """The storage account key: `account_key`, else the connection string's AccountKey."""
    return connection.get("account_key") or connection_parts(connection.get("connection_string")).get(
        "accountkey") or None


def endpoint_problem(value: Any) -> str | None:
    """Why an `endpoint` is neither `host[:port]` nor an http(s) URL without credentials, query or `;`, or None."""
    text = str(value)
    if _CONTROL.search(text) or ";" in text:
        return "expected a host and an optional port, or an http(s) URL: no whitespace, control character or `;`"
    if "://" in text:
        try:
            parts = urlsplit(text)
            _ = parts.port  # raises ValueError for a bad port
        except ValueError:
            return "is not a valid URL"
        if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password or \
                parts.query or parts.fragment:
            return "expected an http(s) URL of a host (and port, path), without credentials, query or fragment"
        return None
    if not _ENDPOINT.match(text):
        return "expected a host and an optional port, or an http(s) URL, e.g. 127.0.0.1:10000"
    return None


def connection_string(account: str, key: str, endpoint: str | None = None, use_ssl: bool = True) -> str:
    """The connection string of a storage account and its key (`endpoint`: the blob endpoint, for emulators)."""
    problem = endpoint_problem(endpoint) if endpoint else None
    if problem or any(";" in str(value) for value in (account, key)):
        raise ValueError("cannot build the connection string: %s" % (
            "endpoint %s" % problem if problem else "the account name and key cannot hold `;`"))
    protocol = "https" if use_ssl else "http"
    tail = "BlobEndpoint=%s;" % endpoint if endpoint else "EndpointSuffix=%s;" % DEFAULT_ENDPOINT_SUFFIX
    return "DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;%s" % (protocol, account, key, tail)


def build_secret(connection: Mapping[str, Any]) -> list[tuple[str, Any]]:
    """
    The parameters of DuckDB's azure secret for one credential combination: a connection string (or one built from
    `account_name` and `account_key`), a service principal (`client_secret`, `tenant_id`, `client_id`), or the account
    alone (a credential chain).
    """
    text = connection.get("connection_string")
    if not text and connection.get("account_key") and connection.get("account_name"):
        text = connection_string(connection["account_name"], connection["account_key"], connection.get("endpoint"),
                                 bool(connection.get("use_ssl", True)))
    if text:
        return [("CONNECTION_STRING", text)]
    options: list[tuple[str, Any]] = []
    if connection.get("client_secret"):
        options.append(("PROVIDER", "service_principal"))
        options += [(parameter, str(connection[key])) for key, parameter in (("tenant_id", "TENANT_ID"),
                                                                            ("client_id", "CLIENT_ID"))
                    if connection.get(key) is not None]
        options.append(("CLIENT_SECRET", str(connection["client_secret"])))
    options += [(parameter, str(connection[key])) for key, parameter in (("account_name", "ACCOUNT_NAME"),
                                                                       ("endpoint", "ENDPOINT"))
                if connection.get(key)]
    return options


def secret_parts(connection: Mapping[str, Any]) -> list[str]:
    """The secrets inside the connection string (its AccountKey, its SAS token), which messages must not show."""
    parts = connection_parts(connection.get("connection_string"))
    return [parts[name] for name in _SECRET_PARTS if parts.get(name)]


STORE: Final = ObjectStore(
    provider="azure_blob",
    schemes=SCHEMES,
    secret_type="azure",
    credentials={"connection_string": "CONNECTION_STRING", "account_key": "ACCOUNT_KEY",
                 "client_secret": "CLIENT_SECRET"},
    settings={"account_name": "ACCOUNT_NAME", "endpoint": "ENDPOINT", "tenant_id": "TENANT_ID",
              "client_id": "CLIENT_ID", "use_ssl": "USE_SSL"},
    flags=frozenset({"use_ssl"}),
    extension="azure",
    container_noun="container",
    container_pattern=re.compile(r"^[a-z0-9][a-z0-9._\-]*$", re.IGNORECASE),
    not_found=re.compile(r"\bHTTP 404\b|No files found that match the pattern|\bBlobNotFound\b|"
                         r"\bResourceNotFound\b|\bNoSuchKey\b", re.IGNORECASE),
    build_secret=build_secret,
    secret_parts=secret_parts,
)


def _credential_problems(connection: Mapping[str, Any]) -> list[str]:
    """One way to sign in: a connection string, or an account (with its key, a service principal, or alone)."""
    if "connection_string" in connection or "account_name" in connection:
        return []
    if "account_key" in connection:
        return ["account_key requires account_name"]
    if "client_secret" in connection:
        return ["client_secret requires account_name"]
    return ["needs credentials: connection_string, or account_name (with account_key, or client_secret, tenant_id "
            "and client_id, or alone for the platform's credential chain)"]


def validate_connection(connection: ResolvedConnection) -> list[str]:
    """
    Why a resolved connection cannot be used (beyond its fields' types and patterns): each root an azure:// or abfss://
    container prefix without query, glob or `..`; one way to sign in; an `endpoint` that is `host[:port]` or an http(s)
    URL (it ends up in a connection string: no `;`); no `;` in the account name or key.
    """
    problems = root_problems(STORE, connection["roots"]) if connection["roots"] else ["roots: name at least one"]
    problems += _credential_problems(connection)
    if connection.get("endpoint"):
        problem = endpoint_problem(connection["endpoint"])
        if problem:
            problems.append("endpoint: %s" % problem)
    if any(";" in str(connection.get(key) or "") for key in ("account_name", "account_key")):
        problems.append("account_name and account_key cannot hold `;`")
    return problems


def has_credentials(connection: Mapping[str, Any]) -> bool:
    """Whether the connection names its own credentials (otherwise: the platform's)."""
    return any(connection.get(key) is not None for key in _CREDENTIAL_KEYS)


def hadoop_url(url: str, account: str) -> str:
    """azure://container/key or abfss://container/key as abfss://container@account.dfs.core.windows.net/key."""
    match = _BLOB_URL.match(url)
    if not match:
        return url
    return "abfss://%s@%s.dfs.%s/%s" % (match.group(1), account, DEFAULT_ENDPOINT_SUFFIX, match.group(2))

