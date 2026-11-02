# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the s3 connector knows about Amazon S3 and S3-compatible stores, shared by every engine adapter: the store's
facts (URL scheme, DuckDB secret type, credential and setting parameters), the rules a resolved connection must meet
beyond its field declarations, and the S3A (Hadoop) URL scheme. No engine library is imported here.
"""

from collections.abc import Mapping
from typing import Any, Final

from streamwright.core.io.locations import ObjectStore, root_problems
from streamwright.core.sdk.contract import ResolvedConnection

__all__ = ["HADOOP_SCHEME", "SCHEME", "STORE", "buckets", "hadoop_url", "has_credentials", "uses_path_style",
           "uses_ssl", "validate_connection"]

SCHEME: Final = "s3"
HADOOP_SCHEME: Final = "s3a"
STORE: Final = ObjectStore(
    provider="s3",
    schemes=(SCHEME,),
    secret_type="s3",
    credentials={"key_id": "KEY_ID", "secret": "SECRET", "session_token": "SESSION_TOKEN"},
    settings={"region": "REGION", "endpoint": "ENDPOINT", "url_style": "URL_STYLE", "use_ssl": "USE_SSL"},
    flags=frozenset({"use_ssl"}),
)


def validate_connection(connection: ResolvedConnection) -> list[str]:
    """
    Why a resolved connection cannot be used (beyond its fields' types, choices and patterns): each root an s3://
    bucket prefix without query, glob or `..`; `key_id` and `secret` together; `session_token` only with them.
    """
    problems = root_problems(STORE, connection["roots"]) if connection["roots"] else ["roots: name at least one"]
    if ("key_id" in connection) != ("secret" in connection):
        problems.append("key_id and secret go together (neither: anonymous access)")
    if "session_token" in connection and "key_id" not in connection:
        problems.append("session_token goes with key_id and secret")
    return problems


def has_credentials(connection: Mapping[str, Any]) -> bool:
    """Whether the connection names its own credentials (otherwise: anonymous, or the platform's chain)."""
    return any(connection.get(key) is not None for key in STORE.credentials)


def buckets(connection: Mapping[str, Any]) -> tuple[str, ...]:
    """The distinct buckets of the connection's roots, sorted."""
    return STORE.containers(connection["roots"])


def uses_ssl(connection: Mapping[str, Any]) -> bool:
    return bool(connection.get("use_ssl", True))


def uses_path_style(connection: Mapping[str, Any]) -> bool:
    """Whether buckets are addressed as endpoint/bucket (url_style: path) rather than bucket.endpoint."""
    return connection.get("url_style") == "path"


def hadoop_url(url: str) -> str:
    """An s3:// URL as the S3A filesystem names it (s3a://); other text unchanged."""
    prefix = SCHEME + "://"
    return HADOOP_SCHEME + "://" + url[len(prefix):] if url.startswith(prefix) else url
