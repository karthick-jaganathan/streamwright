# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the gcs connector knows about Google Cloud Storage, shared by every engine adapter: the store's facts (URL
scheme, DuckDB secret type, HMAC credential parameters), the rules a resolved connection must meet beyond its field
declarations, and how Hadoop reaches GCS (its S3-compatible endpoint through S3A). No engine library is imported here.
"""

from collections.abc import Mapping
from typing import Any, Final

from streamwright.core.io.locations import ObjectStore, root_problems
from streamwright.core.sdk.contract import ResolvedConnection

__all__ = ["HADOOP_ENDPOINT", "HADOOP_SCHEME", "SCHEME", "STORE", "buckets", "hadoop_url", "has_credentials",
           "validate_connection"]

SCHEME: Final = "gs"
HADOOP_SCHEME: Final = "s3a"
HADOOP_ENDPOINT: Final = "storage.googleapis.com"
STORE: Final = ObjectStore(
    provider="gcs",
    schemes=(SCHEME,),
    secret_type="gcs",
    credentials={"key_id": "KEY_ID", "secret": "SECRET"},
    settings={},
)


def validate_connection(connection: ResolvedConnection) -> list[str]:
    """
    Why a resolved connection cannot be used (beyond its fields' types and patterns): each root a gs:// bucket prefix
    without query, glob or `..`; `key_id` and `secret` together.
    """
    problems = root_problems(STORE, connection["roots"]) if connection["roots"] else ["roots: name at least one"]
    if ("key_id" in connection) != ("secret" in connection):
        problems.append("key_id and secret go together (neither: anonymous access)")
    return problems


def has_credentials(connection: Mapping[str, Any]) -> bool:
    """Whether the connection names its own HMAC key (otherwise: anonymous, or the platform's credentials)."""
    return any(connection.get(key) is not None for key in STORE.credentials)


def buckets(connection: Mapping[str, Any]) -> tuple[str, ...]:
    """The distinct buckets of the connection's roots, sorted."""
    return STORE.containers(connection["roots"])


def hadoop_url(url: str) -> str:
    """A gs:// URL as the S3A filesystem names it (s3a://); other text unchanged."""
    prefix = SCHEME + "://"
    return HADOOP_SCHEME + "://" + url[len(prefix):] if url.startswith(prefix) else url
