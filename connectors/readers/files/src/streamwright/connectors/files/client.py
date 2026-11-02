# Copyright 2025 Karthick Jaganathan
# SPDX-License-Identifier: Apache-2.0

"""
What the files connector knows about the local filesystem, shared by every engine adapter: the rules a resolved
connection must meet beyond its field declarations (a list of folders) and Spark's `file://` URL.
No engine library is imported here.
"""

from typing import Final

from streamwright.core.sdk.contract import ResolvedConnection

__all__ = ["HADOOP_SCHEME", "SOURCE_PATTERN", "hadoop_url", "validate_connection"]

HADOOP_SCHEME: Final = "file"
SOURCE_PATTERN: Final = "^file:(//)?"  # a Hadoop file URL back to the path (as a Spark regexp_replace)


def validate_connection(connection: ResolvedConnection) -> list[str]:
    """Why a resolved connection cannot be used: no root, or a root that is no text (roots are folders)."""
    roots = connection["roots"]
    if not roots:
        return ["roots: name at least one folder"]
    problems = []
    for index, root in enumerate(roots):
        if not isinstance(root, str) or not root.strip():
            problems.append("roots[%d]: %r is not a folder" % (index, root))
    return problems


def hadoop_url(path: str) -> str:
    """A local path as Spark's file:// URL; a URL unchanged."""
    return path if "://" in path or path.startswith(HADOOP_SCHEME + ":") else HADOOP_SCHEME + "://" + path
