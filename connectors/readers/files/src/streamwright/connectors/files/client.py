#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

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
