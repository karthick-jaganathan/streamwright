#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
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

"""Collecting a checker's issues, and the iteration helpers every section shares."""

from streamwright.core.jobs.fields import path_text
from streamwright.core.validation import Issue
from streamwright.core.validation.checker import ERROR, WARNING

__all__ = ["Findings", "connections_of", "named_items"]


class Findings:
    def __init__(self, filename=None):
        self.filename, self.issues = filename, []

    def error(self, code, path, message):
        self.issues.append(Issue(ERROR, code, message, self.filename, path_text(path)))

    def warning(self, code, path, message):
        self.issues.append(Issue(WARNING, code, message, self.filename, path_text(path)))

    def add(self, code, path, message, is_warning=False):
        (self.warning if is_warning else self.error)(code, path, message)

    @property
    def errors(self):
        return [issue for issue in self.issues if issue.severity == ERROR]


def named_items(job, section):
    """[(index, item)] of the mappings with a text name in a steps/outputs list (others are structure errors)."""
    items = job.get(section)
    if not isinstance(items, list):
        return []
    return [(index, item) for index, item in enumerate(items)
            if isinstance(item, dict) and isinstance(item.get("name"), str)]


def connections_of(job):
    """{name: connection} for the well-formed connections (a mapping with a text provider)."""
    found = job.get("connections")
    if not isinstance(found, dict):
        return {}
    return {name: conn for name, conn in found.items()
                if isinstance(conn, dict) and isinstance(conn.get("provider"), str)}
