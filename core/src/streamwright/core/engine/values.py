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

"""Partition values: their key (the bookmarks' key in the state) and distinct values in first-seen order."""


from streamwright.core.runtime.templates import to_json

__all__ = ["Distinct", "partition_key"]


def partition_key(partition):
    """A partition as text (its bookmarks' key in the state): JSON, its DECIMAL values exact numbers (to_json)."""
    return to_json(partition, sort_keys=True, default=str)


class Distinct:
    """Distinct values (also lists and mappings) in the order they were first seen."""

    def __init__(self):
        self.values = []
        self._seen = set()

    def add(self, value):
        key = partition_key(value)
        if key not in self._seen:
            self._seen.add(key)
            self.values.append(value)
