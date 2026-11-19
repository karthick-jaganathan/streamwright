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

"""Shared lifecycle for outputs loading exports into destination tables."""

from streamwright.core.config.streams import incremental_exports
from streamwright.core.outputs.base import BaseOutput


class LoadingOutput(BaseOutput):
    """Dispositions, partial-load protection, state and summaries for table outputs."""

    def __init__(self, source):
        self.incremental = incremental_exports(source)
        self.partial = set()
        self.state = None
        self.status = None
        self.state_saved = False

    def write_state(self, state):
        self.state = state

    def mark_partial(self, name):
        self.partial.add(name)

    def disposition(self, name, keys):
        if keys:
            return "merge"
        return "append" if name in self.incremental else "replace"

    def _kept(self, name, keys):
        return name in self.partial and self.disposition(name, keys) == "replace"

    def _summary_entry(self, name, count, keys, table):
        return {
            "export": name, "records": count, "table": table,
            "disposition": "skipped" if self._kept(name, keys) else self.disposition(name, keys),
            "primary_key": list(keys),
        }
