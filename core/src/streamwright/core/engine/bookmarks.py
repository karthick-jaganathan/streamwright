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

"""Incremental bookmark state and copy-on-write metrics snapshots."""

import copy
import datetime

from streamwright.core.config.inputs import as_date


class BookmarkStore:
    def __init__(self, state, output, metrics, today, *, key, legacy_key):
        self.state = copy.deepcopy(state) if state else {}
        self.state.setdefault("bookmarks", {})
        self.output = output
        self.metrics = metrics
        self.today = today
        self.key = key
        self.legacy_key = legacy_key

    def get(self, name, partition):
        """Return the saved cursor, migrating a legacy decimal key when present."""
        bookmarks = self.state["bookmarks"].get(name) or {}
        key = self.key(partition)
        if key not in bookmarks:
            legacy = self.legacy_key(partition)
            if legacy != key and legacy in bookmarks:
                self.unwritten_change()
                bookmarks[key] = bookmarks.pop(legacy)
        return bookmarks.get(key)

    def advance(self, stream, partition, window, write=True):
        """Save the last complete day, never moving a cursor backwards."""
        bookmarks = self.state["bookmarks"].get(stream["name"])
        key = self.key(partition)
        complete = min(window[1], self.today - datetime.timedelta(days=1))
        if bookmarks is None or key not in bookmarks or as_date(bookmarks[key]) < complete:
            if not write:
                self.unwritten_change()
            self.state["bookmarks"].setdefault(stream["name"], {})[key] = complete.isoformat()
        if write:
            self.flush()

    def flush(self):
        self.output.write_state(self.state)
        self.metrics.state = self.state

    def unwritten_change(self):
        if self.metrics.state is self.state:
            self.metrics.state = copy.deepcopy(self.state)
