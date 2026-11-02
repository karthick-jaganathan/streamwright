"""Shared lifecycle for outputs loading exports into destination tables."""

from streamwright.core.engine import sql
from streamwright.core.outputs.base import BaseOutput


class LoadingOutput(BaseOutput):
    """Dispositions, partial-load protection, state and summaries for table outputs."""

    def __init__(self, source):
        self.incremental = sql.incremental_exports(source)
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
