"""Lazy query-builder loading for rendered source requests."""

from streamwright.core.engine import queries
from streamwright.core.engine.errors import SourceError
from streamwright.core.runtime import components
from streamwright.core.runtime.components import ComponentLoadError


class QueryBuilders:
    """The query builders of the connectors a source may use, loaded when a request first calls them."""

    def __init__(self, allowed):
        self.builders = components.query_builders(allowed)  # name -> the ConnectorDefinition that lists it
        self.loaded = {}

    def build(self, unrendered, rendered):
        calls = queries.find_calls(unrendered, set(self.builders))
        if not calls:
            return rendered
        for _, name, _ in calls:
            if name not in self.loaded:
                try:
                    self.loaded[name] = components.load_query_builder(name, self.builders)
                except ComponentLoadError as exc:
                    raise SourceError(str(exc)) from exc
        return queries.build_queries(rendered, self.loaded, calls)
