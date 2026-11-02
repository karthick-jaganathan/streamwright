"""
Section checkers share an issue sink and optional stream context.

The `kind: source` checks are split by section into this package's modules, chained from document to async_jobs
(validation.source.SourceChecker).
"""

__all__ = ["SectionChecker"]


class SectionChecker:
    def __init__(self, sink, stream=None):
        self.sink = sink
        self.stream = stream

    def __getattr__(self, name):
        return getattr(self.sink, name)
