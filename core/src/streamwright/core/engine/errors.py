"""The error of a source run (re-exported by streamwright.core.engine.runner, its public import path)."""

__all__ = ["SourceError"]


class SourceError(Exception):
    """A source that cannot run, or a stream that failed (messages are redacted)."""
