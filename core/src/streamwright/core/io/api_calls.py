"""
What the source sessions of SDK connectors share: the vendor's exceptions become ConnectorError (a source session
raises them itself, so `context.call` can retry the retryable ones), and a context for use outside a stream run.
"""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from streamwright.core.runtime.connector_types import ConnectorContext, ConnectorError
from streamwright.core.runtime.redact import Redactor

__all__ = ["make_caller", "standalone_context", "translate_errors", "translate_pages"]

Translate = Callable[[Exception], ConnectorError | None]


@contextmanager
def translate_errors(translate: Translate) -> Iterator[None]:
    """Raises the ConnectorError `translate` makes of an exception; exceptions it does not know pass through."""
    try:
        yield
    except ConnectorError:
        raise
    except Exception as exc:
        error = translate(exc)
        if error is None:
            raise
        raise error from exc


def translate_pages(pages: Iterator[Any], translate: Translate) -> Iterator[Any]:
    """The pages of a read, with the errors raised while reading them translated."""
    with translate_errors(translate):
        yield from pages


def make_caller(context: ConnectorContext, translate: Translate) -> Callable[..., Any]:
    """`call(function, *args, **kwargs)`: the context's rate limit and retries for a call with translated errors."""

    def call(function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        def translated() -> Any:
            with translate_errors(translate):
                return function(*args, **kwargs)
        return context.call(translated)

    return call


def standalone_context(redactor: Redactor) -> ConnectorContext:
    """A context without a stream's rate limit and retry policy (engines that read driver-side, e.g. Spark)."""
    return ConnectorContext("standalone", redactor)
