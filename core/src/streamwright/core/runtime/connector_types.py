"""
What a connector's source session uses at run time (`ConnectorContext`, `ConnectorError`) and the base of the query
builders a connector definition lists (`QueryBuilder`). Re-exported by streamwright.core.runtime.components and
streamwright.core.sdk.
"""

import logging
import time

from streamwright.core.net.http import RetryPolicy
from streamwright.core.net.policies import retryable_error
from streamwright.core.runtime import logs

__all__ = ["LOG", "ComponentLoadError", "ConnectorContext", "ConnectorError", "QueryBuilder"]

LOG = logging.getLogger("streamwright.source")


class ConnectorError(Exception):
    """
    A failed connector call. `codes` are every provider error code it carries (`code` is the first), matched against
    `retry.codes` (the stream's, else the connector's `retry_codes`); `retryable` is the connector's own transient
    judgement for what has no code (a dropped connection) or what the vendor flags transient; `retry_after` is in
    seconds.
    """

    def __init__(self, message, code=None, retryable=False, retry_after=None, *, codes=()):
        super().__init__(message)
        self.codes = tuple(item for item in codes if item is not None) or (() if code is None else (code,))
        self.code = self.codes[0] if self.codes else None
        self.retryable = retryable
        self.retry_after = retry_after


class ComponentLoadError(Exception):
    pass


class QueryBuilder:
    """
    Base class for query builders, listed by name in a connector definition's `query_builders`. In a request,
    `{<name>: spec}` (a mapping with that single key) is replaced by build(spec), the query text. The spec's references
    are rendered first, with their types (dates, numbers, lists), so build() must write every value safely: checked,
    quoted and escaped for its query language.
    """

    def check(self, spec):
        """Problems with an unrendered spec (values may still be references) as (path inside the spec, message)."""
        return []

    def build(self, spec):
        """The query text for a rendered spec; raises streamwright.core.engine.queries.QueryError for values it cannot
        write."""
        raise NotImplementedError


class ConnectorContext:
    """
    What a stream gives its connector's session: redaction - secret() masks what the connector obtains while the run
    goes (access tokens, refreshed tokens, signatures) in every log line and error - and calls with the stream's rate
    limit and retry policy, counted in the run's metrics (`metrics`, logs.RunMetrics) for the stream and request in
    `where`, which the runner sets. `connector` is the connector's name; `attempt` is the attempts the last call took.
    Sessions raise ConnectorError for the errors of their vendor (see streamwright.core.io.api_calls); call() retries
    those its policy names and lets every other exception through.
    """

    def __init__(self, connector, redact, retry=None, rate_limiter=None,  # noqa: PLR0917 (public API)
                 sleep=time.sleep, metrics=None, clock=time.monotonic, *, default_codes=()):
        self.connector = connector
        self.redact = redact
        self.retry = RetryPolicy(retry)
        # the stream's `retry.codes` replace the connector's `retry_codes` (the HTTP default codes do not apply)
        given = (retry or {}).get("codes")
        self.codes_replaced = given is not None
        self.codes = tuple(given if given is not None else default_codes)
        self.rate_limiter = rate_limiter
        self.sleep = sleep
        self.metrics = metrics
        self.clock = clock
        self.where = None  # the stream, request, partition and window of the calls being made
        self.attempt = 0
        self.log = LOG

    def secret(self, value):
        """Masks `value` (e.g. an access token the connector obtained) as *** in every log line and error from now
        on."""
        self.redact.add(value)

    def retryable(self, error):
        return retryable_error(error, self.codes, replaced=self.codes_replaced)

    def call(self, function, *args, **kwargs):
        """Calls function(*args, **kwargs) after the rate limit; retries ConnectorErrors per the retry policy."""
        def attempt(number):
            if self.rate_limiter is not None:
                self.rate_limiter.wait()
            if self.metrics is not None:
                self.metrics.request(self.where, retry=number > 1)
            self.attempt = number
            return function(*args, **kwargs)

        def classify(error):
            return (self.retryable(error), error.retry_after) if isinstance(error, ConnectorError) else (False, None)

        def on_retry(error, delay, number):
            logs.NETWORK.warning("%s; retrying in %.1fs (attempt %d of %d)", self.redact(error), delay, number + 1,
                                 self.retry.max_attempts,
                                 extra=logs.fields(event="retry", code=None if error.code is None else str(error.code),
                                                   attempt=number + 1, duration_ms=round(delay * 1000),
                                                   **(self.where or {})))
            self.sleep(delay)

        return self.retry.run(attempt, classify, on_retry)
