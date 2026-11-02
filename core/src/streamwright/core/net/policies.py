#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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

"""The policies of HTTP requests: their timeout, retries (RetryPolicy) and rate limits (RateLimiter)."""


import collections
import time
from types import MappingProxyType

from streamwright.core.config.inputs import parse_duration
from streamwright.core.runtime import logs

DEFAULT_RETRY = MappingProxyType({"codes": [429, 500, 502, 503, 504], "max_attempts": 3, "backoff": "exponential",
                                  "max_delay": "60s"})
TIMEOUT_SECONDS = 60


def error_codes(error):
    """Every provider code an error carries, as text: its `codes`, else its `code` (none: a codeless error)."""
    codes = getattr(error, "codes", None) or ()
    if not codes and getattr(error, "code", None) is not None:
        codes = (error.code,)
    return tuple(str(code) for code in codes)


def retryable_error(error, codes, *, replaced):
    """
    Whether a failed call is worth retrying. An error with provider codes is retried when one of them is in `codes`
    (compared as text, so `613` and `"613"` match), or when its connector flags it transient and the user's
    `retry.codes` did not `replace` the defaults. A codeless error (a dropped connection, a timeout) is retried when
    its connector flags it transient.
    """
    flagged = bool(getattr(error, "retryable", False))
    found = error_codes(error)
    if not found:
        return flagged
    wanted = {str(code) for code in codes}
    return any(code in wanted for code in found) or (flagged and not replaced)


class RateLimiter:
    """Allows `requests` calls per `per` seconds (sliding window)."""

    def __init__(self, requests_per_period, period_seconds, clock=time.monotonic, sleep=time.sleep):
        self.limit = requests_per_period
        self.period = period_seconds
        self.clock = clock
        self.sleep = sleep
        self.calls = collections.deque()

    def wait(self):
        now = self.clock()
        while self.calls and now - self.calls[0] >= self.period:
            self.calls.popleft()
        if len(self.calls) >= self.limit:
            delay = self.period - (now - self.calls[0])
            logs.NETWORK.info("rate limit reached; waiting %.1fs", delay,
                              extra=logs.fields(event="rate_limit", duration_ms=round(delay * 1000)))
            self.sleep(delay)
            self.calls.popleft()
        self.calls.append(self.clock())


class RetryPolicy:
    """A `retry` block: codes (HTTP statuses or provider error codes), max_attempts, backoff, max_delay."""

    def __init__(self, retry=None):
        retry = dict(DEFAULT_RETRY, **(retry or {}))
        self.codes = set(retry["codes"])
        self.max_attempts = retry["max_attempts"]
        self.backoff = retry["backoff"]
        self.max_delay = parse_duration(retry["max_delay"]).total_seconds()

    def delay(self, attempt, retry_after=None):
        delay = 2 ** (attempt - 1) if self.backoff == "exponential" else 1
        if retry_after is not None:
            delay = retry_after
        return min(delay, self.max_delay)

    def run(self, attempt, classify, on_retry):
        """Execute attempts; classify(error) returns (retryable, retry_after)."""
        for number in range(1, self.max_attempts + 1):
            try:
                return attempt(number)
            except Exception as exc:
                retryable, retry_after = classify(exc)
                if not retryable or number == self.max_attempts:
                    raise
                on_retry(exc, self.delay(number, retry_after), number)
        return None


def parse_retry_after(response):
    value = response.headers.get("Retry-After") if response is not None else None
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None
