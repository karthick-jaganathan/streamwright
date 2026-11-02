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

"""
The execution policy shared by every job engine (connector contract v1.2 §6a).

One vocabulary for paging, retries, rate limits, error handling and timeouts, with identical defaults wherever it
applies: a stream, a step or an output. An engine declares once (its capabilities) which keys it honours on steps
and on outputs; a key it does not honour is an `unsupported-argument` error that names the engine, never silently
ignored.

| Key                                               | Meaning                                              |
|---------------------------------------------------|------------------------------------------------------|
| `retry {max_attempts, backoff, max_delay, codes}` | transient-error retries of calls/reads/writes        |
| `rate_limit {requests, per}`                       | client-side throttling of calls                      |
| `paginator` / `page_size`                          | HTTP paging; vendor page or batch size               |
| `on_error: fail | skip | warn`                     | a failed partition/file/record batch or output       |
| `timeout`                                          | per-call timeout (seconds or a duration like `30s`)  |

The retries use the ONE shared implementation, `streamwright.core.net.policies.RetryPolicy.run`; there is no other
retry loop. `guard()` is the one place a job engine runs a step or an output under its policy.
"""

import threading
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from streamwright.core.config.inputs import parse_duration
from streamwright.core.net.policies import DEFAULT_RETRY, TIMEOUT_SECONDS, RetryPolicy, retryable_error

__all__ = [
    "DEFAULTS",
    "ON_ERROR",
    "OUTPUT_POLICY_KEYS",
    "SKIPPED",
    "STEP_POLICY_KEYS",
    "ExecutionPolicy",
    "PolicyError",
    "TransientError",
    "check_policy",
    "classify_transient",
    "guard",
]

ON_ERROR = ("fail", "skip", "warn")
# the whole vocabulary; an engine's capabilities are a subset of this per surface
STEP_POLICY_KEYS = ("retry", "rate_limit", "paginator", "page_size", "on_error", "timeout")
OUTPUT_POLICY_KEYS = ("retry", "on_error", "timeout")
# the one default, referenced (not copied) so every engine's defaults are literally identical
DEFAULTS = MappingProxyType({"retry": DEFAULT_RETRY, "on_error": "fail", "timeout": TIMEOUT_SECONDS})

# a sentinel returned by guard() when on_error tolerated (skip/warn) a failure
SKIPPED = object()


class PolicyError(Exception):
    """A unit of work failed and its `on_error` policy is `fail`."""


class TransientError(Exception):
    """
    A failure an engine may retry: a transient read/connect/write error (a timeout, a throttle, a 5xx). Adapters
    raise it (or set `retryable = True` on their own error) so the shared retry treats it as retryable; the optional
    `code` is matched against the policy's `retry.codes` and `retry_after` honours a server-provided back-off.
    """
    retryable = True
    code = None
    retry_after = None


def classify_transient(policy):
    """
    A classifier for `guard`/`RetryPolicy.run` (the shared rule, `net.policies.retryable_error`): an error with codes
    is retried when one is in `retry.codes`, or when it is flagged transient and the step/output kept the default
    codes; a codeless error (a `TransientError`, a timeout) when it is flagged transient.
    """
    codes = tuple(policy.retry.get("codes") or ())

    def classify(exc):
        return retryable_error(exc, codes, replaced=policy.codes_replaced), getattr(exc, "retry_after", None)

    return classify


def _seconds(value):
    """A timeout as seconds: a number is seconds, a string is a duration (`30s`, `2m`)."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return parse_duration(value).total_seconds()


@dataclass(frozen=True)
class ExecutionPolicy:
    """A step's or an output's resolved policy; absent keys fall back to the shared defaults."""
    retry: dict
    on_error: str
    timeout_seconds: float | None
    rate_limit: dict | None = None
    paginator: Any = None
    page_size: int | None = None
    codes_replaced: bool = False  # the step/output gave its own `retry.codes` (they replace the defaults)

    @classmethod
    def of(cls, item):
        """Read the policy keys of a step/output mapping (unknown keys ignored here; validation names them)."""
        item = item or {}
        return cls(
            retry=dict(DEFAULT_RETRY, **(item.get("retry") or {})),
            on_error=item.get("on_error", DEFAULTS["on_error"]),
            timeout_seconds=_seconds(item["timeout"]) if "timeout" in item else None,
            rate_limit=item.get("rate_limit"),
            paginator=item.get("paginator"),
            page_size=item.get("page_size"),
            codes_replaced="codes" in (item.get("retry") or {}))

    @property
    def tolerated(self):
        """True when a failure is skipped or warned about instead of failing the batch."""
        return self.on_error in ("skip", "warn")


def check_policy(item, supported, engine, *, surface):
    """
    Problems with the policy keys of one step/output: a vocabulary key the engine does not honour on this `surface`
    is an `unsupported-argument` error naming the engine; `on_error` choices are checked. `supported` is the engine's
    set of honoured keys for this surface. Returns `[(code, message, is_warning)]`.
    """
    found = []
    for key in STEP_POLICY_KEYS:
        if key in item and key not in supported:
            found.append(("unsupported-argument",
                          "`%s` is not supported by the %s engine on %s" % (key, engine, surface), False))
    if "on_error" in item and item["on_error"] not in ON_ERROR:
        found.append(("bad-value", "on_error is one of: %s" % ", ".join(ON_ERROR), False))
    return found


def _with_timeout(work, seconds):
    """Run `work()` with a per-call deadline; a unit that overruns raises TimeoutError (and is left behind)."""
    if not seconds:
        return work()
    done: list = []
    failed: list = []

    def target():
        try:
            done.append(work())
        except BaseException as exc:                       # re-raised on the calling thread
            failed.append(exc)

    thread = threading.Thread(target=target, name="streamwright-policy-timeout", daemon=True)
    thread.start()
    thread.join(seconds)
    if thread.is_alive():
        raise TimeoutError("timed out after %gs" % seconds)
    if failed:
        raise failed[0]
    return done[0]


def guard(policy, work, *, classify, on_retry=None, on_tolerated=None):
    """
    Run `work()` under `policy` and return its value, or `SKIPPED` when `on_error` tolerated a failure.

    Transient errors retry through the ONE shared `RetryPolicy.run` (`classify(exc) -> (retryable, retry_after)`);
    `timeout` bounds each attempt; then `on_error` decides: `fail` re-raises, `skip`/`warn` return `SKIPPED` after
    calling `on_tolerated(exc, on_error)` (a warn logs; a skip also marks the batch partial — the caller's choice).
    """
    retrier = RetryPolicy(policy.retry)

    def attempt(_number):
        return _with_timeout(work, policy.timeout_seconds)

    try:
        return retrier.run(attempt, classify, on_retry or (lambda *args: None))
    except Exception as exc:
        if policy.on_error == "fail":
            raise
        if on_tolerated is not None:
            on_tolerated(exc, policy.on_error)
        return SKIPPED
