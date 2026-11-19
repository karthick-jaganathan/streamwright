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
"""Retry-After: the seconds a server asks for, as a number or an HTTP-date; anything else falls back to the backoff."""

import datetime
from types import SimpleNamespace

import pytest

from streamwright.core.net.policies import RetryPolicy, parse_retry_after

NOW = datetime.datetime(2026, 11, 18, 12, 0, 0, tzinfo=datetime.UTC)


def _response(value):
    return SimpleNamespace(headers={} if value is None else {"Retry-After": value})


@pytest.mark.parametrize("value,seconds", [
    ("7", 7.0), (" 2.5 ", 2.5), ("0", 0.0), ("Wed, 18 Nov 2026 12:00:30 GMT", 30.0),
    ("Wed, 18 Nov 2026 11:59:00 GMT", 0.0), (None, None), ("", None), ("nan", None), ("-5", None), ("inf", None),
    ("1e400", None), ("soon", None), ("Wed, 99 Nov 2026 12:00:30 GMT", None),
])
def test_retry_after_is_finite_seconds_or_an_http_date(value, seconds):
    assert parse_retry_after(_response(value), now=NOW) == seconds


@pytest.mark.parametrize("retry_after", [float("nan"), -5.0, float("inf"), "3", True])
def test_an_invalid_retry_after_falls_back_to_the_backoff(retry_after):
    policy = RetryPolicy({"max_delay": "10s"})
    assert policy.delay(3, retry_after) == 4
    assert policy.delay(3, 600.0) == 10.0 and policy.delay(3, 0) == 0     # clamped to max_delay
