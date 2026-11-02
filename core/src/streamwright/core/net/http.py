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

"""HTTP for sources: built-in auth, retries, rate limits, pagination and secret redaction."""

import datetime
import logging
import time
from types import MappingProxyType
from typing import NamedTuple
from urllib.parse import urljoin, urlsplit

import requests

from streamwright.core.net.auth import Authenticator
from streamwright.core.net.errors import HttpError, error_excerpt
from streamwright.core.net.paginators import paginate
from streamwright.core.net.policies import DEFAULT_RETRY, TIMEOUT_SECONDS, RateLimiter, RetryPolicy, parse_retry_after
from streamwright.core.runtime import logs
from streamwright.core.runtime.redact import Redactor
from streamwright.core.runtime.templates import to_text

__all__ = [
    "DEFAULT_RETRY",
    "Authenticator",
    "HttpClient",
    "HttpError",
    "RateLimiter",
    "Redactor",
    "RetryPolicy",
    "merge_headers",
    "paginate",
]

LOG = logging.getLogger("streamwright.source")

# the redirects one request follows (on its own origin only)
MAX_REDIRECTS = 10
REDIRECT_CODES = (301, 302, 303, 307, 308)
_DEFAULT_PORTS = MappingProxyType({"http": 80, "https": 443})


def _origin(url):
    parts = urlsplit(url)
    scheme = (parts.scheme or "").lower()
    try:
        port = parts.port
    except ValueError:
        port = None
    return scheme, (parts.hostname or "").lower(), port or _DEFAULT_PORTS.get(scheme)


def same_origin(trusted, url):
    """
    True when `url` is on the origin (scheme, host, port) of `trusted`: next-page links and redirects that leave it
    are not sent the source's credentials. An http -> https upgrade on the same host (default ports) is allowed; a
    downgrade to http is not.
    """
    scheme, host, port = _origin(trusted)
    other_scheme, other_host, other_port = _origin(url)
    if not host or host != other_host:
        return False
    if (scheme, port) == (other_scheme, other_port):
        return True
    return (scheme, port, other_scheme, other_port) == ("http", 80, "https", 443)




def merge_headers(source_headers, request_headers):
    """
    Merges source and request headers. Request headers override source headers of the same name,
    case-insensitively. Headers with empty string or None values are omitted.
    """
    merged = {}
    key_map = {}  # lowercase -> actual key in merged
    for k, v in (source_headers or {}).items():
        if v not in ("", None):
            merged[k] = v
            key_map[k.lower()] = k
    for k, v in (request_headers or {}).items():
        lower = k.lower()
        if lower in key_map:
            old_k = key_map[lower]
            del merged[old_k]
        if v not in ("", None):
            merged[k] = v
            key_map[lower] = k
    return merged


def _jsonable(value):
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def _with_params(url, params):
    """The URL a request with these query parameters goes to."""
    if not params:
        return url
    try:
        prepared = requests.models.PreparedRequest()
        prepared.prepare_url(url, params)
        return prepared.url
    except Exception:  # (the request fails too, and says why)
        return url


class _Sent(NamedTuple):
    """One request as it was sent, for its log lines."""

    method: str
    url: str
    params: dict
    headers: dict
    started: float
    attempt: int
    fields: dict | None


class HttpClient:
    """
    HTTP requests with auth, rate limits and retries. `metrics` (logs.RunMetrics) counts the requests and retries of the
    requests that name their stream (`fields`); `streamwright.network` logs each request (INFO) and its details (DEBUG).
    """

    def __init__(self, base_url=None, headers=None, *, authenticator=None, retry=None, rate_limiter=None,
                 redact=None, session=None, sleep=time.sleep, metrics=None, clock=time.monotonic, timeout=None):
        self.base_url = base_url
        self.headers = dict(headers or {})
        self.session = session or requests.Session()
        self.redact = redact or Redactor()
        self.authenticator = authenticator or Authenticator({}, self.session, self.redact)
        self.retry = RetryPolicy(retry)
        self.rate_limiter = rate_limiter
        self.sleep = sleep
        self.metrics = metrics
        self.clock = clock
        self.timeout = TIMEOUT_SECONDS if timeout is None else timeout
        self.last_url = None
        self.last_headers = None

    def url(self, path):
        if path.startswith(("http://", "https://")):
            return path
        if not self.base_url:
            raise HttpError("%r is not an absolute URL (http:// or https://)" % self.redact(path))
        return self.base_url.rstrip("/") + "/" + path.lstrip("/")

    def follow(self, link):
        """
        Resolves a next-page link against the previous request's URL. The link comes from a response, so it must stay
        on that request's origin: the next request carries the source's credentials and headers.
        """
        current = self.last_url or self.url("")
        url = urljoin(current, str(link))
        if not same_origin(current, url):
            raise HttpError(self.redact("the next-page link %s leaves %s; not sending the source's credentials to "
                                        "another origin" % (logs.mask_url(url), logs.mask_url(current))))
        return url

    def _sent(self, fields, attempt):
        if self.metrics is not None:
            self.metrics.request(fields, retry=attempt > 1)

    def _logged(self, sent, response=None, error=None, *, size=None, body=True):
        """The `streamwright.network` lines of one request: its summary line (INFO), then its details (DEBUG)."""
        if not logs.NETWORK.isEnabledFor(logging.INFO):
            return
        shown = self.redact(logs.mask_url(_with_params(sent.url, sent.params)))
        logs.http_summary(sent.method, shown, response, error, seconds=self.clock() - sent.started,
                          attempt=sent.attempt, where=sent.fields, size=size)
        if response is not None:
            logs.http_details(response, self.redact, body)
        else:
            logs.request_details(sent.method, shown, sent.headers, self.redact)

    def _send(self, method, url, params, headers, json_body, *, attempt, fields):
        """Sends one request, rate limited and authenticated, and logs it; returns its response."""
        if self.rate_limiter is not None:
            self.rate_limiter.wait()
        request_headers = merge_headers(self.headers, headers)
        request_params = dict(params or {})
        self.authenticator.apply(request_headers, request_params)
        request_headers = {name: to_text(value) for name, value in request_headers.items() if value not in ("", None)}
        self._sent(fields, attempt)
        sent = _Sent(method, url, request_params, request_headers, self.clock(), attempt, fields)
        try:
            response = self.session.request(method, url, params=request_params or None, headers=request_headers,
                                            json=_jsonable(json_body) if json_body is not None else None,
                                            timeout=self.timeout, allow_redirects=False)
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ContentDecodingError) as exc:
            self._logged(sent, error=exc)
            raise HttpError(self.redact("%s %s failed: %s" % (method, url, exc))) from exc
        except requests.RequestException as exc:
            self._logged(sent, error=exc)
            error = HttpError(self.redact("%s %s failed: %s" % (method, url, exc)))
            error.retryable = False
            raise error from exc
        self._logged(sent, response)
        return response

    def _decoded(self, response, method, url):
        """A successful response's JSON (None when it has no content)."""
        self.last_url = response.url
        if not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise HttpError("%s %s returned invalid JSON" % (method, self.redact(url)), response.status_code) from exc

    def request(self, method, path, params=None, headers=None, json_body=None, *, fields=None):
        """
        Sends a request with auth, rate limiting and retries; returns the decoded JSON (or None). `fields`: the
        stream, request, partition and window it reads, for log lines and the run's metrics.
        """
        url = self.url(path)
        refreshed = False
        redirects = 0
        retry_after = None

        def attempt(number):
            nonlocal method, url, params, json_body, refreshed, redirects, retry_after
            retry_after = None
            while True:
                response = self._send(method, url, params, headers, json_body, attempt=number, fields=fields)
                status = response.status_code
                if status == 401 and not refreshed and self.authenticator.refresh():
                    refreshed = True
                    continue
                location = response.headers.get("Location") if status in REDIRECT_CODES else None
                if location:
                    # followed here, not by requests: requests drops only `Authorization` on another host, not the
                    # source's other (secret) headers or an api key
                    target = urljoin(url, location)
                    if not same_origin(url, target):
                        raise HttpError(self.redact("%s %s redirects to %s; not sending the source's credentials to "
                                                    "another origin" % (method, logs.mask_url(url),
                                                                        logs.mask_url(target))), status)
                    redirects += 1
                    if redirects > MAX_REDIRECTS:
                        raise HttpError(self.redact("%s %s: more than %d redirects" % (method, logs.mask_url(url),
                                                                                      MAX_REDIRECTS)), status)
                    if status == 303 or (status in (301, 302) and method == "POST"):
                        method, json_body = "GET", None
                    url, params = target, None  # the server's Location carries the query
                    continue
                if response is not None:
                    self.last_headers = response.headers
                if 200 <= status < 300:
                    return self._decoded(response, method, url)
                error = HttpError(self.redact("%s %s failed: HTTP %d: %s" % (
                    method, url, status, error_excerpt(response.text, self.redact))), status)
                retry_after = parse_retry_after(response)
                raise error

        def classify(error):
            if not isinstance(error, HttpError):
                return False, None
            return (getattr(error, "retryable", error.status is None or error.status in self.retry.codes),
                    retry_after)

        return self.retry.run(attempt, classify, lambda error, delay, number:
                              self._on_retry(error, delay, number, fields))

    def _on_retry(self, error, delay, number, fields=None):
        logs.NETWORK.warning("%s; retrying in %.1fs (attempt %d of %d)", error, delay, number + 1,
                             self.retry.max_attempts,
                             extra=logs.fields(event="retry", status=error.status, attempt=number + 1,
                                               duration_ms=round(delay * 1000), **(fields or {})))
        self.sleep(delay)

    def download(self, url, handle, fields=None):
        """
        Streams the file at url into the binary file `handle` (rewritten on every attempt), with this client's
        auth, rate limit and retries; returns the number of bytes. `fields`: as for request().
        """
        url = self.url(url)
        redirects = 0
        has_credentials = bool(self.headers) or self.authenticator.kind is not None
        retry_after = None

        def attempt(number):
            nonlocal url, redirects, retry_after
            retry_after = None
            while True:
                if self.rate_limiter is not None:
                    self.rate_limiter.wait()
                headers, request_params = dict(self.headers), {}
                self.authenticator.apply(headers, request_params)
                headers = {name: to_text(value) for name, value in headers.items()}
                handle.seek(0)
                handle.truncate()
                self._sent(fields, number)
                sent = _Sent("GET", url, request_params, headers, self.clock(), number, fields)
                try:
                    response = self.session.get(url, params=request_params or None, headers=headers, stream=True,
                                                timeout=self.timeout, allow_redirects=False)
                    with response:
                        self.last_headers = response.headers
                        status = response.status_code
                        location = response.headers.get("Location") if status in REDIRECT_CODES else None
                        if location:
                            self._logged(sent, response, body=False)
                            target = urljoin(url, location)
                            if has_credentials and not same_origin(url, target):
                                raise HttpError(self.redact("GET %s redirects to %s; not sending the source's "
                                                            "credentials to another origin" % (
                                                                logs.mask_url(url), logs.mask_url(target))), status)
                            if _origin(url)[0] == "https" and _origin(target)[0] != "https":
                                raise HttpError(self.redact("GET %s redirects to %s; refusing downgrade from "
                                                            "https to http" % (
                                                                logs.mask_url(url), logs.mask_url(target))), status)
                            redirects += 1
                            if redirects > MAX_REDIRECTS:
                                raise HttpError(self.redact("GET %s: more than %d redirects" % (
                                    logs.mask_url(url), MAX_REDIRECTS)), status)
                            url = target
                            continue
                        if 200 <= status < 300:
                            size = 0
                            for chunk in response.iter_content(chunk_size=1 << 20):
                                handle.write(chunk)
                                size += len(chunk)
                            self._logged(sent, response, size=size, body=False)
                            return size
                        error = HttpError(self.redact("GET %s failed: HTTP %d: %s" % (
                            url, status, error_excerpt(response.text, self.redact))), status)
                        self._logged(sent, response, body=False)
                        retry_after = parse_retry_after(response)
                        raise error
                except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError,
                        requests.exceptions.ContentDecodingError) as exc:
                    self._logged(sent, error=exc)
                    raise HttpError(self.redact("GET %s failed: %s" % (url, exc))) from exc
                except requests.RequestException as exc:
                    self._logged(sent, error=exc)
                    error = HttpError(self.redact("GET %s failed: %s" % (url, exc)))
                    error.retryable = False
                    raise error from exc

        def classify(error):
            if not isinstance(error, HttpError):
                return False, None
            return (getattr(error, "retryable", error.status is None or error.status in self.retry.codes),
                    retry_after)

        return self.retry.run(attempt, classify, lambda error, delay, number:
                              self._on_retry(error, delay, number, fields))
