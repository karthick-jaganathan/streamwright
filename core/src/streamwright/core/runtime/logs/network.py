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

"""The lines of `streamwright.network`: one per HTTP request or SDK call, and the HTTP exchange details."""


import json
import logging

from streamwright.core.runtime.logs.lines import NETWORK, fields, milliseconds, thousands, where_prefix
from streamwright.core.runtime.logs.redaction import mask_form, mask_headers, mask_url


def _headers_text(headers, redact):
    return redact(json.dumps(mask_headers(dict(headers or {})), default=str))


def _body_text(response):
    try:
        return response.content.decode(response.encoding or "utf-8", "replace")
    except LookupError:  # an unknown charset
        return response.content.decode("utf-8", "replace")


def http_summary(method, url, response=None, error=None, *, seconds=0.0, attempt=1, where=None, size=None):
    """
    The INFO line of one HTTP request on `streamwright.network`: its method, URL (pass it masked and redacted), status
    (or the error's type), response bytes, time and attempt.
    """
    if not NETWORK.isEnabledFor(logging.INFO):
        return
    status = None
    if response is not None:
        status = response.status_code
        if size is None:
            size = len(response.content or b"")
        outcome = "%d, %s bytes" % (status, thousands(size))
    else:
        outcome = type(error).__name__ if error is not None else "no response"
    NETWORK.info("%s%s %s: %s, %.2f s, attempt %d", where_prefix(where), method, url, outcome, seconds, attempt,
                 extra=fields(event="http_request", method=method, url=url, status=status, bytes=size,
                              duration_ms=milliseconds(seconds), attempt=attempt, **(where or {})))


def http_details(response, redact, body=True):
    """
    The DEBUG lines of one HTTP exchange of the requests library on `streamwright.network`: the request's method, URL,
    headers and body, then the response's status, headers and (`body`) body. Credentials in headers, URLs and form
    bodies are masked by name and secrets redacted; LogSetup truncates long bodies.
    """
    if not NETWORK.isEnabledFor(logging.DEBUG):
        return
    sent = response.request
    url = redact(mask_url(sent.url or ""))
    content = sent.body
    if isinstance(content, bytes):
        content = content.decode("utf-8", "replace")
    if content and "x-www-form-urlencoded" in (sent.headers.get("Content-Type") or ""):
        content = mask_form(content)
    NETWORK.debug("%s %s: request headers %s%s", sent.method, url, _headers_text(sent.headers, redact),
                  "\nrequest body: %s" % redact(content) if content else "")
    NETWORK.debug("%s %s: response %s %s, headers %s%s", sent.method, url, response.status_code, response.reason,
                  _headers_text(response.headers, redact),
                  "\nresponse body: %s" % redact(_body_text(response)) if body else "")


def request_details(method, url, headers, redact):
    """The DEBUG line of a request that got no response: its method, URL (masked and redacted) and headers."""
    if NETWORK.isEnabledFor(logging.DEBUG):
        NETWORK.debug("%s %s: request headers %s", method, url, _headers_text(headers, redact))


def sdk_summary(connector, call, where=None, page=None, records=None, *, seconds=0.0, attempt=1):
    """
    The INFO line of one response of a connector's SDK request on `streamwright.network`: the connector, service.method,
    the stream, request, partition and window, the page number and its records (for requests that read records), the
    time and the attempt of the call.
    """
    if not NETWORK.isEnabledFor(logging.INFO):
        return
    what = "%s %s" % (connector, call)
    if records is not None:
        what += " page %d: %s record(s)," % (page, thousands(records))
    else:
        what += ":"
    NETWORK.info("%s%s %.2f s, attempt %d", where_prefix(where), what, seconds, attempt,
                 extra=fields(event="sdk_call", connector=connector, call=call,
                              page=page if records is not None else None, records=records,
                              duration_ms=milliseconds(seconds), attempt=attempt, **(where or {})))
