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

"""Credentials masked by name in headers, URLs, form bodies and any log text."""


import collections
import re
from urllib.parse import unquote_plus

_CREDENTIAL_NAMES = ("authorization", "proxy-authorization", "cookie", "set-cookie", "sig")
_CREDENTIAL_PARTS = ("token", "key", "secret", "password", "signature", "credential")
# name=value in URLs (also XML-escaped, &amp;, and JSON-escaped as Go and .NET write & and ?: \u0026, \u003f) and
# other text
_ESCAPED_LEAD = r"\\u00(?:26|3[fF])"
_PARAMETER = re.compile(r"(?P<lead>[?&;]|%s)(?P<name>(?:(?!%s)[^=&;#?\s\"'<>])+)=(?P<value>(?:(?!%s)[^&#\s\"'<>])*)"
                        % ((_ESCAPED_LEAD,) * 3))
_FORM_PARAMETER = re.compile(r"(?P<lead>^|&)(?P<name>[^=&]+)=(?P<value>[^&]*)")
_AUTH_SCHEME = re.compile(r"\b(Bearer|Basic)(\s+)([A-Za-z0-9\-._~+/]+=*)", re.IGNORECASE)
# the password of scheme://user:password@host
# (found at its `://`, after a scheme character: a match that started at the scheme, at a word boundary, was tried at
# every part of a long dotted or dashed run, each scanning to its end, which made masking quadratic)
_USERINFO = re.compile(r"(?<=[A-Za-z0-9+.\-])(://[^/?#@:\s\"'<>]*:)([^/?#@\s\"'<>]+)@")


def sensitive(name):
    """
    Whether a header or parameter holds a credential: Authorization, Proxy-Authorization, Cookie, Set-Cookie, sig
    (signed URLs), and names containing token, key, secret, password, signature or credential (developer-token,
    api_key, client_secret, X-Amz-Signature, ...).
    """
    lowered = unquote_plus(str(name)).strip().lower()
    return lowered in _CREDENTIAL_NAMES or any(part in lowered for part in _CREDENTIAL_PARTS)


def _masked_parameter(match):
    value = match.group("value")
    if value and sensitive(match.group("name")):
        after = value[len(value.rstrip(":,.;)]}")):]  # what follows a URL in a sentence stays (masking twice too)
        return "%s%s=***%s" % (match.group("lead"), match.group("name"), after)
    return match.group(0)


def _masked_scheme(match):
    value = match.group(3)
    if len(value) >= 8 and any(character.isdigit() for character in value):
        return "%s%s***" % (match.group(1), match.group(2))
    return match.group(0)


def mask_headers(headers):
    """The headers (a mapping) with the values of credentials (see sensitive) replaced by ***."""
    return collections.OrderedDict((name, "***" if sensitive(name) else value) for name, value in
                                   (headers or {}).items())


def mask_url(url):
    """The URL with its password and the values of its credential parameters (see sensitive) replaced by ***."""
    return mask_text(str(url))


def mask_form(text):
    """A form-encoded body (a=1&b=2) with the values of its credential fields replaced by ***."""
    return _FORM_PARAMETER.sub(_masked_parameter, text)


def mask_text(text):
    """
    Any text with credentials masked by name: the values of credential parameters in URLs (see sensitive), URL
    passwords and Bearer / Basic authorization values. Every log line goes through it (LogSetup), after the run's
    Redactor.
    """
    if "=" in text:
        text = _PARAMETER.sub(_masked_parameter, text)
    if "://" in text:
        text = _USERINFO.sub(r"\1***@", text)
    return _AUTH_SCHEME.sub(_masked_scheme, text)
