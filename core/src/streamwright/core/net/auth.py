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

"""Built-in HTTP authentication: a rendered `auth` block applied to each request."""


import base64
import time

import requests

from streamwright.core.net.errors import HttpError, error_excerpt
from streamwright.core.net.policies import TIMEOUT_SECONDS
from streamwright.core.runtime import logs
from streamwright.core.runtime.templates import to_text


class Authenticator:
    """Applies a rendered built-in `auth` block to each request."""

    def __init__(self, auth, session, redact, clock=time.time):
        self.auth = auth or {}
        self.kind = self.auth.get("type")
        self.session = session
        self.redact = redact
        self.clock = clock
        self._token = None
        self._expires_at = 0.0

    def apply(self, headers, params):
        auth = self.auth
        if self.kind == "bearer":
            token = auth.get("token")
            if not token:
                raise HttpError("bearer auth token is empty or missing")
            headers["Authorization"] = "Bearer %s" % token
        elif self.kind == "api_key":
            if auth.get("in", "header") == "query":
                params[auth["name"]] = auth["value"]
            else:
                headers[auth["name"]] = to_text(auth["value"])
        elif self.kind == "basic":
            pair = ("%s:%s" % (auth["username"], auth["password"])).encode("utf-8")
            credential = base64.b64encode(pair).decode("ascii")
            self.redact.add(credential)
            headers["Authorization"] = "Basic %s" % credential
        elif self.kind == "oauth2_refresh_token":
            headers["Authorization"] = "Bearer %s" % self._access_token()

    def refresh(self):
        """Drops a cached OAuth token after a 401; returns True if a retry can help."""
        if self.kind != "oauth2_refresh_token":
            return False
        self._token = None
        return True

    def _access_token(self):
        if self._token and self.clock() < self._expires_at:
            return self._token
        auth = self.auth
        refresh_token = auth.get("refresh_token")
        if not refresh_token:
            raise HttpError("oauth2 refresh_token is empty or missing")
        data = {"grant_type": "refresh_token", "refresh_token": refresh_token,
                "client_id": auth["client_id"], "client_secret": auth["client_secret"]}
        if auth.get("scopes"):
            data["scope"] = " ".join(auth["scopes"])
        url = self.redact(logs.mask_url(auth["token_url"]))
        started = time.monotonic()
        try:
            response = self.session.post(auth["token_url"], data=data, timeout=TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            logs.http_summary("POST", url, error=exc, seconds=time.monotonic() - started)
            raise HttpError(self.redact("token request to %s failed: %s" % (auth["token_url"], exc))) from exc
        payload = response.json() if response.status_code == 200 else None
        if isinstance(payload, dict):  # the access token, and a new refresh token or ID token, are secrets too
            for key, value in payload.items():
                if str(key).lower().endswith("token") and isinstance(value, str):
                    self.redact.add(value)
        logs.http_summary("POST", url, response, seconds=time.monotonic() - started)
        logs.http_details(response, self.redact, body=False)  # a token response is credentials
        if response.status_code != 200:
            raise HttpError(self.redact("token request to %s failed: HTTP %d: %s" % (
                auth["token_url"], response.status_code, error_excerpt(response.text, self.redact))),
                response.status_code)
        token = payload.get("access_token")
        if not token:
            raise HttpError("token response from %s has no access_token" % auth["token_url"])
        self.redact.add(token)
        self._token = token
        self._expires_at = self.clock() + max(0.0, float(payload.get("expires_in", 3600)) - 60)
        return token
