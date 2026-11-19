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
Who is calling (plan §5.3, §5.4): `Authorization: Bearer <token>`, an API key or a JWT.

- API keys: the service keeps only their SHA-256 digests; a presented key is hashed and compared with every digest
  in constant time. Rotating a key: list the new digest beside the old one, then remove the old.
- JWT: the signature is checked against the issuer's keys (JWKS, asymmetric algorithms only, and only those of the
  key's type: RSA, EC with its curve, Ed25519), with `iss`, `aud`, `exp` and `nbf` (a minute of clock skew); the caller
  is its `sub`, and its claims are the token's.
- The two kinds never meet: a caller's id is `key:<api key id>` or `jwt:<sub>`, and owns its runs and Idempotency-Keys;
  a JWT whose `sub` is an API key's id is not that key.
- A service without an `auth` section (loopback only: config refuses any other address) has one anonymous caller.
Failures are 401 with `WWW-Authenticate: Bearer` and no detail on which check failed.
"""

import hashlib
import hmac
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

import jwt

from streamwright.core.serve.envelope import RequestError

__all__ = ["ANONYMOUS", "JWT_ALGORITHMS", "Authenticator", "Principal"]

JWT_ALGORITHMS = ("RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA")
_RSA_ALGORITHMS = JWT_ALGORITHMS[:6]
LEEWAY_SECONDS = 60
_CHALLENGE = MappingProxyType({"WWW-Authenticate": "Bearer"})


@dataclass(frozen=True)
class Principal:
    """A caller: an API key (kind `key`, named by its id), a JWT's subject (`jwt`) or a service's one `anonymous`."""

    kind: str
    name: str
    claims: Any = field(default_factory=lambda: MappingProxyType({}))

    @property
    def id(self):
        """What owns the caller's runs, Idempotency-Keys and rate limits: `key:<id>`, `jwt:<sub>` or `anonymous`."""
        return self.name if self.kind == "anonymous" else "%s:%s" % (self.kind, self.name)


ANONYMOUS = Principal("anonymous", "anonymous")


def _unauthorized():
    return RequestError(401, "unauthorized", "missing or invalid credentials", _CHALLENGE)


class Authenticator:
    """Turns an Authorization header into a Principal (RequestError 401 otherwise)."""

    def __init__(self, service, jwks_client=None):
        self.service = service
        self._keys = tuple(service.api_keys)
        self._jwks = jwks_client
        if service.jwt is not None and jwks_client is None:
            self._jwks = jwt.PyJWKClient(service.jwt.jwks_url, cache_keys=True, lifespan=300, timeout=10)

    def authenticate(self, authorization):
        if not self.service.has_auth:
            return ANONYMOUS
        scheme, _, token = (authorization or "").partition(" ")
        token = token.strip()
        if scheme.lower() != "bearer" or not token or len(token) > 8192:
            raise _unauthorized()
        principal = self._api_key(token)
        if principal is None and self.service.jwt is not None and token.count(".") == 2:
            principal = self._jwt(token)
        if principal is None:
            raise _unauthorized()
        return principal

    def _api_key(self, token):
        digest = hashlib.sha256(token.encode("utf-8", "surrogatepass")).hexdigest()
        found = None
        for key in self._keys:                    # every digest is compared: the time does not tell which matched
            if hmac.compare_digest(digest, key.sha256) and found is None:
                found = key
        return None if found is None else Principal("key", found.id, found.claims)

    def _jwt(self, token):
        settings = self.service.jwt
        try:
            key = self._jwks.get_signing_key_from_jwt(token)
            claims = jwt.decode(token, key.key, algorithms=_algorithms(key), audience=settings.audience,
                                issuer=settings.issuer, leeway=LEEWAY_SECONDS,
                                options={"require": ["exp", "iss", "aud", "sub"]})
        except Exception:                         # a bad token, a key it does not fit, an unreachable JWKS: 401
            return None
        subject = claims.get("sub")
        if not isinstance(subject, str) or not subject:
            return None
        return Principal("jwt", subject, MappingProxyType(dict(claims)))


def _algorithms(key):
    """The algorithms a JWKS key (PyJWK) verifies: an RSA key's RS* and PS*; any other key's own (EC: its curve's)."""
    named = _RSA_ALGORITHMS if key.key_type == "RSA" else (key.algorithm_name,)
    return [name for name in named if name in JWT_ALGORITHMS]
