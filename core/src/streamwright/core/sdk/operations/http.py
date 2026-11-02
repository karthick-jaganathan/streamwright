"""
The HTTP request operation of declarative REST connectors, and their authentication fields. A source declares the
API in its YAML (`auth:` and `http:`, then `requests:` items `http: {path, method, ...}`); the source engine's HTTP
engine runs them. A connector's spec says which authentication the network takes - its `auth` block is the built-in
`type` and the keys of that type (the same names as `streamwright.core.validation.vocabulary.AUTH_TYPES`).
"""

from types import MappingProxyType
from typing import Final

from streamwright.core.sdk.contract import Field, Operation, OperationKind

__all__ = ["AUTH_TYPES", "REQUEST", "auth_fields"]

AUTH_TYPES: Final = ("bearer", "api_key", "basic", "oauth2_refresh_token")
_AUTH_KEYS: Final = MappingProxyType({
    "bearer": ("token",),
    "api_key": ("name", "value", "in"),
    "basic": ("username", "password"),
    "oauth2_refresh_token": ("token_url", "client_id", "client_secret", "refresh_token", "scopes"),
})
_CREDENTIALS: Final = ("token", "value", "password", "client_secret", "refresh_token")
_FIELDS: Final = MappingProxyType({
    "token": Field("token", "string", secret=True, description="the bearer token"),
    "name": Field("name", "string", description="the header or query parameter that carries the key"),
    "value": Field("value", "string", secret=True, description="the API key"),
    "in": Field("in", "string", choices=("header", "query"), default="header", description="where the key goes"),
    "username": Field("username", "string", description="the basic-auth user"),
    "password": Field("password", "string", secret=True, description="the basic-auth password"),
    "token_url": Field("token_url", "string", description="the OAuth 2 token endpoint"),
    "client_id": Field("client_id", "string", description="the OAuth 2 client id"),
    "client_secret": Field("client_secret", "string", secret=True, description="the OAuth 2 client secret"),
    "refresh_token": Field("refresh_token", "string", secret=True, description="the OAuth 2 refresh token"),
    "scopes": Field("scopes", "list", item_type="string", description="the scopes to request"),
})


def auth_fields(types: tuple[str, ...] = AUTH_TYPES, *, secrets: tuple[str, ...] = ()) -> tuple[Field, ...]:
    """
    The `auth` fields of a network that takes the built-in authentication `types`: `type` and the keys of those types.
    The credentials are secret references; `secrets` names more keys that are (e.g. an OAuth `client_id`). A key a
    type requires is validated by the source's own checks (`auth: <type>` names its required keys).
    """
    unknown = [kind for kind in types if kind not in _AUTH_KEYS]
    if unknown:
        raise ValueError("unknown authentication type(s): %s" % ", ".join(unknown))
    names = list(dict.fromkeys(key for kind in types for key in _AUTH_KEYS[kind]))
    fields = [Field("type", "string", required=True, choices=tuple(types), description="the authentication")]
    for name in names:
        field = _FIELDS[name]
        is_secret = name in _CREDENTIALS or name in secrets
        fields.append(Field(name, "string" if is_secret else field.type, secret=is_secret, choices=field.choices,
                            default=field.default, item_type=field.item_type, description=field.description))
    return tuple(fields)


REQUEST: Final = Operation(
    service="http",
    method="request",
    kind=OperationKind.READ,
    description="A request of the source's `http:` block (path, method, parameters) and its pages of records.",
    arguments=(
        Field("path", "string", required=True, description="appended to the source's `http.base_url`"),
        Field("method", "string", default="GET", choices=("GET", "POST", "PUT", "PATCH", "DELETE"),
              description="the HTTP verb"),
        Field("params", "mapping", description="the query string (nested mappings need `params_encoding: dotted`)"),
        Field("headers", "mapping", description="headers over the source's (templates; an empty value omits one)"),
        Field("json", ("mapping", "list"), description="the JSON body"),
        Field("params_encoding", "string", choices=("plain", "dotted"), description="how nested params are encoded"),
        Field("paginator", "mapping", description="overrides the source's paginator (type: none, offset, "
                                                  "page_number, cursor, link_header)"),
        Field("records", "mapping", description="overrides the source's records: {path, explode}"),
    ),
)
