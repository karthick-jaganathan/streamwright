"""
The connection of an attached database (postgres, mysql): a DSN (a secret: a URL or libpq-style `key=value` pairs) or
the structured keys (host, port, database, user, password - a secret -, sslmode, options), as (keyword, value)
connection parameters, and the rules a resolved connection must meet. The dialect's facts come from DatabaseDialect.
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final
from urllib.parse import quote, unquote, urlsplit

from streamwright.core.io.queries import QueryPolicy, timeout_ms

__all__ = ["CREDENTIAL_OPTIONS", "STRUCTURED_KEYS", "DatabaseDialect", "Parameters", "connection_parameters",
           "connection_problems", "parse_connection", "password_forms"]

Parameters = list[tuple[str, Any]]
STRUCTURED_KEYS: Final = ("host", "port", "database", "user", "password", "sslmode", "options")
# what `options` never sets: a credential (the only one is `password`, a secret reference)
CREDENTIAL_OPTIONS: Final = ("password", "passfile", "sslpassword", "oauth_client_secret", "scram_client_key",
                             "scram_server_key")
_OWN_KEYS: Final = MappingProxyType({"host": "host", "port": "port", "dbname": "database", "database": "database",
                                     "user": "user", "sslmode": "sslmode"})
_KEYWORD_PAIR: Final = re.compile(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*('(?:[^'\\]|\\.)*'|[^\s']*)")
_CONTROL: Final = re.compile(r"[\x00-\x1f\x7f]")
_KEYWORD: Final = re.compile(r"^[a-z][a-z0-9_]*$")
_HOSTNAME: Final = re.compile(r"^[^\s/]+$")


@dataclass(frozen=True)
class DatabaseDialect:
    """
    What differs between attached databases, stated once by a connector's client.
      name                 the connector (messages, the sandbox's folder)
      extension            the DuckDB extension, also its secret and ATTACH type
      url_schemes          the schemes of a DSN URL (`postgresql://`, ...)
      default_port         the port of a connection that names none
      secret_options       (keyword, value) parameters -> [(SECRET PARAMETER, value)]; ConnectError for one it cannot
      key_aliases          a DSN keyword's name in the parameters (e.g. dbname -> database)
      connection_options   the parameters a structured connection's `options` may set; empty: any name passes through
                           to the driver (§6b), a closed set bounds `options` to what the engine's secret can take
      identifier_limit     the longest schema, table or column name, in bytes
      policy               what a source query may use
      settings             [(DuckDB setting, SQL value)] the source reader applies before attaching
      server_timeout       (parameters, milliseconds) -> parameters with a timeout the server applies, or None: the
                           reader interrupts a statement itself
    """
    name: str
    extension: str
    url_schemes: tuple[str, ...]
    default_port: int
    secret_options: Callable[[Parameters], Parameters]
    key_aliases: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({"dbname": "database"}))
    connection_options: tuple[str, ...] = ()
    identifier_limit: int | None = None
    policy: QueryPolicy = field(default_factory=QueryPolicy)
    settings: tuple[tuple[str, str], ...] = ()
    server_timeout: Callable[[Parameters, int], Parameters] | None = None


def parse_connection(dsn: str, url_schemes: Sequence[str], aliases: Mapping[str, str] | None = None) -> Parameters:
    """
    The (keyword, value) pairs of a DSN: a URL (`scheme://user:password@host:port/database?key=value`, percent-decoded;
    the scheme in `url_schemes`) or libpq-style `key=value` pairs (values may be single-quoted, with \\ escapes).
    `aliases` renames keywords. ValueError (never quoting the DSN) for a DSN that is neither.
    """
    text = dsn.strip()
    aliases = aliases or {}
    if re.match(r"^(%s)://" % "|".join(re.escape(scheme) for scheme in url_schemes), text, re.IGNORECASE):
        pairs = _url_pairs(text, url_schemes[0])
    else:
        pairs = _keyword_pairs(text, url_schemes[0])
    return [(aliases.get(key.lower(), key.lower()), value) for key, value in pairs]


def _url_pairs(text: str, scheme: str) -> Parameters:
    try:
        parts = urlsplit(text)
        port = parts.port
    except ValueError:
        raise ValueError("the connection string is not a valid %s:// URL" % scheme) from None
    pairs = [(key, value) for key, value in (
        ("host", parts.hostname), ("port", None if port is None else str(port)),
        ("user", None if parts.username is None else unquote(parts.username)),
        ("password", None if parts.password is None else unquote(parts.password)),
        ("dbname", unquote(parts.path.lstrip("/")))) if value]
    for item in parts.query.split("&"):
        key, _, value = item.partition("=")
        if key:
            pairs.append((unquote(key), unquote(value)))
    return pairs


def _keyword_pairs(text: str, scheme: str) -> Parameters:
    pairs, position = [], 0
    while position < len(text):
        match = _KEYWORD_PAIR.match(text, position)
        if not match:
            raise ValueError("the connection string is neither a %s:// URL nor key=value pairs" % scheme)
        key, value = match.groups()
        if value.startswith("'"):
            value = re.sub(r"\\(.)", r"\1", value[1:-1])
        pairs.append((key, value))
        position = match.end()
    return pairs


def password_forms(dsn: str, url_schemes: Sequence[str]) -> list[str]:
    """The passwords a DSN holds (a URL's, or a `password=` keyword's), as written and decoded: to redact."""
    try:
        found = [form for key, value in parse_connection(dsn, url_schemes) if key == "password" and value
                 for form in (value, quote(value, safe=""))]
    except ValueError:
        found = []
    found += [match.group(1) for match in re.finditer(r"(?:^|\s)password\s*=\s*('(?:[^'\\]|\\.)*'|[^\s']+)", dsn,
                                                      re.IGNORECASE)]
    return [value for value in dict.fromkeys(found) if value]


def connection_parameters(dialect: DatabaseDialect, connection: Mapping[str, Any]) -> Parameters:
    """The (keyword, value) connection parameters of a resolved connection: its `dsn`, or its structured keys."""
    if connection.get("dsn"):
        return parse_connection(connection["dsn"], dialect.url_schemes, dialect.key_aliases)
    pairs: Parameters = [("host", connection["host"]),
                         ("port", connection.get("port") or dialect.default_port),
                         ("database", connection["database"]), ("user", connection["user"])]
    pairs += [(key, connection[key]) for key in ("password", "sslmode") if connection.get(key) not in (None, "")]
    vendor = list(dict(getattr(connection, "vendor", {}) or {}).items())  # undeclared libpq/mysql connection options
    return pairs + list((connection.get("options") or {}).items()) + vendor


def _host_problem(host: str) -> str | None:
    """
    Why `host` is no host libpq-style clients accept, or None: a hostname (international names too), an IPv4 or IPv6
    address (bracketed too) or an absolute unix socket directory. No whitespace, NUL or control character.
    """
    if host.startswith("/"):
        return None
    candidate = host[1:-1] if host.startswith("[") and host.endswith("]") else host
    try:
        ipaddress.ip_address(candidate)
    except ValueError:
        if not _HOSTNAME.match(host):
            return "`host` must be a hostname, an IP address or an absolute socket directory: %r" % host
    return None


def _option_problem(dialect: DatabaseDialect, key: Any, value: Any) -> str | None:
    if not isinstance(key, str) or not _KEYWORD.match(key):
        return "`options` key %r is not a parameter name (lowercase letters, digits and _)" % (key,)
    if key in CREDENTIAL_OPTIONS:
        return "`options` cannot set `%s`: the only credential is `password`, a secret reference" % key
    if key == "dsn":
        return "`options` cannot set `dsn`: a DSN is the connection key `dsn` (a secret)"
    if key in _OWN_KEYS:
        return "`options` cannot set `%s`: use the connection key `%s`" % (key, _OWN_KEYS[key])
    # contract v1.3 §6b: a dialect that declares no `connection_options` accepts any parameter name - unknown options
    # pass through to the driver, which is the authority (e.g. a new libpq parameter needs no code change here). A
    # dialect that declares a closed set (one bounded by the engine, e.g. DuckDB's mysql secret) still enforces it.
    if dialect.connection_options and key not in dialect.connection_options:
        return "`options` key `%s` is not a connection parameter it may set (%s)" % (
            key, ", ".join(dialect.connection_options) or "none")
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return "`options.%s` must be text (or a whole number), e.g. \"10\"" % key
    if "\x00" in str(value):
        return "`options.%s` cannot hold a NUL character" % key
    return None


def _structured_problems(dialect: DatabaseDialect, connection: Mapping[str, Any]) -> list[str]:
    problems = ["needs `%s` (or `dsn`)" % key for key in ("host", "database", "user") if key not in connection]
    for key in ("host", "database", "user"):
        if key in connection and _CONTROL.search(str(connection[key])):
            problems.append("`%s` cannot hold a NUL or control character" % key)
        elif key in connection and not str(connection[key]).strip():
            problems.append("`%s` is empty" % key)
    if connection.get("host") and not _CONTROL.search(str(connection["host"])):
        problems += [problem for problem in [_host_problem(str(connection["host"]))] if problem]
    port = connection.get("port")
    if port is not None and not 1 <= port <= 65535:
        problems.append("`port` must be a port number (1-65535), e.g. %d; not %r" % (dialect.default_port, port))
    problems += [problem for key, value in (connection.get("options") or {}).items()
                 for problem in [_option_problem(dialect, key, value)] if problem]
    return problems


def connection_problems(dialect: DatabaseDialect, connection: Mapping[str, Any]) -> list[str]:
    """
    Why a resolved connection cannot be used: either `dsn` or the structured keys (`host`, `database`, `user`, then
    optionally `port`, `password`, `sslmode`, `options`), not both; a DSN that parses; a `statement_timeout` that is a
    timeout. Messages never quote a secret.
    """
    structured = [key for key in STRUCTURED_KEYS if key in connection]
    if "dsn" in connection:
        problems = ["use either `dsn` or the connection keys, not both: %s" % ", ".join(
            "`%s`" % key for key in structured)] if structured else []
        try:
            parse_connection(connection["dsn"], dialect.url_schemes)
        except ValueError as exc:
            problems.append("`dsn`: %s" % exc)
    else:
        problems = _structured_problems(dialect, connection)
    if connection.get("statement_timeout") is not None:
        try:
            timeout_ms(connection["statement_timeout"])
        except ValueError as exc:
            problems.append("`statement_timeout`: %s" % exc)
    return problems
