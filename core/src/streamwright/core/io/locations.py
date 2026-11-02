"""
Where files are: an object store's facts (ObjectStore), the URL rules every path, root and listed object must meet,
and the FileLocation of one connection - its roots, inside which every path resolves (a key relative to the first
root, or a URL of the store's scheme). Pure functions and frozen values; nothing here touches DuckDB or the network.

A URL is refused when DuckDB could read it as more than an object's key: a `?` (DuckDB reads a URL's query
parameters as connection settings, e.g. `?s3_endpoint=`, that override the configured ones - so `?` is never a glob
character here, only `*` and `[` are), a `%` (percent-encoding could hide `?`, `.`, `/` or `\\`), a `#`, a backslash
or a control character; a `..`, `.` or empty (`//`) folder; user@ or a port in the bucket.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final

from streamwright.core.io.file_formats import AccessError, ReadError, regex_problem

__all__ = ["CONTAINER_PATTERN", "GLOB_CHARACTERS", "NOT_FOUND", "FileLocation", "ObjectStore", "allowed_roots",
           "as_flag", "is_glob", "is_inside", "is_url", "path_problem", "root_problems", "secret_parameters",
           "url_problem"]

CONTAINER_PATTERN: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-]*$")
NOT_FOUND: Final = re.compile(r"\bHTTP 404\b|No files found that match the pattern|\bNoSuchKey\b", re.IGNORECASE)
GLOB_CHARACTERS: Final = "*["
_CONTROL: Final = re.compile(r"[\x00-\x1f\x7f]")
_URL: Final = re.compile(r"^([A-Za-z][A-Za-z0-9+.\-]*)://")

Lister = Callable[[str], Iterable[str]]


@dataclass(frozen=True)
class ObjectStore:
    """
    provider           the connector's name (messages, the reader's secret name)
    schemes            URL schemes it reads; the first is the primary one
    secret_type        DuckDB's secret type (s3, gcs, azure, ...)
    credentials        {connection key: DuckDB secret parameter} of the secret fields, in parameter order
    settings           {connection key: DuckDB secret parameter} of the plain settings, in parameter order
    flags              settings that are booleans (bound as booleans)
    extension          the DuckDB extension that reads it
    container_noun     what the first URL segment is called (bucket, container) and what it may look like
    not_found          a regex over DuckDB's message for a URL that names no object
    build_secret       builds the secret's parameters from the connection when the declared mapping cannot say
                       (azure: a connection string, a service principal): connection -> [(parameter, value)]
    secret_parts       the credentials derived from the connection's (azure: a connection string's AccountKey)
                       that messages must not show either: connection -> [text]
    """
    provider: str
    schemes: tuple[str, ...]
    secret_type: str
    credentials: Mapping[str, str]
    settings: Mapping[str, str]
    flags: frozenset[str] = frozenset()
    extension: str = "httpfs"
    container_noun: str = "bucket"
    container_pattern: re.Pattern[str] = CONTAINER_PATTERN
    not_found: re.Pattern[str] = NOT_FOUND
    build_secret: Callable[[Mapping[str, Any]], list[tuple[str, Any]]] | None = None
    secret_parts: Callable[[Mapping[str, Any]], list[str]] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "schemes", tuple(self.schemes))
        object.__setattr__(self, "credentials", MappingProxyType(dict(self.credentials)))
        object.__setattr__(self, "settings", MappingProxyType(dict(self.settings)))
        object.__setattr__(self, "flags", frozenset(self.flags))

    @property
    def scheme(self) -> str:
        return self.schemes[0]

    def schemes_text(self) -> str:
        return " or ".join("%s://" % scheme for scheme in self.schemes)

    def containers(self, roots: Iterable[Any]) -> tuple[str, ...]:
        """The buckets (containers) of URL roots, sorted, each once; a root that is no URL of this store is skipped."""
        return tuple(sorted({_split_url(root)[1] for root in roots or () if is_url(root)
                             and _split_url(root)[0] in self.schemes}))

    def masked_values(self, connection: Mapping[str, Any]) -> list[str]:
        """The credentials of a connection (and what is derived from them) that no message may show."""
        values = [str(connection[key]) for key in self.credentials if connection.get(key) is not None]
        values += [str(value) for parameter, value in secret_parameters(self, connection)
                   if parameter in self.credentials.values()]
        values += self.secret_parts(connection) if self.secret_parts is not None else []
        return values


# * ----------------
# * the URL rules
# * ----------------

def is_url(path: Any) -> bool:
    """Whether a path is a URL (scheme://...)."""
    return isinstance(path, str) and bool(_URL.match(path))


def is_glob(text: str) -> bool:
    return any(character in text for character in GLOB_CHARACTERS)


def _split_url(url: str) -> tuple[str, str, str]:
    """(scheme in lower case, container, key: the rest after the container's `/`) of a URL."""
    scheme = _URL.match(url).group(1)
    container, _, key = url[len(scheme) + 3:].partition("/")
    return scheme.lower(), container, key


def _normalize(url: str) -> str:
    """A URL with its scheme in lower case (containers and keys are case-sensitive: kept as written)."""
    return "%s://%s/%s" % _split_url(url)


def _text_problem(text: str) -> str | None:
    """Why a URL or key holds more than an object's name (see the module documentation), or None."""
    if "?" in text:
        return ("has a `?`: object storage URLs take no query - DuckDB would read its parameters (e.g. "
                "?s3_endpoint=, ?s3_access_key_id=) as connection settings; `?` is not a glob character here")
    if "%" in text:
        return "has a `%`: percent-encoded characters (e.g. %3f, %2e, %2f, %5c) are refused"
    if "#" in text:
        return "has a fragment (#)"
    if "\\" in text:
        return "has a backslash"
    if _CONTROL.search(text):
        return "has a control character"
    return None


def _key_problem(key: str) -> str | None:
    """Why the key part of a URL (or a key relative to a root) cannot be read, or None: `..`, `.` or `//`."""
    segments = key.split("/")
    if ".." in segments:
        return "goes up a folder (..)"
    if "." in segments:
        return "has a `.` folder"
    if "" in segments[:-1]:
        return "has an empty folder (//)"
    return None


def _container_problem(store: ObjectStore, container: str) -> str | None:
    noun = store.container_noun
    if "@" in container:
        return "has credentials in it (user@%s): set them in the auth block, from secrets" % noun
    if not container:
        return "names no %s" % noun
    if "{{" not in container:
        if ":" in container:
            return "has a port: name the %s only (an endpoint is set in the auth block)" % noun
        if not store.container_pattern.match(container):
            return "has a %s name that is not valid: %r" % (noun, container)
    return None


def url_problem(store: ObjectStore, url: Any, *, root: bool = False) -> str | None:
    """
    Why a (rendered or not) URL cannot be read - or, `root`, be a root - or None: another scheme, a bad container,
    what _text_problem or _key_problem refuse; a root that is a glob.
    """
    if not is_url(url):
        return "is not a %s URL" % store.schemes_text()
    scheme, container, key = _split_url(url)
    if scheme not in store.schemes:
        return "uses the %s:// scheme: the %s connector reads %s URLs only" % (scheme, store.provider,
                                                                               store.schemes_text())
    problem = _text_problem(url) or _container_problem(store, container) or _key_problem(key)
    if problem is None and root and is_glob(key):
        return "is a glob: a root is a folder (a URL prefix)"
    return problem


def path_problem(store: ObjectStore, path: Any) -> str | None:
    """
    Why a (rendered or not) `path` cannot be read, or None: empty, a URL url_problem() refuses, or a key relative
    to the first root that is absolute, another scheme's URL, or holds what a key may not.
    """
    if not isinstance(path, str) or not path.strip():
        return "is empty"
    if is_url(path):
        return url_problem(store, path)
    if "://" in path:
        return "is not a %s URL" % store.schemes_text()
    if path.startswith("/"):
        return "is an absolute local path: name a %s URL, or a key relative to the first root" % store.schemes_text()
    return _text_problem(path) or _key_problem(path)


def root_problems(store: ObjectStore, roots: Iterable[Any]) -> list[str]:
    """Why roots cannot be roots of this store (each one a URL prefix without query, glob or `..`)."""
    problems = []
    for index, root in enumerate(roots):
        problem = url_problem(store, root, root=True) if isinstance(root, str) else "is not a text"
        if problem:
            problems.append("roots[%d]: %r %s" % (index, root, problem))
    return problems


def _to_root_prefix(store: ObjectStore, root: str) -> str:
    problem = url_problem(store, root, root=True)
    if problem:
        raise AccessError("roots: %r %s" % (root, problem))
    prefix = _normalize(root)
    return prefix if prefix.endswith("/") else prefix + "/"


def allowed_roots(store: ObjectStore, roots: Iterable[Any]) -> tuple[str, ...]:
    """The URL prefixes (ending with `/`, duplicates once) files can be read from; AccessError for a bad root."""
    allowed: list[str] = []
    for root in roots or ():
        if not isinstance(root, str) or not root.strip():
            raise AccessError("roots: %r is not a %s URL prefix" % (root, store.schemes_text()))
        prefix = _to_root_prefix(store, root)
        if prefix not in allowed:
            allowed.append(prefix)
    if not allowed:
        raise AccessError("roots: no %s URL prefix to read objects from" % store.schemes_text())
    return tuple(allowed)


def is_inside(root: str, url: str) -> bool:
    """Whether a URL is inside a root prefix: the same scheme and container, its key under the prefix."""
    return is_url(url) and _normalize(url).startswith(root)


def as_flag(value: Any, name: str = "use_ssl") -> bool:
    """A boolean setting given as a boolean or as the text true/false; ValueError otherwise."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise ValueError("%s: expected true or false" % name)


def secret_parameters(store: ObjectStore, connection: Mapping[str, Any]) -> list[tuple[str, Any]]:
    """
    [(DuckDB secret parameter, value)] of the connection's credentials, then its settings, in declared order, then
    any undeclared connection key (a vendor argument, e.g. s3 `url_style`, `use_ssl`, `endpoint`) forwarded verbatim
    to the DuckDB secret (contract v1.2 §6a).
    """
    vendor = list(dict(getattr(connection, "vendor", {}) or {}).items())
    if store.build_secret is not None:
        return store.build_secret(connection) + vendor
    keys = list(store.credentials.items()) + list(store.settings.items())
    return [(parameter, as_flag(connection[key], key) if key in store.flags else str(connection[key]))
            for key, parameter in keys if connection.get(key) is not None] + vendor


# * ---------------------------
# * the location of a connection
# * ---------------------------

@dataclass(frozen=True)
class FileLocation:
    """
    The roots of one connection (normalized URL prefixes; AccessError for a bad one): every path resolves inside
    them - a URL of the store's scheme, or a key relative to the first root (as `streamwright.spark.io.files`).
    """
    store: ObjectStore
    roots: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "roots", allowed_roots(self.store, self.roots))

    def root_of(self, url: str) -> str | None:
        return next((root for root in self.roots if is_inside(root, url)), None)

    def _outside(self, path: Any) -> AccessError:
        return AccessError("path %r is outside the roots objects can be read from (%s)" % (
            path, ", ".join(self.roots)))

    def resolve_path(self, path: str, *, folder: bool = False) -> str:
        """The normal URL of a rendered path (a URL or a key relative to the first root), checked: AccessError."""
        problem = path_problem(self.store, path)
        url = path if is_url(path) else self.roots[0] + path
        problem = problem or url_problem(self.store, url)
        if problem:
            raise AccessError("path %r %s" % (path, problem))
        url = _normalize(url)
        if folder and not url.endswith("/"):
            url += "/"
        if self.root_of(url) is None:
            raise self._outside(path)
        return url

    def check(self, url: Any, *, glob: bool = False) -> None:
        """AccessError unless `url` is a URL url_problem() accepts inside a root (a glob only with `glob`)."""
        problem = url_problem(self.store, url) if isinstance(url, str) else "is not a %s URL" % (
            self.store.schemes_text())
        if problem:
            raise AccessError("%r %s" % (url, problem))
        if self.root_of(url) is None:
            raise self._outside(url)
        if not glob and is_glob(_split_url(url)[2]):
            raise AccessError("%r is a glob: read the objects resolve() lists" % url)

    def listed(self, path: Any, urls: Iterable[Any]) -> list[tuple[str, str]]:
        """
        [(URL, root)] of the objects a listing returned, in name order, each once, without folder markers; AccessError
        for any that url_problem() refuses, whose key has a glob character or that is outside every root.
        """
        files: list[tuple[str, str]] = []
        for found in sorted(set(urls)):
            problem = url_problem(self.store, found) if isinstance(found, str) else "is not a %s URL" % (
                self.store.schemes_text())
            if not problem and is_glob(_split_url(found)[2]):
                problem = "has a glob character (* or [) in its key: it cannot be read by name"
            if problem:
                raise AccessError("path %r: the listed object %r %s" % (path, found, problem))
            url = _normalize(found)
            root = self.root_of(url)
            if root is None:
                raise AccessError("path %r: the listed object %s is outside the roots objects can be read from "
                                  "(%s)" % (path, url, ", ".join(self.roots)))
            if not url.endswith("/") and (url, root) not in files:
                files.append((url, root))
        return files

    def resolve_files(self, path: str, lister: Lister | None = None) -> list[tuple[str, str]]:
        """
        [(URL, root)] of the objects a rendered path names, in name order: the URL itself (whether it names an object
        is known when it is read: MissingError), or each object a glob (`*`, `[ab]`, `**`) matches, which
        lister(url) lists. Checked before anything is read (AccessError); a URL that names a folder is a ReadError.
        """
        url = self.resolve_path(path)
        if not is_glob(_split_url(url)[2]):
            if url.endswith("/"):
                raise ReadError("path %r is a folder: name its objects, e.g. %s, or set `match`" % (
                    path, path + "*.csv"))
            return [(url, self.root_of(url))]
        if lister is None:
            raise ReadError("path %r is a glob: listing %s needs the reader" % (path, url))
        return self.listed(path, lister(url))

    def match_files(self, path: str, pattern: str, recursive: bool = False,
                    lister: Lister | None = None) -> list[tuple[str, str]]:
        """
        [(URL, root)] of the objects under the folder `path` names whose key relative to it fully matches the regex
        `pattern`, in that order; `recursive`: sub-folders too. lister(glob) lists them. AccessError, ReadError.
        """
        folder = self.resolve_path(path, folder=True)
        if is_glob(_split_url(folder)[2]):
            raise ReadError("path %r is a glob: with `match`, `path` is the folder to look in" % path)
        problem = regex_problem(pattern)
        if problem:
            raise ReadError("`match` %r %s" % (pattern, problem))
        if lister is None:
            raise ReadError("path %r: listing %s needs the reader" % (path, folder))
        regex = re.compile(pattern)
        files = []
        for found, root in self.listed(path, lister(folder + ("**" if recursive else "*"))):
            relative = found[len(folder):] if found.startswith(folder) else None
            if relative is not None and (recursive or "/" not in relative) and regex.fullmatch(relative):
                files.append((found, root))
        return files
