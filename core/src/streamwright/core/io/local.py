"""
Where local files are (the counterpart of `locations` for URLs): a LocalLocation holds the roots of one connection
(real paths) and says which files a path, a glob or a folder with a `match` regex names - BEFORE anything is read:
no `..`, every file inside a root once symbolic links are followed, and no glob character (*, ?, [) in its real path
(DuckDB would read it as a glob, not by name). Relative paths are relative to the first root. Pure functions and
frozen values; nothing here touches DuckDB.
"""

from __future__ import annotations

import glob
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Final

from streamwright.core.io.file_formats import AccessError, ReadError, regex_problem
from streamwright.core.io.locations import is_url

__all__ = ["LOCAL_GLOB_CHARACTERS", "OBJECT_STORAGE", "LocalLocation", "is_local_glob", "local_path_problem",
           "real_roots"]

OBJECT_STORAGE: Final = "local files only (object storage has its own connectors)"
LOCAL_GLOB_CHARACTERS: Final = "*?["


def is_local_glob(text: str) -> bool:
    return any(character in text for character in LOCAL_GLOB_CHARACTERS)


def local_path_problem(path: Any) -> str | None:
    """Why a (rendered or not) file path cannot be read, or None: empty, a URL, or going up a folder (..)."""
    if not isinstance(path, str) or not path.strip():
        return "is empty"
    if "\x00" in path:
        return "has a NUL character"
    if is_url(path):
        return "is a URL: %s" % OBJECT_STORAGE
    if ".." in re.split(r"[/\\]", path):
        return "goes up a folder (..)"
    return None


def real_roots(directories: Iterable[Any]) -> tuple[str, ...]:
    """
    The folders files can be read from, each as its real path (duplicates once); AccessError for one that is not an
    existing local folder (a URL included).
    """
    roots: list[str] = []
    for directory in directories or ():
        if not isinstance(directory, str) or not directory.strip():
            raise AccessError("roots: %r is not a folder" % (directory,))
        if is_url(directory):
            raise AccessError("roots: %r is a URL: roots are local folders; %s" % (directory, OBJECT_STORAGE))
        if not os.path.isdir(directory):
            raise AccessError("roots: %s: no such folder" % directory)
        root = os.path.realpath(directory)
        if root not in roots:
            roots.append(root)
    if not roots:
        raise AccessError("roots: no folder to read files from")
    return tuple(roots)


def _is_inside(root: str, path: str) -> bool:
    return path == root or path.startswith(root.rstrip(os.sep) + os.sep)


def _fixed_part(path: str) -> str:
    """The part of a path before its first glob character, up to the last folder separator before it."""
    if not is_local_glob(path):
        return path
    first = min(path.index(character) for character in LOCAL_GLOB_CHARACTERS if character in path)
    return os.path.dirname(path[:first]) or (os.sep if path.startswith(os.sep) else ".")


def _walk_error(exc: OSError) -> None:
    raise ReadError("cannot list %s: %s" % (exc.filename, exc.strerror or exc))


@dataclass(frozen=True)
class LocalLocation:
    """The roots of one connection (real paths; AccessError for a bad one): every path resolves inside them."""
    roots: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "roots", real_roots(self.roots))

    def root_of(self, path: str) -> str | None:
        return next((root for root in self.roots if _is_inside(root, path)), None)

    def _outside(self, path: Any) -> AccessError:
        return AccessError("path %r is outside the roots files can be read from (%s)" % (path, ", ".join(self.roots)))

    def _checked(self, path: str, found: str, files: list[tuple[str, str]]) -> str:
        """
        Adds the file `found` (for `path`) to `files` as (real path, root) once its real path is in a root;
        AccessError for one whose real path has a glob character (DuckDB would read other files in its place).
        """
        real = os.path.realpath(found)
        root = self.root_of(real)
        if root is None:
            raise AccessError("path %r: %s leads outside the roots files can be read from (%s) through a symbolic "
                              "link" % (path, found, ", ".join(self.roots)))
        if os.path.isfile(real):  # (a broken link is no file)
            if is_local_glob(real):
                raise AccessError("path %r: the file %s has a glob character (*, ? or [) in its path: it cannot be "
                                  "read by name" % (path, real))
            if (real, root) not in files:
                files.append((real, root))
        return real

    def check(self, location: str) -> None:
        if is_local_glob(location):
            raise AccessError("%s has a glob character (*, ? or [) in its path: it cannot be read by name" % location)

    def resolve_files(self, path: str) -> list[tuple[str, str]]:
        """
        [(real path, root)] of the files a rendered path names, in name order: the file, or each file a glob (*, ?,
        [ab], ** for any folders) matches; none when nothing matches. Checked before anything is read (AccessError):
        no `..`, the path's fixed part inside a root and, once symbolic links are followed, every file too. A path that
        names a folder is a ReadError.
        """
        problem = local_path_problem(path)
        if problem:
            raise AccessError("path %r %s" % (path, problem))
        full = path if os.path.isabs(path) else os.path.join(self.roots[0], path)
        if self.root_of(os.path.realpath(_fixed_part(full))) is None:
            raise self._outside(path)
        pattern = is_local_glob(path)
        if pattern:
            written = path if os.path.isabs(path) else os.path.join(glob.escape(self.roots[0]), path)
            matches = sorted(glob.glob(written, recursive=True))
        else:
            matches = [full] if os.path.lexists(full) else []
        files: list[tuple[str, str]] = []
        for found in matches:
            real = self._checked(path, found, files)
            if os.path.isdir(real) and not pattern:
                raise ReadError("path %r is a folder: name its files, e.g. %s, or set `match`" % (
                    path, os.path.join(path, "*.csv")))
        return files

    def match_files(self, path: str, pattern: str, recursive: bool = False) -> list[tuple[str, str]]:
        """
        [(real path, root)] of the files in the folder `path` names whose path relative to it (sub-folders separated
        by `/`) fully matches the regex `pattern`, in the order of those relative paths; `recursive`: the files of
        its sub-folders too (symbolic links to folders are not followed). None when the folder does not exist or
        nothing matches. AccessError before anything is read; a `path` that is a glob or a file, or a pattern that is
        no valid regex, is a ReadError.
        """
        problem = local_path_problem(path)
        if problem:
            raise AccessError("path %r %s" % (path, problem))
        if is_local_glob(path):
            raise ReadError("path %r is a glob: with `match`, `path` is the folder to look in" % path)
        problem = regex_problem(pattern)
        if problem:
            raise ReadError("`match` %r %s" % (pattern, problem))
        regex = re.compile(pattern)
        full = path if os.path.isabs(path) else os.path.join(self.roots[0], path)
        folder = os.path.realpath(full)
        if self.root_of(folder) is None:
            raise self._outside(path)
        if not os.path.exists(folder):
            return []
        if not os.path.isdir(folder):
            raise ReadError("path %r is not a folder: with `match`, `path` is the folder to look in" % path)
        matches = []
        for current, folders, names in os.walk(folder, onerror=_walk_error):
            if not recursive:
                folders[:] = []
            for name in names:
                relative = os.path.relpath(os.path.join(current, name), folder).replace(os.sep, "/")
                if regex.fullmatch(relative):
                    matches.append((relative, os.path.join(current, name)))
        files: list[tuple[str, str]] = []
        for _, found in sorted(matches):
            self._checked(path, found, files)
        return files
