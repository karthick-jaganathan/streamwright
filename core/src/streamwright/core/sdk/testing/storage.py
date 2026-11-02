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
"""An offline stand-in for an object store (ObjectStorage) and a recorder of its calls (Listener)."""

import fnmatch
import os
import socket
import threading
import time

import duckdb

from streamwright.core.io.locations import is_url

# * ----------------------------------------------------
# * object stores, offline
# * ----------------------------------------------------

class _Result:
    def __init__(self, rows):
        self.rows = list(rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows

    def fetchmany(self, size):
        rows, self.rows = self.rows[:size], self.rows[size:]
        return rows


def _glob_match(url, pattern):
    """Whether DuckDB's object-storage glob `pattern` matches `url`: `*` and `[ab]` within a folder, `**` any."""
    def match(keys, patterns):
        if not patterns:
            return not keys
        if patterns[0] == "**":
            return any(match(keys[index:], patterns[1:]) for index in range(len(keys) + 1))
        return bool(keys) and fnmatch.fnmatchcase(keys[0], patterns[0]) and match(keys[1:], patterns[1:])
    return match(url.split("/"), pattern.split("/"))


class ObjectStorage:
    """
    What the Reader connections of a test see: whether the extension is installed, the objects (URL -> a local file),
    forced errors and what was run. `errors` maps "INSTALL", "LOAD", "SECRET", "GLOB" or "READ" to the exception the
    statement raises; `statements` holds every (statement, parameters) of a Reader connection, in order; `listed` the
    globs listed; `unsorted` makes listings come back in reverse order with repeated entries, as a store may.
    """

    def __init__(self, connect, store, extension):
        self.connect = connect  # the real duckdb.connect
        self.store = store
        self.extension = extension
        self.installed = True
        self.objects = {}
        self.errors = {}
        self.statements = []
        self.listed = []
        self.unsorted = False
        self._plain = None

    def plain(self):
        if self._plain is None:
            self._plain = self.connect()
            self._plain.execute("SET TimeZone = 'UTC'")
        return self._plain

    def sql(self):
        return [statement for statement, _ in self.statements]

    def reads(self):
        return [statement for statement in self.sql() if any(name in statement for name in (
            "read_csv", "read_json", "read_parquet", "glob("))]

    def serve(self, folder, prefix):
        """Serves every file under a local folder as an object: prefix + its path relative to the folder."""
        for current, _, names in os.walk(str(folder)):
            for name in names:
                path = os.path.join(current, name)
                self.objects[prefix + os.path.relpath(path, str(folder)).replace(os.sep, "/")] = path


def patch_object_storage(monkeypatch, store, extension="httpfs"):
    """
    An ObjectStorage the test's object store readers use: each reader's DuckDB connection (its temp_directory names
    `streamwright-<provider>-`) becomes a recording stand-in; every other connection stays DuckDB's. `store` is what
    names the provider: an ObjectStore (streamwright.core.io.files).
    """
    state = ObjectStorage(duckdb.connect, store, extension)
    marker = "streamwright-%s-" % store.provider

    def connect(*args, **kwargs):
        connection = state.connect(*args, **kwargs)
        ours = marker in str((kwargs.get("config") or {}).get("temp_directory"))
        return _Recorder(connection, state) if ours else connection
    monkeypatch.setattr(duckdb, "connect", connect)
    return state


class _Recorder:
    """
    A Reader's DuckDB connection: a real one, except that extension and secret statements are recorded, not run, globs
    over URLs list ObjectStorage.objects, and reads of URLs read their local files (on a connection without the
    Reader's limits: the Reader's own allows URLs only).
    """

    def __init__(self, real, storage):
        self.duckdb = duckdb
        self.real = real
        self.storage = storage

    def _fail(self, kind):
        error = self.storage.errors.get(kind)
        if error is not None:
            raise error

    def execute(self, query, parameters=None):
        storage = self.storage
        sql = " ".join(query.split())
        storage.statements.append((sql, parameters))
        if "duckdb_extensions()" in sql and parameters and parameters[0] == storage.extension:
            return _Result([(storage.installed,)])
        for start, kind in (("INSTALL %s" % storage.extension, "INSTALL"), ("LOAD %s" % storage.extension, "LOAD"),
                            ("CREATE OR REPLACE TEMPORARY SECRET", "SECRET")):
            if sql.startswith(start):
                self._fail(kind)
                return _Result([])
        if "FROM glob(?)" in sql:
            self._fail("GLOB")
            storage.listed.append(parameters[0])
            urls = [url for url in sorted(storage.objects) if _glob_match(url, parameters[0])]
            if storage.unsorted:
                urls = list(reversed(urls)) + urls[:1]
            return _Result([(url,) for url in urls])
        first = parameters[0] if parameters else None
        if isinstance(first, list) and first and is_url(first[0]):
            self._fail("READ")
            if first[0] not in storage.objects:
                raise self.duckdb.HTTPException("HTTP Error: HTTP GET error reading '%s' (HTTP 404 Not Found)" %
                                                first[0])
            return storage.plain().execute(query, [[storage.objects[first[0]]], *list(parameters[1:])])
        return self.real.execute(query) if parameters is None else self.real.execute(query, parameters)

    def close(self):
        self.real.close()


class Listener:
    """A local HTTP listener on 127.0.0.1 that records the requests made to it and answers 404 (nothing leaves)."""

    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.listen(5)
        self.socket.settimeout(0.2)
        self.port = self.socket.getsockname()[1]
        self.requests = []
        self.running = True
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        while self.running:
            try:
                connection, _ = self.socket.accept()
            except (TimeoutError, OSError):
                continue
            try:
                connection.settimeout(2)
                self.requests.append(connection.recv(4096))
                connection.sendall(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            except OSError:
                pass
            finally:
                connection.close()

    def connected(self):
        time.sleep(0.3)
        return bool(self.requests)

    def close(self):
        self.running = False
        self.thread.join(2)
        self.socket.close()


BIG = "12345678901234567890.123456"


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return str(path)


def _parquet(path, select):
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute("COPY (%s) TO '%s' (FORMAT parquet)" % (select, path))
    finally:
        connection.close()
    return str(path)
