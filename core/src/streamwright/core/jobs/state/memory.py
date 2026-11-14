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

"""An in-memory SQLite state store (`sqlite: ":memory:"`): one database that lives as long as the store."""

import sqlite3
import threading

__all__ = ["memory_connect"]


class _Borrowed:
    """The store's one connection, lent to one operation at a time: close() returns it instead of closing it."""

    def __init__(self, conn, lock):
        self._conn, self._lock = conn, lock

    def execute(self, *args):
        return self._conn.execute(*args)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._lock.release()


def memory_connect():
    """
    A connect() for SqlStateStore over one in-memory database: every sqlite3.connect(":memory:") is a new, empty
    database, so the store keeps one connection and lends it to an operation at a time (the lease heartbeat runs in
    another thread).
    """
    conn = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
    lock = threading.Lock()

    def connect():
        lock.acquire()
        return _Borrowed(conn, lock)

    return connect
