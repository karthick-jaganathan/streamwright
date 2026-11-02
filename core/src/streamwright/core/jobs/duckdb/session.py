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
The DuckDB session of one job run: an in-memory database where step views and tables live as TEMP objects, with
every connection set up by its adapter (attached catalogs, scoped secrets, extensions).

All step and write SQL runs on `con`, so `runtime.timeout_seconds` can interrupt it. Other threads (the lease
heartbeat's state store) use their own cursors from `connection_factory()`.
"""

import logging
import os
import threading
import time

import duckdb

from streamwright.core.sql.quote import quote_ident, quote_literal

__all__ = ["Session", "SessionError"]

LOG = logging.getLogger("streamwright.duckdb")


class SessionError(Exception):
    """A connection cannot be set up, or the run exceeded runtime.timeout_seconds."""


class Session:
    def __init__(self, runtime=None, writable=(), base_dir=None):
        """
        `writable`: the connection names something writes to (outputs, the state store); `base_dir`: the job's
        folder, which relative local paths of connections start at (the working directory when None).
        """
        self.runtime = runtime or {}
        self.base_dir = base_dir
        self.writable = set(writable)
        self.con = duckdb.connect(":memory:")
        self._state_con = None
        self._attached = {}
        self._deadline = None
        self._timer = None
        try:
            self._configure_runtime()
        except Exception:
            self.close()
            raise

    def _configure_runtime(self):
        if self.runtime.get("threads"):
            self.con.execute("SET threads = %d" % int(self.runtime["threads"]))
        if self.runtime.get("memory_limit"):
            self.con.execute("SET memory_limit = %s" % quote_literal(self.runtime["memory_limit"]))
        if self.runtime.get("temp_directory"):
            self.con.execute("SET temp_directory = %s" % quote_literal(self.runtime["temp_directory"]))

    # --- what adapters use ----------------------------------------------------------------------------------

    def is_writable(self, name):
        return name in self.writable

    def local_path(self, path):
        """A local path of a connection, relative ones starting at the job's folder."""
        return path if os.path.isabs(path) else os.path.join(self.base_dir or os.getcwd(), path)

    def load_extension(self, name):
        """Loads (installing when needed) a DuckDB extension; raises SessionError when it is unavailable."""
        if not name.replace("_", "").isalnum():
            raise SessionError("invalid extension name %r" % name)
        try:
            self.con.execute("LOAD %s" % name)
        except duckdb.Error:
            try:
                self.con.execute("INSTALL %s" % name)
                self.con.execute("LOAD %s" % name)
            except duckdb.Error as exc:
                raise SessionError("cannot load DuckDB extension %r: %s" % (name, exc)) from None

    def create_secret(self, name, kind, options, scope=None):
        """
        A temporary secret for connection `name` (`options`: [(KEY, value)], None values skipped; values are
        literals, never SQL text). `scope` limits it to a URL prefix, so connections never share credentials.
        """
        secret = quote_ident("streamwright_%s" % name)
        params = ["TYPE %s" % quote_ident(kind)]
        for key, value in options:
            if value is None or value == "":
                continue
            if not key.replace("_", "").isalnum():
                raise SessionError("invalid secret option %r" % key)
            rendered = str(value).lower() if isinstance(value, bool) else (
                str(value) if isinstance(value, int) else quote_literal(value))
            params.append("%s %s" % (key, rendered))
        if scope:
            params.append("SCOPE %s" % quote_literal(scope))
        self.con.execute("CREATE OR REPLACE TEMPORARY SECRET %s (%s)" % (secret, ", ".join(params)))
        return secret

    def attach(self, name, path, kind=None, secret=None, read_only=False):
        """ATTACH `path` as catalog `name` (`kind`: an extension's database type; `secret`: from create_secret)."""
        options = []
        if kind:
            options.append("TYPE %s" % quote_ident(kind))
        if secret:
            options.append("SECRET %s" % secret)
        if read_only:
            options.append("READ_ONLY")
        suffix = " (%s)" % ", ".join(options) if options else ""
        try:
            self.con.execute("ATTACH %s AS %s%s" % (quote_literal(path), quote_ident(name), suffix))
        except duckdb.Error as exc:
            raise SessionError("cannot attach connection %r: %s" % (name, exc)) from None
        self._attached[name] = kind or "duckdb"

    def is_attached(self, name):
        return name in self._attached

    def connection_factory(self):
        """A callable returning a new cursor (its own transaction) on this session's database instance."""
        if self._state_con is None:
            self._state_con = self.con.cursor()
        return self._state_con.cursor

    # --- run limits -----------------------------------------------------------------------------------------

    def start_clock(self):
        """Starts runtime.timeout_seconds: running SQL is interrupted, and check_clock raises, once it is over."""
        timeout = self.runtime.get("timeout_seconds")
        if timeout:
            self._deadline = time.monotonic() + float(timeout)
            self._timer = threading.Timer(float(timeout), self.con.interrupt)
            self._timer.daemon = True
            self._timer.start()

    def check_clock(self):
        if self._deadline is not None and time.monotonic() >= self._deadline:
            raise SessionError("the job ran longer than runtime.timeout_seconds (%s)" % self.runtime["timeout_seconds"])

    def close(self):
        if self._timer is not None:
            self._timer.cancel()
        for con in (self._state_con, self.con):
            if con is not None:
                try:
                    con.close()
                except duckdb.Error as exc:
                    LOG.warning("closing the DuckDB session failed: %s", exc)
