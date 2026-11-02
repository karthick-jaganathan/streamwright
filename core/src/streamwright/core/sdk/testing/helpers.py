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
"""Helpers for connector tests: a registered connector, installed DuckDB extensions, the CLI entry."""

import contextlib

import duckdb

from streamwright.core import cli
from streamwright.core.runtime import components

run_cli = cli.main


@contextlib.contextmanager
def registered(connector):
    """The connector registered in this process (as an installed one would be) for the length of the block."""
    components.register(connector)
    try:
        yield connector
    finally:
        components.unregister(connector.name)


def extension_installed(name):
    """Whether DuckDB's extension `name` is installed here (tests that need the real one skip otherwise)."""
    connection = duckdb.connect()
    try:
        row = connection.execute("SELECT installed FROM duckdb_extensions() WHERE extension_name = ?", [name]
                                 ).fetchone()
        return bool(row and row[0])
    finally:
        connection.close()


def _problems_with(texts, values):
    return [value for value in values if any(value in text for text in texts)]
