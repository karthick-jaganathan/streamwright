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
What the DuckDBJob engine asks of a connection's provider (`ProviderAdapter`), and the engine's own `duckdb` catalog.

The engine never names a provider: everything provider-specific is asked of its ProviderAdapter. Connectors are bound
to it by `definition_adapter.DefinitionDuckDBAdapter` (from their definition and their `duckdb` adapter); the
engine-native `duckdb` catalog is built in. `loading.load_adapters` finds both. This is engine-internal: connectors
implement the protocols of `streamwright.core.sdk.engines.duckdb`.

  connection_fields   {key: Field}: the connection keys it accepts (besides `provider`); also its JSON schema part
  secret_keys         keys whose value must be a `{{ secrets.NAME }}` reference
  reads               {read kind: step keys}: what a step's `read:` may be, and the keys that read uses
  holds_state         it can hold the job's bookkeeping state (runtime.state.connection)
  validate()          extra connection checks (no network)
  check_read()        extra checks of a read step's keys
  configure()         per-connection setup in the session: scoped CREATE SECRET, ATTACH (writable only when an
                      output targets the connection), extensions
  compile_read()      the SQL of a step's read
  write_target()      the WriteTarget of outputs through the connection (None: not writable)
  write()             one output's write; returns a WriteReceipt
  open_state()        the StateStore on this connection
"""

import os
from types import MappingProxyType

from streamwright.core.jobs.duckdb.reads import quote_relation
from streamwright.core.jobs.duckdb.writers.tables import TABLE_TARGET, write_table
from streamwright.core.jobs.fields import Field, json_schema_of
from streamwright.core.jobs.state import open_store

__all__ = [
    "BUILT_IN_ADAPTERS",
    "AdapterError",
    "DuckDBCatalogAdapter",
    "ProviderAdapter",
    "ReadContext",
    "WriteContext",
    "check_location",
]


class AdapterError(Exception):
    """An adapter cannot do what a step or output asks (a path outside the connection's roots, for example)."""


class ReadContext:
    """What compile_read may use: the session, the connection, the step, pinned values and file bookkeeping."""

    def __init__(self, session, name, connection, step, *, batch, pinned, limits, values, previous_signatures=None):
        self.session, self.name, self.connection, self.step = session, name, connection, step
        self.batch, self.pinned, self.limits, self.values = batch, pinned, limits, values
        self.previous_signatures = previous_signatures or {}
        self.file_errors, self.schema_reports, self.schema_signatures = [], [], []


class WriteContext:
    """What write may use: the session, the connection, the normalized output and the rendering inputs."""

    def __init__(self, session, name, connection, spec, *, source, batch, values, limits):
        self.session, self.name, self.connection, self.spec = session, name, connection, spec
        self.source = source                       # the quoted relation to write (a step or the output's view)
        self.batch, self.values, self.limits = batch, values, limits

    @property
    def constants(self):
        """The `{{ name }}` constants a predicate may use."""
        return {"batch.id": self.batch.batch_id, "batch.ts": self.batch.batch_ts}


class ProviderAdapter:
    """The engine-internal side of a provider; the defaults are a provider that offers nothing."""

    provider = None
    connection_fields = MappingProxyType({})
    secret_keys = ()
    reads = MappingProxyType({})
    holds_state = False

    def describe(self):
        """The adapter's declared contract (no connection is opened): fields, credentials, reads, state."""
        return {"provider": self.provider,
                "connection_fields": {key: json_schema_of(field) for key, field in self.connection_fields.items()},
                "secret_keys": list(self.secret_keys),
                "reads": {read: list(keys) for read, keys in self.reads.items()},
                "holds_state": self.holds_state}

    def validate(self, name, connection):
        """[(code, message, is_warning)] beyond the field rules."""
        return []

    def check_read(self, step):
        """[(code, message, is_warning)] of a read step's keys beyond the field rules."""
        return []

    def configure(self, session, name, connection):
        """Sets the connection up in the session."""

    def compile_read(self, context, step):
        raise AdapterError("%s connections cannot be read" % self.provider)

    def write_target(self, connection):
        return None

    def write(self, context):
        raise AdapterError("%s connections cannot be written to" % self.provider)

    def open_state(self, session, name, connection, schema):
        raise AdapterError("%s connections cannot hold bookkeeping state" % self.provider)


def check_location(path, roots, what="path"):
    """
    `path` (a URL, an absolute path, or relative to the first root) as a location inside one of `roots`; raises
    AdapterError for `..` or a location outside every root. Local locations are normalized.
    """
    if not isinstance(path, str) or not path or "\x00" in path:
        raise AdapterError("%s must be non-empty text" % what)
    if ".." in path.replace("\\", "/").split("/"):
        raise AdapterError("%s %r may not contain '..'" % (what, path))
    remote = "://" in path
    location = path if remote or os.path.isabs(path) else roots[0].rstrip("/") + "/" + path
    if "://" not in location:
        location = os.path.normpath(location)
    for root in roots:
        base = root.rstrip("/") if "://" in root else os.path.normpath(root)
        if location == base or location.startswith(base + "/"):
            return location
    raise AdapterError("%s %r is outside the connection's roots (%s)" % (what, path, ", ".join(roots)))


class DuckDBCatalogAdapter(ProviderAdapter):
    """A DuckDB database file (or `:memory:`), attached under the connection's name."""
    provider = "duckdb"
    connection_fields = MappingProxyType({"path": Field("string", required=True,
                                                        description="database file, or :memory:")})
    reads = MappingProxyType({"table": ("table",)})
    holds_state = True

    def configure(self, session, name, connection):
        path = connection["path"] if connection["path"] == ":memory:" else session.local_path(connection["path"])
        session.attach(name, path, read_only=not session.is_writable(name))

    def compile_read(self, context, step):
        return "SELECT * FROM %s" % quote_relation(context.name, step["table"])

    def write_target(self, connection):
        return TABLE_TARGET

    def write(self, context):
        return write_table(context, context.name, merge_strategy="merge")

    def open_state(self, session, name, connection, schema):
        return open_store(session.connection_factory(), "duckdb", schema, catalog=name)


BUILT_IN_ADAPTERS = MappingProxyType({"duckdb": DuckDBCatalogAdapter})
