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

"""Writing an output into a table of an attached catalog (a DuckDB file, or a database a connector attaches)."""

from streamwright.core.jobs.duckdb.reads import quote_relation, relation_parts
from streamwright.core.jobs.types import WriteReceipt
from streamwright.core.jobs.write_rules import WriteTarget
from streamwright.core.sql.quote import quote_ident

__all__ = ["MERGE_STRATEGIES", "TABLE_TARGET", "write_table"]

TABLE_TARGET = WriteTarget(
    name="table", modes=("append", "overwrite", "merge"), keys=("table", "primary_key"),
    required={"*": ("table",), "merge": ("primary_key",)}, replay_safe=("overwrite", "merge"))

# merge: MERGE INTO (DuckDB tables); on_conflict: INSERT ... ON CONFLICT (needs a unique key on the target)
MERGE_STRATEGIES = ("merge", "on_conflict")


def _exists(con, parts):
    query = ("SELECT 1 FROM information_schema.tables WHERE lower(table_catalog) = lower(?) "
             "AND lower(table_schema) = lower(?) AND lower(table_name) = lower(?) LIMIT 1")
    return bool(con.execute(query, list(parts)).fetchall())


def _merge(con, target, source, *, keys, columns, strategy):
    key_list = ", ".join(quote_ident(k) for k in keys)
    updates = [c for c in columns if c not in keys]
    if strategy == "on_conflict":
        action = ("DO UPDATE SET %s" % ", ".join("%s = EXCLUDED.%s" % (quote_ident(c), quote_ident(c))
                                                 for c in updates)) if updates else "DO NOTHING"
        names = ", ".join(quote_ident(c) for c in columns)
        con.execute("INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) %s" % (
            target, names, names, source, key_list, action))
        return
    on = " AND ".join("target.%s = source.%s" % (quote_ident(k), quote_ident(k)) for k in keys)
    matched = ("WHEN MATCHED THEN UPDATE SET %s " % ", ".join(
        "%s = source.%s" % (quote_ident(c), quote_ident(c)) for c in updates)) if updates else ""
    con.execute("MERGE INTO %s AS target USING %s AS source ON %s %sWHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)" % (
        target, source, on, matched, ", ".join(quote_ident(c) for c in columns),
        ", ".join("source.%s" % quote_ident(c) for c in columns)))


def write_table(context, catalog, merge_strategy, default_schema="main"):
    """Writes context.source into `spec.table` of `catalog`; a missing table is created from the data."""
    if merge_strategy not in MERGE_STRATEGIES:
        raise ValueError("merge_strategy must be one of %s" % ", ".join(MERGE_STRATEGIES))
    spec, con = context.spec, context.session.con
    parts = relation_parts(catalog, spec.table, default_schema)
    target = quote_relation(catalog, spec.table, default_schema)
    rows = con.execute("SELECT COUNT(*) FROM %s" % context.source).fetchall()[0][0]
    if not _exists(con, parts):
        con.execute("CREATE TABLE %s AS SELECT * FROM %s" % (target, context.source))
    elif spec.mode == "overwrite":
        con.execute("CREATE OR REPLACE TABLE %s AS SELECT * FROM %s" % (target, context.source))
    elif spec.mode == "append":
        con.execute("INSERT INTO %s BY NAME SELECT * FROM %s" % (target, context.source))
    elif spec.mode == "merge":
        columns = [d[0] for d in con.execute("SELECT * FROM %s LIMIT 0" % context.source).description]
        missing = [k for k in spec.primary_key if k not in columns]
        if missing:
            raise ValueError("output %s: primary_key column(s) %s are not in the data" % (
                spec.name, ", ".join(missing)))
        _merge(con, target, context.source, keys=list(spec.primary_key), columns=columns, strategy=merge_strategy)
    else:
        raise ValueError("output %s: table writes do not support mode %r" % (spec.name, spec.mode))
    return WriteReceipt(spec.name, target, rows)
