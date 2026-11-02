#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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
"""Sandbox: the locked-down embedded DuckDB a stream's SQL steps run in."""

import json
import re
import shutil
import tempfile
import threading

import duckdb

from streamwright.core.engine.sql.tables import (
    _NAME,
    COLUMNS,
    DENIED_FUNCTIONS,
    LIMITS,
    MAX_ROWS,
    TABLE_FUNCTIONS,
    TIMEOUT,
    TRANSFORM_TIMEOUT,
    SqlError,
    _checked,
    _identifier,
    _json_text,
    _message,
    _partition_condition,
    _quoted,
)
from streamwright.core.engine.sql.types import _decode, _plain
from streamwright.core.runtime.templates import to_json
from streamwright.core.sql.analysis import query_references

_IN = re.compile(rb"IN\b", re.I)  # the keyword IN, at the start of a token
_PARAMETER = re.compile(rb"\$([A-Za-z_][A-Za-z0-9_]*)")  # a `$name` parameter, in a query's UTF-8 bytes
# a `$name` parameter in a step: a scalar subquery, which DuckDB never folds into a constant, so a step's column types
# are the same whatever the values (`'a' || NULL` folds to an INTEGER NULL); a plain cast where DuckDB allows no
# subquery (lambdas, arguments that must be constants, `= ANY(...)`); right after IN, either one in a cast: `x IN $ids`
# is DuckDB's list membership, which `x IN (...)` is not
_SCALAR, _CAST, _BARE = "(SELECT CAST(%s AS %s))", "(CAST(%s AS %s))", "CAST(%s AS %s)"


# * -------
# * sandbox
# * -------

class Sandbox:
    """
    The DuckDB of one run, locked down: in memory, spilling to a private temporary folder (removed by close()), with
    the tables of the stream that runs (its requests and steps).
    """

    def __init__(self, timeout=TIMEOUT, max_rows=MAX_ROWS):
        self.duckdb = duckdb
        self.timeout = timeout
        self.max_rows = max_rows
        self.folder = tempfile.mkdtemp(prefix="streamwright-sql-")
        try:
            # no files (DuckDB itself spills to the private folder), network or extensions, and no reading the
            # Python objects of the calling code (replacement scans)
            config = dict(LIMITS, enable_external_access=False, autoinstall_known_extensions=False,
                          autoload_known_extensions=False, python_enable_replacements=False,
                          temp_directory=self.folder)
            self.connection = duckdb.connect(":memory:", config=config)
        except Exception:
            shutil.rmtree(self.folder, ignore_errors=True)
            raise
        self.denied = set(DENIED_FUNCTIONS) | {row[0] for row in self.connection.execute(
            "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_type = 'macro' "
            "AND regexp_matches(macro_definition, '\\b(select|from)\\b', 'i')").fetchall()}
        # the built-in views a query reaches without a schema (pg_settings, duckdb_tables, ...): no WITH query,
        # request or step can take their names, so a name the query may read is never one of them instead
        self.relations = {row[0].lower() for row in self.connection.execute(
            "SELECT view_name FROM duckdb_views() WHERE schema_name IN ('main', 'pg_catalog') "
            "UNION SELECT table_name FROM duckdb_tables() WHERE schema_name IN ('main', 'pg_catalog')").fetchall()}
        self.connection.execute("SET TimeZone = 'UTC'")  # timestamps with time zones are written in UTC
        self.connection.execute("SET lock_configuration = true")

    def close(self):
        try:
            self.connection.close()
        finally:
            shutil.rmtree(self.folder, ignore_errors=True)

    def _timed(self, work, timeout=None):
        """
        work(), interrupted after `timeout` seconds (DuckDB stops between batches of rows). The interrupt repeats
        until the work ends: one sent between two statements would be lost, and the next statement not stopped.
        """
        timeout = timeout or self.timeout
        done, expired = threading.Event(), []

        def watchdog():
            if done.wait(timeout):
                return
            expired.append(True)
            while not done.is_set():
                self.connection.interrupt()
                done.wait(0.05)
        thread = threading.Thread(target=watchdog, name="streamwright-sql-timeout")
        thread.daemon = True
        thread.start()
        try:
            return work()
        except self.duckdb.Error as exc:
            if isinstance(exc, self.duckdb.InterruptException) or expired:
                raise SqlError("the query took longer than %gs" % timeout) from exc
            raise SqlError(_message(exc)) from exc
        finally:
            done.set()
            thread.join()  # no interrupt can reach the next query

    def _execute(self, sql, parameters=None):
        return self.connection.execute(sql, parameters) if parameters else self.connection.execute(sql)

    def parse(self, sql, tables):
        """
        (text, tables read, parameters) of a step's query: exactly one SELECT, reading only `tables` (and its own
        CTEs), the allowed table functions and no function that reads or changes settings. The tables read are
        lowercase, as DuckDB compares names; the parameters are the names of its `$name` parameters, once per use.
        """
        if not isinstance(sql, str) or not sql.strip():
            raise SqlError("`select` must be a SQL query")
        try:
            statements = self.duckdb.extract_statements(sql)
        except self.duckdb.Error as exc:
            raise SqlError(_message(exc)) from exc
        kinds = [statement.type.name for statement in statements]
        if kinds != ["SELECT"]:
            # a PIVOT without its values listed first creates a type for them
            hint = " (list the values of a PIVOT: ON column IN ('a', 'b'))" if "PIVOT" in sql.upper() else ""
            raise SqlError("`select` must be one SELECT statement, found: %s%s" % (", ".join(kinds) or "none", hint))
        query = statements[0].query.strip().rstrip(";").strip()
        used = query_references(self.tree(query))
        tables = {name.lower() for name in tables}
        # WITH queries have plain names that no table or view has, so a name the query reads is one of its WITH
        # queries (where that is in scope), a table it can read, or nothing (DuckDB then reports it)
        unnamed = sorted(name for name in used["ctes"] if not _NAME.match(name))
        if unnamed:
            raise SqlError("WITH queries need plain names (letters, digits and _), not %s" % ", ".join(unnamed))
        shadowing = sorted(used["ctes"] & tables)
        if shadowing:
            raise SqlError("WITH queries cannot be named %s: the query can read tables with these names; rename them"
                           % ", ".join(shadowing))
        shadowing = sorted(used["ctes"] & self.relations)
        if shadowing:
            raise SqlError("WITH queries cannot be named %s: DuckDB has tables or views with these names; rename "
                           "them" % ", ".join(shadowing))
        allowed = tables | used["ctes"]
        unknown = sorted(name for name in used["tables"] if name.startswith(".") or name not in allowed)
        if unknown:
            raise SqlError("the query can only read %s, not %s" % (", ".join(sorted(tables)) or "no tables", ", ".join(
                name.lstrip(".") + (" (with a schema)" if name.startswith(".") else "") for name in unknown)))
        functions = sorted(used["table_functions"] - set(TABLE_FUNCTIONS))
        if functions:
            raise SqlError("table function(s) %s are not allowed (allowed: %s)" % (", ".join(functions),
                                                                                   ", ".join(TABLE_FUNCTIONS)))
        denied = sorted(used["functions"] & self.denied)
        if denied:
            raise SqlError("function(s) %s are not allowed" % ", ".join(denied))
        if any(not _NAME.match(name) for name in used["parameters"]):
            raise SqlError("parameters are config inputs named in the query, `$name`: not ? or $1")
        return query, used["tables"] - used["ctes"], used["parameters"]

    def tree(self, query):
        """The parse tree of a query, as DuckDB serializes it (json_serialize_sql)."""
        tree = json.loads(self.connection.execute("SELECT json_serialize_sql($query)", {"query": query}).fetchone()[0])
        if tree.get("error"):
            raise SqlError("%s (write the query as a SELECT)" % tree.get("error_message", "the query cannot be read"))
        return tree

    def typed(self, query, parameters, types, scalar=True):
        """
        The query with each use of a `$name` parameter given its type (`types`: name -> DuckDB type): a scalar
        subquery, (SELECT CAST($name AS BIGINT)), whose type DuckDB keeps whatever the value (null too); with
        `scalar` false, a plain cast, (CAST($name AS BIGINT)). Right after IN, either goes in a cast: `x IN $ids` is
        DuckDB's list membership, contains($ids, x), and `x IN (...)` a list of values. Only these are added: the
        values stay bound parameters. `parameters` are the parameters DuckDB's parser found (parse); its tokenizer
        must find the same.
        """
        if not parameters:
            return query
        data = query.encode("utf-8")  # the tokenizer's positions are UTF-8 byte offsets
        found, pieces, last, previous = [], [], 0, None
        comment_type = getattr(self.duckdb.token_type, "comment", None)
        for start, kind in self.duckdb.tokenize(query):
            if kind == comment_type:
                continue
            before, previous = previous, (start, kind)
            if kind != self.duckdb.token_type.operator or data[start:start + 1] != b"$":
                continue
            match = _PARAMETER.match(data, start)
            name = match.group(1).decode("ascii") if match is not None else None
            if name not in types:
                continue
            found.append(name)
            text = (_SCALAR if scalar else _CAST) % (match.group(0).decode("ascii"), types[name])
            if before is not None and before[1] == self.duckdb.token_type.keyword and _IN.match(data, before[0]):
                text = _BARE % (text, types[name])
            pieces += [data[last:start], text.encode("utf-8")]
            last = match.end()
        if sorted(found) != sorted(parameters):
            raise SqlError("cannot tell the `$name` parameters of the query apart: write each as `$name`")
        return (b"".join(pieces) + data[last:]).decode("utf-8")

    def describe(self, query, parameters=None):
        """The (name, type) of the query's result columns, from empty tables: no data needed."""
        rows = self._timed(lambda: self._execute("DESCRIBE " + query, parameters).fetchall())
        return [(row[0], row[1]) for row in rows]

    def _conform(self, name, columns):
        """
        Gives the table `name` these (name, type) columns if DuckDB typed it otherwise: a step whose parameters are
        plain casts gets an INTEGER column where a null value folded an expression into an untyped NULL.
        """
        if [(row[0], row[1]) for row in self.connection.execute("DESCRIBE %s" % _quoted(name)).fetchall()] == list(
                columns):
            return
        self.connection.execute('DROP TABLE IF EXISTS "streamwright conform"')
        self.connection.execute('CREATE TABLE "streamwright conform" AS SELECT %s FROM %s' % (", ".join(
            "CAST(%s AS %s) AS %s" % (_identifier(column), kind, _identifier(column)) for column, kind in columns),
            _quoted(name)))
        self.connection.execute("DROP TABLE %s" % _quoted(name))
        self.connection.execute('ALTER TABLE "streamwright conform" RENAME TO %s' % _quoted(name))

    def materialize(self, name, query, parameters=None, columns=None, timeout=TRANSFORM_TIMEOUT):
        """Runs a step's query once, into the table `name` (replaced); with `columns`, of these types (_conform)."""
        def work():
            self.connection.execute("DROP TABLE IF EXISTS %s" % _quoted(name))
            self.connection.sql(query, params=parameters or None).create(_checked(name))
            if columns is not None:
                self._conform(name, columns)
        self._timed(work, timeout)

    def run_page(self, request, records, scope, steps, timeout=None):
        """
        Page mode: one page of `records` becomes the table of the stream's request, then the steps run in order, each
        into its table (`steps`: (name, query, parameters, columns), see materialize) and making at most max_rows
        rows; all within the time limit. Errors name the step.
        """
        running = []

        def work():
            self.connection.execute("DELETE FROM %s" % _quoted(request))
            self._insert(request, records, scope)
            for name, query, parameters, columns in steps:
                running[:] = [name]
                self.connection.execute("DROP TABLE IF EXISTS %s" % _quoted(name))
                self.connection.sql(query, params=parameters or None).create(_checked(name))
                if columns is not None:
                    self._conform(name, columns)
                if self.connection.execute("SELECT count(*) FROM %s" % _quoted(name)).fetchone()[0] > self.max_rows:
                    raise SqlError("the query made more than %d rows from one page" % self.max_rows)
        try:
            self._timed(work, timeout)
        except SqlError as exc:
            raise SqlError("step %r: %s" % (running[0], exc) if running else str(exc)) from exc

    # the stream's tables ---------------------------------------------------------

    def create_empty(self, name, columns):
        """Creates an empty table with known columns (replacing a table of that name)."""
        self.drop_table(name)
        try:
            self.connection.execute("CREATE TABLE %s (%s)" % (
                _quoted(name), ", ".join("%s %s" % (_identifier(column), kind) for column, kind in columns)))
        except self.duckdb.Error as exc:
            raise SqlError(_message(exc)) from exc

    def create_raw(self, name):
        """The empty table of a request (COLUMNS), named after it."""
        self.create_empty(name, COLUMNS)

    def _insert(self, name, records, scope):
        """Adds one page of records to the request table `name`; scope: partition, config, window, today."""
        window = scope.get("window") or (None, None)
        self.connection.execute(
            "INSERT INTO %s SELECT value, $partition::JSON, $config::JSON, $start, $end, $today "
            "FROM json_each($records::JSON)" % _quoted(name),
            {"records": _json_text(records), "partition": _json_text(scope.get("partition") or {}),
             "config": _json_text(scope.get("config") or {}), "start": window[0], "end": window[1],
             "today": scope.get("today")})

    def add_raw(self, name, records, scope):
        """Adds one page of records to the request table `name` (see create_raw)."""
        self._timed(lambda: self._insert(name, records, scope))

    def drop_table(self, name):
        """Drops a table of the stream if it exists."""
        try:
            self.connection.execute("DROP TABLE IF EXISTS %s" % _quoted(name))
        except self.duckdb.Error as exc:
            raise SqlError(_message(exc)) from exc

    def delete_partition(self, name, partition):
        """Removes the rows that the request table `name` holds for a stream partition (see _partition_condition)."""
        condition, params = _partition_condition(partition)
        self._timed(lambda: self.connection.execute("DELETE FROM %s WHERE %s" % (_quoted(name), condition), params),
                    TRANSFORM_TIMEOUT)

    def key_problem(self, name, keys, timeout=TRANSFORM_TIMEOUT):
        """Why the rows of table `name` are not unique on `keys`, or None."""
        table, columns = _quoted(name), ", ".join(_quoted(key) for key in keys)

        def work():
            nulls = self.connection.execute("SELECT %s FROM %s" % (", ".join(
                "count(*) FILTER (WHERE %s IS NULL)" % _quoted(key) for key in keys), table)).fetchone()
            empty = ["%r in %d row(s)" % (key, count) for key, count in zip(keys, nulls, strict=True) if count]
            if empty:
                return "primary_key column(s) are null: %s" % ", ".join(empty)
            repeated = self.connection.execute(
                "SELECT count(*) OVER () AS keys, %s, count(*) AS n FROM %s GROUP BY ALL HAVING count(*) > 1 "
                "ORDER BY n DESC, %s LIMIT 1" % (columns, table, columns)).fetchone()
            if repeated:
                example = to_json([_plain(value) for value in repeated[1:-1]], default=str)
                return ("primary_key (%s) is not unique: %d key(s) appear more than once, e.g. %s %d times (a join "
                        "may repeat rows)" % (", ".join(keys), repeated[0], example, repeated[-1]))
            return None
        return self._timed(work, timeout)

    def table_columns(self, name):
        """The (name, type) columns of table `name`."""
        try:
            return [(row[0], row[1]) for row in self.connection.execute("DESCRIBE %s" % _quoted(name)).fetchall()]
        except self.duckdb.Error as exc:
            raise SqlError(_message(exc)) from exc

    def table_records(self, name, columns, keys=()):
        """The rows of table `name`, with these columns, as plain records (read in batches, ordered by `keys`)."""
        names, kinds = [column for column, _ in columns], [kind for _, kind in columns]
        order = " ORDER BY %s" % ", ".join(_quoted(key) for key in keys) if keys else ""
        result = self.connection.execute("SELECT * FROM %s%s" % (_quoted(name), order))
        while True:
            batch = result.fetchmany(10000)
            if not batch:
                return
            for row in batch:
                yield {column: _decode(value, kind) for column, value, kind in zip(names, row, kinds, strict=True)}

    def raw_rows(self, name):
        """
        (partition, record) of each row of the request table `name`, as data, in the order they were read. A
        generator: use it up before the next query.
        """
        try:
            result = self.connection.execute("SELECT partition, record FROM %s" % _quoted(name))
            while True:
                batch = result.fetchmany(10000)
                if not batch:
                    return
                for partition, record in batch:
                    yield json.loads(partition) if partition else {}, None if record is None else json.loads(record)
        except self.duckdb.Error as exc:
            raise SqlError(_message(exc)) from exc

    def distinct_rows(self, name, fields):
        """
        The distinct rows of these columns of table `name` without SQL nulls, sorted, as data: JSON values are
        decoded (per the columns' types), so a JSON text is its text, not a quoted one.
        """
        kinds = {column.lower(): kind for column, kind in self.table_columns(name)}
        columns = ", ".join(_quoted(field) for field in fields)
        where = " AND ".join("%s IS NOT NULL" % _quoted(field) for field in fields)
        types = [kinds.get(field.lower(), "") for field in fields]

        def work():
            rows = self.connection.execute("SELECT DISTINCT %s FROM %s WHERE %s ORDER BY %s" % (
                columns, _quoted(name), where, columns)).fetchall()
            return [tuple(_decode(value, kind) for value, kind in zip(row, types, strict=True)) for row in rows]
        return self._timed(work, TRANSFORM_TIMEOUT)
