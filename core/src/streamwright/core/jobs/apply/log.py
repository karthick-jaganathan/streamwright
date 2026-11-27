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
An apply output's `log:` (DuckDBJob and SparkJob, one meaning): once a run has sent a batch's rows of the output, a line
per row of its results, as a step's `log:` logs a step's rows (on the `streamwright.output` logger):

    output <name>: <message, its {column} placeholders filled from the row>
    output <name>: ... and N more row(s)

The results are a table named after the output: the row's columns, as the output's SQL made them, and `outcome`, a
struct of `status` (applied or failed), `error_code`, `error_message`, `result` (JSON: what the vendor returned for an
applied row) and `row` (the row's number among the output's rows of the batch, from 1). `sql`, a SELECT over that table,
picks and shapes the lines (e.g. the failed rows, or a count per error code); without it, every row is logged. It runs
in an in-memory DuckDB that holds only the results (no files, no network). The rows logged are the ones this run sent; a
`log:` that cannot run is a warning, never a failure (the rows were sent). A run that may not log row values
(`streamwright serve`) logs only how many rows the SELECT returned.
"""

import json
import logging
import os
import tempfile

import duckdb

from streamwright.core.jobs.fields import Field
from streamwright.core.jobs.step_log import STEP_LOG_FIELD, StepLog, StepLogError, log_step_rows
from streamwright.core.runtime.logs.sql import logged
from streamwright.core.sql.quote import quote_ident

__all__ = ["OUTPUT_LOG", "OUTPUT_LOG_FIELD", "OutputLog", "check_output_log", "log_outcomes"]

OUTPUT_LOG = logging.getLogger("streamwright.output")
OUTPUT_LOG_FIELD = Field("object", fields={
    **STEP_LOG_FIELD.fields,
    "sql": Field("string", description="apply outputs: a SELECT over the results, the table named after the output "
                                       "(its columns, and outcome.status, outcome.error_code, outcome.error_message, "
                                       "outcome.result, outcome.row); default: every row"),
    "message": Field("string", required=True,
                     description="the line logged per row: text with {column} placeholders of the SELECT's columns "
                                 "(or {outcome.status}, ... without sql)"),
}, description="apply outputs: log the results of the rows a run sent (streamwright.output): `output <name>: "
               "<message>` per row, up to max_rows")
_LIMITS = ("SET threads = 1", "SET memory_limit = '256MB'")
OUTCOME = "outcome"
_LOCK = ("SET enable_external_access = false", "SET lock_configuration = true")


class OutputLog(StepLog):
    """An apply output's `log:`, parsed: its message, level, max_rows and SELECT."""

    kind, logger, event = "output", OUTPUT_LOG, "output_log"

    def __init__(self, output, spec):
        super().__init__(output, spec)
        self.sql = spec.get("sql")

    @classmethod
    def of(cls, item):
        spec = item.get("log")
        return cls(item["name"], spec) if isinstance(spec, dict) else None


def _one_select(sql):
    """Raises StepLogError unless `sql` is one SELECT (parsed, never run)."""
    try:
        statements = duckdb.connect().extract_statements(sql)
    except duckdb.Error as exc:
        raise StepLogError("log.sql: %s" % str(exc).splitlines()[0]) from None
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
        raise StepLogError("log.sql: one SELECT over the results is expected")


def check_output_log(log):
    """Problems of an apply output's `log:` the structure check does not see: [(code, message, is_warning)]."""
    if not isinstance(log, dict) or "message" not in log:
        return []                                 # the structure check reports it
    try:
        OutputLog("output", log)
        if isinstance(log.get("sql"), str):
            _one_select(log["sql"])
    except StepLogError as exc:
        return [("bad-value", str(exc), False)]
    return []


def _results(connection, name, rows, folder):
    """The results table `name` from rows of (payload, (row, status, result, error_code, error_message))."""
    path = os.path.join(folder, "results.json")
    if any(OUTCOME in (payload or {}) for payload, _ in rows):
        raise StepLogError("a column of the output is named %r, the results' own column: rename it" % OUTCOME)
    with open(path, "w", encoding="utf-8") as stream:
        for payload, (number, status, result, code, message) in rows:
            outcome = {"status": status, "error_code": code, "error_message": message, "row": number + 1,
                       "result": None if result is None else json.dumps(result, default=str)}
            stream.write(json.dumps({**(payload or {}), "outcome": outcome}, default=str) + "\n")
    connection.execute(
        "CREATE TABLE %s AS SELECT * REPLACE (struct_pack(status := outcome.status::VARCHAR, error_code := "
        "outcome.error_code::VARCHAR, error_message := outcome.error_message::VARCHAR, result := "
        "outcome.result::JSON, \"row\" := outcome.\"row\"::BIGINT) AS outcome) FROM read_json(?, format = "
        "'newline_delimited', sample_size = -1)" % quote_ident(name), (path,))     # types from every row


def log_outcomes(log, rows, *, row_values=True, redact=None, constants=None):
    """
    Logs an apply output's `log:` over the results of the rows a run sent (`rows`: (payload, outcome) pairs, the
    outcome as the state store records it: row, status, result, error_code, error_message).
    """
    if not rows:
        return
    try:
        quiet = duckdb.connect()                        # its limits and lock: unlogged; its queries: logged
        connection = logged(quiet, "output log")
        try:
            for statement in _LIMITS:
                quiet.execute(statement)
            with tempfile.TemporaryDirectory(prefix="sw-output-log-") as folder:
                _results(connection, log.step, rows, folder)
            for statement in _LOCK:
                quiet.execute(statement)
            if log.sql is not None:
                _one_select(log.sql)
            cursor = connection.execute(log.sql or "SELECT * FROM %s" % quote_ident(log.step))
            columns = [column[0] for column in cursor.description]
            found = cursor.fetchall()
        finally:
            connection.close()
        log_step_rows(log, columns, lambda n: found[:n], lambda: len(found), row_values=row_values,
                      constants=constants, redact=redact)
    except (duckdb.Error, StepLogError, OSError) as exc:
        text = (redact or str)("output %s: its log: cannot run: %s" % (log.step, str(exc).splitlines()[0]))
        OUTPUT_LOG.warning("%s", text)

