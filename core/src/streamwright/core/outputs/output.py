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

"""
Writers for a run's SCHEMA / RECORD / STATE output: Singer messages, files or warehouse loaders.

Every output has summary(): what it wrote, one entry (a dict) per export, in the order the exports were first
written. Call it after close(). Keys:

- export: the export's name; records: the number of records the run wrote to it.
- path and bytes (jsonl, csv, tsv and parquet files): the export's file (the output directory joined with its name)
  and its size in bytes, once it is written.
- table, disposition and primary_key (duckdb, ducklake and dlt): the table, as SCHEMA.TABLE (dlt: DATASET.TABLE, as
  dlt names it), how it was loaded - merge (on primary_key), replace, append, or skipped (an unkeyed export of a
  full-refresh stream that skipped partitions: its table keeps the rows of the last complete run) - and its key.
- state: where the run's state went, when it was saved: DIR/state.json, SCHEMA._streamwright_state,
  DATASET._dlt_pipeline_state, or stdout for Singer STATE messages.

After close(failed=True), or a close() that failed, the outputs that write when a run ends have no entries: nothing
was written. Singer messages go out while the run goes, so SingerOutput describes them in any case.

Outputs log on the logger `streamwright.output`: one INFO line per entry when they close successfully (log_summary),
their warnings, and staging details at DEBUG.

Records keep the exact values of DECIMAL columns (decimal.Decimal): JSON (Singer messages, jsonl files) writes them
as exact numbers, csv and tsv files as the same text (templates.decimal_text: 2.6, 100, 12345678901234567890.123456),
and DuckDB, DuckLake and Parquet store them in DECIMAL columns (dlt too, in the columns it makes).
"""

import datetime
import importlib
import logging
import re
import sys
from types import MappingProxyType

from streamwright.core.outputs.base import BaseOutput, Output, log_summary
from streamwright.core.outputs.file_names import FILE_NAME_SCOPES, export_files
from streamwright.core.outputs.files import FileOutput, ParquetOutput, parquet_type
from streamwright.core.outputs.warehouse import DuckDBOutput, DuckLakeOutput
from streamwright.core.runtime.templates import to_json, to_text

__all__ = [
    "FILE_FORMATS",
    "FILE_NAME_OUTPUTS",
    "FILE_NAME_SCOPES",
    "OUTPUT_FACTORIES",
    "BaseOutput",
    "FileOutput",
    "Output",
    "ParquetOutput",
    "SingerOutput",
    "export_files",
    "log_summary",
    "open_output",
    "parquet_type",
]

LOG = logging.getLogger("streamwright.output")
# --output FORMAT:DIR: one file per export, plus state.json
FILE_FORMATS = ("jsonl", "csv", "tsv", "parquet")
FILE_NAME_OUTPUTS = "--file-name works only with --output jsonl:DIR, csv:DIR, tsv:DIR or parquet:DIR"
DLT_OUTPUT_MODULE = "streamwright.core.outputs.dlt_output"


def _now():
    return datetime.datetime.now(datetime.UTC).isoformat()


class SingerOutput(BaseOutput):
    """Newline-delimited Singer messages, readable by Singer targets."""

    def __init__(self, stream=None):
        self.stream = stream or sys.stdout
        self.where = "stdout" if self.stream is sys.stdout else str(getattr(self.stream, "name", "stream"))
        self.records = {}  # export -> records written, in the order of their schemas
        self.state_written = False

    def _write(self, message):
        self.stream.write(to_json(message, default=to_text, sort_keys=True) + "\n")

    def write_schema(self, name, schema, key_properties):
        self._write({"type": "SCHEMA", "stream": name, "schema": schema, "key_properties": key_properties})
        self.records.setdefault(name, 0)

    def write_record(self, name, record):
        self._write({"type": "RECORD", "stream": name, "record": record, "time_extracted": _now()})
        self.records[name] = self.records.get(name, 0) + 1

    def write_state(self, state):
        self._write({"type": "STATE", "value": state})
        self.state_written = True

    def summary(self):
        """
        One entry per export (see the module's documentation): export, records, and state (stdout) once a STATE
        message was written. The messages go out while the run goes, so a failed run's are described too.
        """
        entries = []
        for name, count in self.records.items():
            entry = {"export": name, "records": count}
            if self.state_written:
                entry["state"] = self.where
            entries.append(entry)
        return entries

    def close(self, failed=False):
        self.stream.flush()
        if not failed:
            log_summary(self.summary(), self.where)


def _open_singer(detail, source, file_names):
    if detail:
        raise ValueError("--output singer does not take a destination")
    return SingerOutput()


def _open_file(kind):
    def factory(detail, source, file_names):
        if not detail:
            raise ValueError("--output %s needs a directory" % kind)
        return ParquetOutput(detail, file_names) if kind == "parquet" else FileOutput(detail, kind, file_names)
    return factory


def _open_dlt(detail, source, file_names):
    destination, _, dataset = detail.partition(":")
    if not destination:
        raise ValueError("--output dlt needs a destination, e.g. dlt:duckdb or dlt:bigquery:DATASET")
    return _dlt_output_class()(destination, dataset or None, source)


def _dlt_output_class():
    """DltOutput, from the module that imports the optional dlt library (loaded for `--output dlt` only)."""
    try:
        module = importlib.import_module(DLT_OUTPUT_MODULE)
    except ImportError as exc:
        raise ValueError('--output dlt:... needs dlt: pip install "streamwright[dlt]" and the destination\'s extra, '
                         'e.g. "dlt[duckdb]"') from exc
    return module.DltOutput


def _open_warehouse(kind):
    def factory(detail, source, file_names):
        path, _, schema = detail.partition(":")
        if kind == "ducklake" and path == "postgres":
            path, colon, schema = detail.rpartition(":")
            if not (colon and path != "postgres" and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", schema)):
                path, schema = detail, ""
        if not path:
            raise ValueError("--output %s needs a path, e.g. %s:warehouse.duckdb or %s:catalog.ducklake:SCHEMA" % (
                kind, kind, kind))
        return (DuckDBOutput if kind == "duckdb" else DuckLakeOutput)(path, schema or source["name"], source)
    return factory


# output kind -> factory taking (detail, source, file_names): the kinds `--output` accepts


OUTPUT_FACTORIES = MappingProxyType({
    "singer": _open_singer, "dlt": _open_dlt,
    "duckdb": _open_warehouse("duckdb"), "ducklake": _open_warehouse("ducklake"),
    **{kind: _open_file(kind) for kind in FILE_FORMATS},
})


def open_output(spec, source=None, file_names=None, *, factories=OUTPUT_FACTORIES):
    """'singer' (default), 'jsonl:DIR', 'csv:DIR', 'tsv:DIR', 'parquet:DIR', 'dlt:DESTINATION[:DATASET]',
    'duckdb:PATH[:SCHEMA]' or 'ducklake:CATALOG[:SCHEMA]' (the dataset / schema defaults to the source name; a
    DuckLake CATALOG can be 'postgres:DSN', a libpq connection string: ducklake:postgres:DSN[:SCHEMA]).
    file_names (--file-name, see export_files): each export's file, for the file outputs (FILE_FORMATS) only.
    factories: the output kinds and their factories; pass OUTPUT_FACTORIES with others to add kinds."""
    kind, _, directory = (spec or "singer").partition(":")
    if file_names is not None and kind not in FILE_FORMATS:
        raise ValueError(FILE_NAME_OUTPUTS)
    if kind not in factories:
        raise ValueError("--output must be singer, jsonl:DIR, csv:DIR, tsv:DIR, parquet:DIR, "
                         "dlt:DESTINATION[:DATASET], duckdb:PATH[:SCHEMA] or ducklake:CATALOG[:SCHEMA], got %r" % spec)
    return factories[kind](directory, source or {"name": "source", "streams": []}, file_names)
