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

"""File outputs (--output jsonl:DIR, csv:DIR, tsv:DIR, parquet:DIR): one file per export, plus state.json."""


import csv
import datetime
import decimal
import json
import logging
import os
import stat
import tempfile

import duckdb

from streamwright.core.outputs.base import BaseOutput, log_summary
from streamwright.core.outputs.file_names import STATE_FILE, is_inside
from streamwright.core.outputs.staging import Staging, quote, sql_string, staged_table, storage_type, typed_columns
from streamwright.core.runtime.templates import to_json, to_text

LOG = logging.getLogger("streamwright.output")


class _AtomicFile:
    """Reserve a unique hidden path and publish it atomically on success."""

    def __init__(self, file_name, output_path, suffix):
        prefix = ".".join([file_name, datetime.datetime.now().strftime("%Y-%m-%d.%H%M%S%f.")])
        descriptor, self.tmp_path = tempfile.mkstemp(prefix="." + prefix, suffix=suffix + ".part", dir=output_path)
        os.close(descriptor)
        self.path = os.path.join(output_path, os.path.basename(self.tmp_path)[1:-len(".part")])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            os.replace(self.tmp_path, self.path)
        elif os.path.exists(self.tmp_path):
            os.remove(self.tmp_path)
        return False


class FileOutput(BaseOutput):
    """
    One file per export plus state.json, written atomically (into a hidden temporary file, renamed into place when
    the run succeeds): `<export>.<timestamp>.<unique>.<jsonl|csv|tsv>` in the directory, or the path its file name
    gives (`file_names`: export -> path inside the directory, from --file-name or the export's `file_name`; folders
    are made when the run succeeds). A directory whose state.json is a folder is refused, and closing writes no file
    if a named path would leave the directory through a symbolic link.

    jsonl files hold one JSON object per line. csv and tsv files (the csv module's excel and excel-tab dialects:
    comma- or tab-separated, rows ending with CRLF) start with a header row of the export's columns; a null is an
    empty field, a DECIMAL its exact value (templates.decimal_text), a list is comma-joined (templates.to_text) and
    a mapping is JSON. A field that holds the separator, a quote, a carriage return or a newline is quoted, its quotes
    doubled.
    """

    def __init__(self, directory, file_format, file_names=None):
        state = os.path.join(directory, STATE_FILE)
        if os.path.isdir(state):  # (before any request: the run would fail when it ends)
            raise ValueError("%s is a folder: the run writes its state to that path" % state)
        self.directory = directory
        self.format = file_format
        self.file_names = file_names
        self.files = {}  # export -> dict(temporary, path, records; handle and writer while it is written; bytes)
        self.state = None
        self.status = None  # "written" once close() has put the files in place, "failed" if it wrote none
        os.makedirs(directory, exist_ok=True)

    def _reserve(self, name):
        """(temporary, path): an export's hidden temporary file, made in the directory, and the path it becomes."""
        if name not in (self.file_names or {}):  # an export without a file name: <export>.<timestamp>.<unique>
            target = _AtomicFile(name, self.directory, "." + self.format)
            temporary, path = target.tmp_path, target.path
        else:
            path = os.path.join(self.directory, self.file_names[name])
            descriptor, temporary = tempfile.mkstemp(prefix="." + os.path.basename(path) + ".", suffix=".part",
                                                     dir=self.directory)
            os.close(descriptor)
        LOG.debug("export %r: its file is %s until the run succeeds, then %s", name, temporary, path,
                  extra={"export": name, "path": path})
        return temporary, path

    def write_schema(self, name, schema, key_properties):
        temporary, path = self._reserve(name)
        handle = open(temporary, "w", encoding="utf-8", newline="")  # noqa: SIM115  open until _close_files()
        writer = None
        if self.format in ("csv", "tsv"):
            writer = csv.DictWriter(handle, fieldnames=list(schema.get("properties") or {}), extrasaction="ignore",
                                    dialect="excel-tab" if self.format == "tsv" else "excel")
            writer.writeheader()
        self.files[name] = {"temporary": temporary, "path": path, "handle": handle, "writer": writer, "records": 0}

    def write_record(self, name, record):
        item = self.files[name]
        if self.format == "jsonl":
            item["handle"].write(to_json(record, default=to_text, sort_keys=True) + "\n")
        else:
            item["writer"].writerow({k: to_text(v) if isinstance(v, (dict, list, decimal.Decimal)) else v
                                         for k, v in record.items()})
        item["records"] += 1

    def write_state(self, state):
        self.state = state

    def summary(self):
        """
        One entry per export (see the module's documentation): export, records, path, and bytes and state
        (DIR/state.json, when the run saved one) once the files are written. No entries after a failed run: no file
        was written.
        """
        if self.status == "failed":
            return []
        entries = []
        for name, item in self.files.items():
            entry = {"export": name, "records": item["records"], "path": item["path"]}
            if "bytes" in item:
                entry["bytes"] = item["bytes"]
            if self.status == "written" and self.state is not None:
                entry["state"] = os.path.join(self.directory, STATE_FILE)
            entries.append(entry)
        return entries

    def _close_files(self):
        for item in self.files.values():
            item["handle"].close()

    def _write_files(self):
        """Fills the temporary files before they are renamed into place (jsonl, csv and tsv ones hold the records)."""

    def _cleanup(self):
        """Removes what the output kept while the run went, whether or not it wrote its files."""

    def close(self, failed=False):
        self.status = "failed"
        written = False
        try:
            self._close_files()
            # a link made during the run cannot take a named file outside the directory: then no file is written
            outside = [] if failed or self.file_names is None else [
                item["path"] for item in self.files.values() if not is_inside(self.directory, item["path"])]
            if outside:
                raise ValueError("file names: %s %s outside %s through a symbolic link: no file was written" % (
                    ", ".join(repr(os.path.relpath(path, self.directory)) for path in outside),
                    "leads" if len(outside) == 1 else "lead", self.directory))
            if failed:
                return
            self._write_files()
            for item in self.files.values():
                if os.path.dirname(item["path"]):
                    os.makedirs(os.path.dirname(item["path"]), exist_ok=True)
                item["bytes"] = os.path.getsize(item["temporary"])
                os.replace(item["temporary"], item["path"])
            if self.state is not None:
                with _AtomicFile("state", self.directory, ".json") as target, \
                        open(target.tmp_path, "w", encoding="utf-8") as stream:
                    json.dump(self.state, stream, indent=2, sort_keys=True)
                os.replace(target.path, os.path.join(self.directory, STATE_FILE))
            written = True
        finally:
            try:
                for item in [] if written else self.files.values():
                    if os.path.exists(item["temporary"]):
                        os.remove(item["temporary"])
            finally:
                self._cleanup()
        self.status = "written"
        log_summary(self.summary())


def parquet_type(kind):
    """
    The Parquet column type of a DuckDB column type: the type DuckDB tables store it as (staging.storage_type), except
    that HUGEINT and UHUGEINT are DECIMAL(38,0), exact where DuckDB would write a DOUBLE (values of more than 38 digits
    fail the run), and JSON (nested lists, structs, maps and JSON values) is its text, VARCHAR.
    """
    kind = storage_type(kind)
    if kind in ("HUGEINT", "UHUGEINT"):
        return "DECIMAL(38,0)"
    return "VARCHAR" if kind == "JSON" else kind


def _parquet_column(column, kind):
    """The select item of a column (of a storage type, see typed_columns) in a Parquet file."""
    target = parquet_type(kind)
    return quote(column) if target == kind else "CAST(%s AS %s) AS %s" % (quote(column), target, quote(column))


class ParquetOutput(FileOutput):
    """
    One Parquet file per export (zstd compression) plus state.json, named and written atomically like FileOutput's
    files, with the export's DuckDB column types (write_columns; without them, its schema's) as parquet_type maps them:
    DECIMAL(p,s), DATE, TIMESTAMP WITH TIME ZONE, BIGINT, BOOLEAN, DOUBLE, VARCHAR... Records are staged in a private
    temporary folder while the run goes (staging.Staging), so memory stays flat; when the run succeeds, a DuckDB of
    its own (not the run's locked-down sandbox) types them and writes each export's file into its hidden temporary
    file, which is then renamed into place. A failed run writes no files; closing removes the staged records.
    """

    def __init__(self, directory, file_names=None):
        super().__init__(directory, "parquet", file_names)
        self.staging = Staging("streamwright-parquet-")

    def write_columns(self, name, columns):
        self.staging.write_columns(name, columns)

    def write_schema(self, name, schema, key_properties):
        temporary, path = self._reserve(name)
        self.files[name] = {"temporary": temporary, "path": path, "records": 0}
        self.staging.write_schema(name, schema, key_properties)

    def write_record(self, name, record):
        self.staging.write_record(name, record)
        self.files[name]["records"] += 1

    def _close_files(self):
        self.staging.close()

    def _write_files(self):
        if not self.files:
            return
        # it reads the staged files and writes the Parquet ones in the records' order, spilling to the staging folder
        connection = duckdb.connect(":memory:", config={
            "temp_directory": os.path.join(self.staging.directory, "duckdb"), "preserve_insertion_order": True})
        try:
            connection.execute("SET TimeZone = 'UTC'")
            for name, item in self.files.items():
                export, columns = self.staging.exports[name], self.staging.columns_of(name)
                rows = "SELECT %s FROM %s" % (", ".join(typed_columns(columns, export["whole_record"])),
                                              staged_table(export["path"]))
                select = ", ".join(_parquet_column(column, kind) for column, kind in columns)
                LOG.debug("export %r: DuckDB writes %s from %s, columns %s", name, item["temporary"], export["path"],
                          ", ".join("%s %s" % (column, parquet_type(kind)) for column, kind in columns),
                          extra={"export": name, "path": item["path"]})
                mode = stat.S_IMODE(os.stat(item["temporary"]).st_mode)
                try:
                    # straight into the hidden file, which is the temporary one (DuckDB removes it if it fails)
                    connection.execute("COPY (SELECT %s FROM (%s)) TO %s (FORMAT parquet, COMPRESSION zstd, "
                                       "USE_TMP_FILE false)" % (select, rows, sql_string(item["temporary"])))
                except duckdb.Error as exc:
                    message = " ".join(line.strip() for line in str(exc).strip().split("\n\n")[0].splitlines()
                                       if line.strip())
                    if "DECIMAL(38,0)" in message and any(kind in ("HUGEINT", "UHUGEINT") for _, kind in columns):
                        message += " (HUGEINT and UHUGEINT columns are DECIMAL(38,0) in Parquet files: 38 digits)"
                    raise ValueError("export %r: cannot write its Parquet file: %s" % (name, message)) from exc
                os.chmod(item["temporary"], mode)  # the permissions of the other files, should DuckDB make a new one
        finally:
            connection.close()

    def _cleanup(self):
        self.staging.remove()
