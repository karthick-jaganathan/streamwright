"""
The output contract of source runs (Output, BaseOutput) and the summary log lines outputs write when they close
(log_summary). Every output module builds on this one.
"""

import logging
from typing import Protocol, runtime_checkable

__all__ = ["BaseOutput", "Output", "log_summary", "readable_size"]

LOG = logging.getLogger("streamwright.output")
# the summary() keys that log records carry as fields (for structured logs), with the event "output"
_LOG_FIELDS = ("export", "records", "path", "bytes", "table")


def _counted(count, word):
    """1 record, 1,234 records."""
    return "{:,} {}{}".format(count, word, "" if count == 1 else "s")


def readable_size(count):
    """A number of bytes for people: 512 bytes, 41.2 KB, 3.0 MB (1 KB is 1,024 bytes)."""
    if count < 1024:
        return _counted(count, "byte")
    size = float(count)
    for unit in ("KB", "MB", "GB"):
        size /= 1024
        if round(size, 1) < 1024:
            return "%.1f %s" % (size, unit)
    return "%.1f TB" % (size / 1024)


def _summary_line(entry, where):
    """(format, arguments) of the log line of a summary() entry; `where`: where Singer messages went."""
    records = entry["records"]
    if "table" in entry:
        if entry.get("disposition") == "skipped":
            return ("did not load %s: %s (its stream skipped partitions: the table keeps the rows of the last "
                    "complete run)", (entry["table"], _counted(records, "row")))
        how = entry.get("disposition")
        if how == "merge":
            how = "merge on %s" % ", ".join(entry.get("primary_key") or ())
        return "loaded %s: %s (%s)", (entry["table"], _counted(records, "row"), how)
    if "path" in entry:
        return "wrote %s: %s, %s", (entry["path"], _counted(records, "record"), readable_size(entry.get("bytes") or 0))
    return "wrote %r to %s: %s", (entry["export"], where, _counted(records, "record"))


def log_summary(entries, where="stdout"):
    """
    Logs one INFO line per summary() entry on the logger streamwright.output, with the event "output" and the entry's
    export, records, path, bytes and table as attributes of the log record (fields of structured logs):

        wrote out/campaigns.2026-10-04.112233123456.k2j4.jsonl: 29 records, 41.2 KB
        loaded acme_google.campaigns: 29 rows (merge on customer_id, campaign_id)
        wrote 'campaigns' to stdout: 29 records
    """
    for entry in entries:
        line, arguments = _summary_line(entry, where)
        fields = {key: entry[key] for key in _LOG_FIELDS if key in entry}
        LOG.info(line, *arguments, extra=dict(fields, event="output"))


@runtime_checkable
class Output(Protocol):
    """The contract required by source runs, including optional capabilities with safe defaults."""

    def write_schema(self, name, schema, key_properties): ...
    def write_record(self, name, record): ...
    def write_state(self, state): ...
    def write_columns(self, name, columns): ...
    def mark_partial(self, name): ...
    def initial_state(self): ...
    def summary(self): ...
    def close(self, failed=False): ...


class BaseOutput:
    """Default no-op capabilities for non-loading outputs."""

    def write_columns(self, name, columns):
        pass

    def mark_partial(self, name):
        pass

    def initial_state(self):
        return None
