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
The batches of `streamwright state` (list, show, retry, abandon, resend, skip, leases, purge): read from the state
database alone - every job's batches, each batch's outbox rows (an apply output's rows with their outcome) - and
settled under the batch's lock tokens.
"""

import datetime
import json
import sys
from types import MappingProxyType

from streamwright.core.jobs.locking import batch_tokens
from streamwright.core.jobs.runner import owner_id
from streamwright.core.jobs.state.protocol import StateError
from streamwright.core.jobs.state.url import open_url

__all__ = ["batch_details", "cmd_change", "cmd_leases", "cmd_list", "cmd_purge", "cmd_show", "outbox_summary",
           "public"]

SUCCESS = 0
FAILURE = 1
INVALID_INPUT = 2
_LEASE_SECONDS = 60
_PUBLIC_KEYS = MappingProxyType({"batch_id": "job_id", "batch_sequence": "sequence"})   # store keys -> output


def public(record):
    """A store record with the state tables' names: job_id and sequence (the Python API says batch_id)."""
    return {_PUBLIC_KEYS.get(key, key): value for key, value in record.items()}


def batch_details(store, job_name, batch):
    """One batch as `streamwright state show` shows it: its record, pinned manifest, output receipts and outbox rows."""
    manifest = dict(batch["manifest"])
    if manifest.get("messages"):
        manifest["messages"] = "%d message(s)" % len(manifest["messages"])
    return dict(public(batch), manifest=manifest,
                receipts={name: public(receipt)
                          for name, receipt in store.get_receipts(job_name, batch["batch_id"]).items()},
                outbox=[public(row) for row in store.list_events(job_name, batch["batch_id"])])


def outbox_summary(counts):
    """A batch's outbox rows by status, for a listing: `2 applied, 2 failed` (`-`: none)."""
    return ", ".join("%d %s" % (count, status) for status, count in sorted((counts or {}).items())) or "-"


# * -------
# * batches
# * -------

def _store(args):
    """The state database the arguments name, as it is (never created, never migrated); None after an error."""
    try:
        return open_url(args.url, schema=args.schema, existing=True)
    except (ValueError, ImportError) as exc:
        sys.stderr.write("streamwright state: %s\n" % exc)
        return None


def _read(store, work):
    """`work()`, the database's own error (no such table, connection refused) said in one line."""
    try:
        return work()
    except Exception as exc:  # the driver's error: its type varies with the database
        sys.stderr.write("streamwright state: cannot read the state tables in %s: %s (run `streamwright state "
                         "check` to see their migrations)\n" % (store.target, str(exc).split("\n", 1)[0]))
        return None


def _first_line(text, limit=80):
    line = (text or "").strip().split("\n", 1)[0]
    return line if len(line) <= limit else line[:limit - 3] + "..."


def cmd_list(args) -> int:
    store = _store(args)
    if store is None:
        return INVALID_INPUT
    batches = _read(store, lambda: store.find_batches(job_name=args.job, scope=args.scope, status=args.status,
                                                      request_id=args.request_id, limit=args.limit))
    outbox = _read(store, lambda: _outbox_counts(store, batches or []))
    if batches is None or outbox is None:
        return FAILURE
    rows = [_listed(batch, outbox.get((batch["job_name"], batch["batch_id"]), {})) for batch in batches]
    if args.json:
        sys.stdout.writelines(json.dumps(row, default=str, sort_keys=True) + "\n" for row in rows)
        return SUCCESS
    if not rows:
        sys.stdout.write("no batches in %s\n" % store.target)
        return SUCCESS
    columns = [("JOB", "job"), ("JOB_ID", "job_id"), ("STATUS", "status"), ("ATTEMPTS", "attempts"), ("SCOPE", "scope"),
               ("REQUEST_ID", "request_id"), ("UPDATED", "updated_at"), ("WRITTEN", "rows_written"),
               ("OUTBOX", "outbox"), ("ERROR", "error")]
    cells = [[_cell(key, row[key]) for _, key in columns] for row in rows]
    widths = [max(len(title), *(len(line[index]) for line in cells)) for index, (title, _) in enumerate(columns)]
    for line in [[title for title, _ in columns], *cells]:
        sys.stdout.write("  ".join(text.ljust(width) for text, width in zip(line, widths, strict=True)).rstrip() + "\n")
    return SUCCESS


def _outbox_counts(store, batches):
    """{(job, job id): {status: rows}} of the batches' outbox rows, one query per job."""
    by_job: dict[str, list[str]] = {}
    for batch in batches:
        by_job.setdefault(batch["job_name"], []).append(batch["batch_id"])
    return {(job, batch_id): counts for job, ids in by_job.items()
            for batch_id, counts in store.count_events(job, ids).items()}


def _cell(key, value):
    if key == "outbox":
        return outbox_summary(value)
    if key == "error":
        return _first_line(value)
    return "-" if value in (None, "") else str(value)


def _listed(batch, outbox):
    """A batch as `state list` lists it: `rows_written` counts table and file rows (receipts); apply rows and events
    are in `outbox`, by status (applied, failed, in_flight, pending, ...)."""
    return {"job": batch["job_name"], "job_id": batch["batch_id"], "status": batch["status"],
            "sequence": batch["batch_sequence"], "attempts": batch["attempts"], "scope": batch["scope"],
            "request_id": (batch.get("engine_metadata") or {}).get("request_id"), "updated_at": batch["updated_at"],
            "rows_written": batch["rows_written"], "outbox": outbox, "error": batch["error"]}


def _one_batch(store, args):
    """(the batch the job id names, None) or (None, exit code) after saying why."""
    batches = _read(store, lambda: store.find_batches(job_name=args.job, batch_id=args.job_id, limit=2))
    if batches is None:
        return None, FAILURE
    if not batches:
        sys.stderr.write("no batch %s in %s (see `streamwright state list`)\n" % (args.job_id, store.target))
        return None, FAILURE
    if len(batches) > 1:
        sys.stderr.write("job id %s is in several jobs (%s): give --job\n" % (
            args.job_id, ", ".join(sorted(batch["job_name"] for batch in batches))))
        return None, INVALID_INPUT
    return batches[0], None


def cmd_show(args) -> int:
    if bool(args.job_id) == bool(args.request_id):
        sys.stderr.write("streamwright state show: give a JOB_ID or --request-id (one of them)\n")
        return INVALID_INPUT
    store = _store(args)
    if store is None:
        return INVALID_INPUT
    if args.job_id:
        batch, code = _one_batch(store, args)
        batches = [batch] if batch is not None else None
    else:
        batches = _read(store, lambda: store.find_batches(job_name=args.job, request_id=args.request_id,
                                                          limit=1000))
        code = FAILURE if batches is None else None
        if batches == []:
            sys.stderr.write("no batch of request %s in %s (see `streamwright state list`)\n" % (
                args.request_id, store.target))
            code = FAILURE
    if code is not None:
        return code
    shown = _read(store, lambda: [dict(batch_details(store, batch["job_name"], batch), job=batch["job_name"])
                                  for batch in reversed(batches)])     # a run's batches, oldest first
    if shown is None:
        return FAILURE
    sys.stdout.write(json.dumps(shown[0] if args.job_id else shown, indent=2, default=str, sort_keys=True) + "\n")
    return SUCCESS


def cmd_change(args) -> int:
    """retry, abandon, resend or skip one batch, under its lock tokens (refused while a run of its scope holds them)."""
    store = _store(args)
    if store is None:
        return INVALID_INPUT
    batch, code = _one_batch(store, args)
    if batch is None:
        return code
    action, job_name = args.state_action, batch["job_name"]
    try:
        store.prepare(migrate=False)                       # never change tables a migration is pending on
        lease = store.acquire_lease(job_name, owner_id(), _LEASE_SECONDS, tokens=batch_tokens(batch))
        try:
            sys.stdout.write(_settle(store, lease, batch, action, getattr(args, "output", None)) + "\n")
        finally:
            store.release_lease(lease)
    except StateError as exc:                              # not failed, held by a run, migrations pending, ...
        sys.stderr.write("streamwright state %s: %s\n" % (action, exc))
        return FAILURE
    return SUCCESS


def _settle(store, lease, batch, action, output):
    batch_id = batch["batch_id"]
    if action in ("resend", "skip"):
        count = store.settle_in_flight_events(lease, batch_id, output, resend=action == "resend")
        return "%s: %d held row(s) %s" % (batch_id, count, "will be sent again at the job's next run"
                                          if action == "resend" else "marked failed (UNKNOWN_OUTCOME)")
    if action == "retry":
        resumed = store.retry_batch(lease, batch_id)
        return "%s resumes from %s at the job's next run (attempt %d)" % (batch_id, resumed,
                                                                         batch.get("attempts", 1) + 1)
    store.record_batch(lease, batch_id, "abandoned", batch["manifest"])
    return "%s abandoned" % batch_id


def cmd_leases(args) -> int:
    store = _store(args)
    if store is None:
        return INVALID_INPUT
    leases = _read(store, lambda: store.list_leases(args.job))
    if leases is None:
        return FAILURE
    if args.json:
        sys.stdout.write(json.dumps(leases, indent=2, default=str) + "\n")
        return SUCCESS
    for lease in leases:
        sys.stdout.write("%-48s %-6s epoch=%-4s owner=%s until=%s\n" % (
            lease["token"] or "<job>", "held" if lease["held"] else "free", lease["epoch"], lease["owner_id"],
            lease["expires_at"] or "-"))
    if not leases:
        sys.stdout.write("no lease recorded for %s\n" % args.job)
    return SUCCESS


def cmd_purge(args) -> int:
    store = _store(args)
    if store is None:
        return INVALID_INPUT
    age = args.older_than if args.older_than is not None else datetime.timedelta(days=30)
    cutoff = datetime.datetime.now(datetime.UTC) - age
    try:
        store.prepare(migrate=False)
        purged = store.purge_batches(args.job, cutoff, scope=args.scope)
    except StateError as exc:
        sys.stderr.write("streamwright state purge: %s\n" % exc)
        return FAILURE
    sys.stdout.write("purged %d batch(es) of %s\n" % (purged, args.job))
    return SUCCESS
