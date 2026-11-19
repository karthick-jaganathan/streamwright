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
The `state` commands of a job engine (`streamwright duckdb state`, `sw spark state`): one implementation, each
engine supplying how its state store is opened and what a retry does next. Every action takes the job: JOB (or --job),
--overlay, --config, --set, and --secrets (only the state connection's).

    list     [--limit N] [--scope TEXT] [--json]   the newest batches
    status                                          the newest batch of the job's scope
    show     JOB_ID                                  one batch: pinned manifest, lock tokens, metrics, receipts, outbox
    retry    JOB_ID [--no-run]                       retry a failed batch from the status it failed in
    abandon  JOB_ID                                  a batch is given up
    resend   JOB_ID [--output NAME]                  apply rows with an unknown outcome are sent again by the next run
    skip     JOB_ID [--output NAME]                  apply rows with an unknown outcome are marked failed
    leases   [--json]                                every lease token of the job
    purge    [--older-than 30d] [--scope TEXT]       terminal batches are deleted (sequences are kept)
    migrate  [--dry-run]                             the state tables are created or upgraded
    check                                            exit 1 while a migration is pending
"""

import argparse
import datetime
import json
import sys
from types import MappingProxyType

from streamwright.core.cli.commands.state import check as check_migrations
from streamwright.core.cli.commands.state import migrate as apply_migrations
from streamwright.core.cli.help_format import DescriptionFormatter, sentence
from streamwright.core.cli.jobs import add_job_inputs, add_lookup_exec_argument
from streamwright.core.cli.log_options import log_defaults
from streamwright.core.config.inputs import InputError, parse_duration
from streamwright.core.jobs.locking import batch_tokens
from streamwright.core.jobs.runner import owner_id

__all__ = ["JOB_ID_ACTIONS", "add_state_commands", "job_id_from_job", "purge_age", "run_state_command"]

SUCCESS, FAILURE = 0, 1
_STATE_SECRETS_HELP = ("a YAML/JSON file of spec.secrets values (or STREAMWRIGHT_SECRET_<NAME> variables); only the "
                       "state connection's are needed")
_RUN_SECRETS_HELP = ("a YAML/JSON file of spec.secrets values (or STREAMWRIGHT_SECRET_<NAME> variables): all of them, "
                     "since the job runs after the retry (--no-run: only the state connection's)")
JOB_ID_ACTIONS = ("show", "retry", "abandon", "resend", "skip")
_PUBLIC_KEYS = MappingProxyType({"batch_id": "job_id", "batch_sequence": "sequence"})   # store keys -> output
_NO_INITIALIZE = ("migrate", "check")    # these look at the tables as they are
_LEASE_SECONDS = 60
_LIST_COLUMNS = "%-*s %-10s %8s %8s  %-*s %-24s %8s  %s\n"


def purge_age(text):
    """A purge age: days as a number (30), or a duration (30d, 12h)."""
    text = str(text).strip()
    try:
        return datetime.timedelta(days=int(text)) if text.isdigit() else parse_duration(text)
    except InputError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _add_job_id(parser, what):
    parser.add_argument("job_id", nargs="?", metavar="JOB_ID",
                        help="the job id of the batch to %s, as `state list` shows it" % what)


def _add_inputs(parser, secrets_help):
    add_job_inputs(parser)
    parser.add_argument("--secrets", metavar="FILE", help=secrets_help)
    add_lookup_exec_argument(parser)


def _actions(retry_runs):
    then = (", then the job runs its unfinished batches (--no-run: only mark it)" if retry_runs
            else ", at the job's next run")
    retry_help = ("retry a failed batch: it resumes from the status it failed in, with the config values and lock "
                  "tokens it pinned" + then + "; it counts one more attempt, and is allowed whatever "
                  "runtime.state.max_job_attempts says")

    def list_options(parser):
        parser.add_argument("--limit", type=int, default=20, metavar="N", help="how many batches (default 20)")
        parser.add_argument("--scope", metavar="TEXT",
                            help="only this runtime.state.scope (\"\" for batches without one); default: every scope")
        parser.add_argument("--json", action="store_true", help="one JSON object per batch instead of columns")

    def retry_options(parser):
        _add_job_id(parser, "retry")
        if retry_runs:
            parser.add_argument("--no-run", action="store_true", help="mark the batch for retry, do not run the job")

    def settle_options(parser, what):
        _add_job_id(parser, what)
        parser.add_argument("--output", metavar="NAME", help="only this apply output's rows (default: every output)")

    def purge_options(parser):
        parser.add_argument("--older-than", type=purge_age, metavar="AGE",
                            help="delete completed, abandoned and skipped batches last updated longer ago than this: "
                                 "days (30) or a duration (30d, 12h); default 30 days")
        parser.add_argument("--scope", metavar="TEXT", help="only this runtime.state.scope; default: every scope")

    return (
        ("list", "list the newest batches: job id, status, sequence, scope, last update, rows, error", list_options),
        ("status", "show the newest batch of the job's scope (the --config/--set values select it)", lambda p: None),
        ("show", "show one batch: its pinned manifest (inputs), lock tokens, metrics and output receipts",
         lambda p: _add_job_id(p, "show")),
        ("retry", retry_help, retry_options),
        ("abandon", "give a batch up: its scope runs new batches again; what it wrote stays",
         lambda p: _add_job_id(p, "abandon")),
        ("resend", "send an apply output's held rows again at the next run: a run stopped mid-call, their outcome "
                   "is unknown and the operation is not idempotent; check the vendor first (they may be applied "
                   "twice)", lambda p: settle_options(p, "resend")),
        ("skip", "mark an apply output's held rows (unknown outcome) failed with UNKNOWN_OUTCOME: the next run "
                 "carries on without them", lambda p: settle_options(p, "skip")),
        ("leases", "list every lease token of the job (scope:<scope> and lock tokens): owner, epoch, expiry",
         lambda p: p.add_argument("--json", action="store_true", help="the leases as a JSON list")),
        ("purge", "delete old completed, abandoned and skipped batches with their receipts (sequences are kept)",
         purge_options),
        ("migrate", "create or upgrade the job's state tables (a run does it only with --apply-state-migrations or "
                    "runtime.state.apply_migrations: true)",
         lambda p: p.add_argument("--dry-run", action="store_true",
                                  help="print the SQL of the pending migrations instead of applying them")),
        ("check", "exit 0 when the job's state tables are up to date, 1 when a migration is pending", lambda p: None),
    )


def add_state_commands(parser, *, description, add_run_options=None):
    """
    The state subcommands under `parser`, the same for every engine: each takes the job (JOB, --overlay, --config,
    --set), the state connection's --secrets and its own options. `add_run_options(subparser)`: the engine runs the
    job after a retry; it adds the options of that run to retry (which also gets --no-run). The others log with the
    defaults and use every installed connector.
    """
    parser.description = description
    parser.formatter_class = DescriptionFormatter
    sub = parser.add_subparsers(dest="state_action", metavar="ACTION", required=True)
    commands = {}
    for name, text, add_options in _actions(add_run_options is not None):
        command = sub.add_parser(name, help=text, description=sentence(text), formatter_class=DescriptionFormatter,
                                 allow_abbrev=False)
        runs = name == "retry" and add_run_options is not None
        _add_inputs(command, _RUN_SECRETS_HELP if runs else _STATE_SECRETS_HELP)
        add_options(command)
        if runs:
            add_run_options(command)
        else:
            command.set_defaults(allow_connector=None, **log_defaults())
        command.set_defaults(state_command=name, parser=command)
        commands[name] = command
    return commands


def job_id_from_job(args, job_option):
    """
    `state retry --job JOB JOB_ID`: with the job given as an option, the one positional is the job id; checks that
    the actions that need one have it (in args.job_id).
    """
    if getattr(args, "state_command", None) not in JOB_ID_ACTIONS:
        return None
    if job_option and args.job and args.job_id is None and args.job != job_option:
        args.job_id, args.job = args.job, None
    if args.job_id:
        return None
    return ("state %s acts on one batch: give its job id after the job (state %s JOB JOB_ID, or -j JOB JOB_ID); "
            "`state list JOB` lists the job ids and `state status JOB` shows the newest batch of the job's scope"
            % (args.state_command, args.state_command))


def _line(batch, widths):
    metadata = batch.get("engine_metadata") or {}
    extra = " ".join("%s=%s" % (label, metadata[key]) for key, label in (("app_id", "app"), ("driver_host", "host"))
                     if metadata.get(key))
    return _LIST_COLUMNS % (widths[0], batch["batch_id"], batch["status"], batch["batch_sequence"],
                            batch.get("attempts", 1), widths[1],
                            batch["scope"] or "-", batch["updated_at"] or "", batch["rows_written"],
                            " ".join(part for part in (_first_line(batch["error"]), extra) if part))


def _first_line(error, limit=120):
    """The error's first line, cut to `limit` characters (state show has the whole text)."""
    line = (error or "").strip().split("\n", 1)[0]
    return line if len(line) <= limit else line[:limit - 3] + "..."


def _list(store, job_name, scope, args, limit=None):
    batches = store.list_batches(job_name, limit or args.limit, scope=scope)
    if getattr(args, "json", False):
        sys.stdout.writelines(json.dumps(_public(batch), default=str, sort_keys=True) + "\n" for batch in batches)
        return SUCCESS
    if not batches:
        sys.stdout.write("no batches%s\n" % ("" if scope is None else " in scope %r" % scope))
        return SUCCESS
    widths = (max(len("JOB_ID"), *(len(batch["batch_id"]) for batch in batches)),
              max(len("SCOPE"), *(len(batch["scope"] or "-") for batch in batches)))
    sys.stdout.write(_LIST_COLUMNS % (widths[0], "JOB_ID", "STATUS", "SEQUENCE", "ATTEMPTS", widths[1], "SCOPE",
                                      "UPDATED", "ROWS", "ERROR"))
    sys.stdout.writelines(_line(batch, widths) for batch in batches)
    return SUCCESS


def _public(record):
    """A store record with the state tables' names: job_id and sequence (the Python API says batch_id)."""
    return {_PUBLIC_KEYS.get(key, key): value for key, value in record.items()}


def _show(store, job_name, batch):
    manifest = dict(batch["manifest"])
    if manifest.get("messages"):
        manifest["messages"] = "%d message(s)" % len(manifest["messages"])
    shown = dict(_public(batch), manifest=manifest,
                 receipts={name: _public(receipt)
                           for name, receipt in store.get_receipts(job_name, batch["batch_id"]).items()},
                 outbox=[_public(row) for row in store.list_events(job_name, batch["batch_id"])])
    sys.stdout.write(json.dumps(shown, indent=2, default=str, sort_keys=True) + "\n")
    return SUCCESS


def _change(store, job_name, batch, args, *, scope, retry_runs):
    if args.state_command == "retry" and retry_runs and not args.no_run and batch["scope"] != scope:
        raise InputError("job id %s belongs to scope %r, the job's config selects %r: give the config values of its "
                         "scope (or --no-run to only mark it)" % (batch["batch_id"], batch["scope"], scope))
    lease = store.acquire_lease(job_name, owner_id(), _LEASE_SECONDS, tokens=batch_tokens(batch))
    try:
        if args.state_command in ("resend", "skip"):
            count = store.settle_in_flight_events(lease, batch["batch_id"], args.output,
                                                  resend=args.state_command == "resend")
            sys.stdout.write("%s: %d held row(s) %s\n" % (batch["batch_id"], count, "will be sent again"
                             if args.state_command == "resend" else "marked failed (UNKNOWN_OUTCOME)"))
        elif args.state_command == "retry":
            resumed = store.retry_batch(lease, batch["batch_id"])
            sys.stdout.write("%s resumes from %s (attempt %d)\n" % (batch["batch_id"], resumed,
                                                                   batch.get("attempts", 1) + 1))
        else:
            store.record_batch(lease, batch["batch_id"], "abandoned", batch["manifest"])
            sys.stdout.write("%s abandoned\n" % batch["batch_id"])
    finally:
        store.release_lease(lease)
    return SUCCESS


def _leases(store, job_name, args):
    leases = store.list_leases(job_name)
    if args.json:
        sys.stdout.write(json.dumps(leases, indent=2, default=str) + "\n")
        return SUCCESS
    for lease in leases:
        sys.stdout.write("%-48s %-6s epoch=%-4s owner=%s until=%s\n" % (
            lease["token"] or "<job>", "held" if lease["held"] else "free", lease["epoch"], lease["owner_id"],
            lease["expires_at"] or "-"))
    if not leases:
        sys.stdout.write("no lease recorded for %s\n" % job_name)
    return SUCCESS


def _purge(store, job_name, args):
    age = args.older_than if args.older_than is not None else datetime.timedelta(days=30)
    cutoff = datetime.datetime.now(datetime.UTC) - age
    sys.stdout.write("purged %d batch(es)\n" % store.purge_batches(job_name, cutoff, scope=args.scope))
    return SUCCESS


# the actions that need no batch: (store, job name, the job's scope, args) -> exit code
_JOB_ACTIONS = MappingProxyType({
    "migrate": lambda store, job_name, scope, args: apply_migrations(store.migrations(), args.dry_run),
    "check": lambda store, job_name, scope, args: check_migrations(store.migrations()),
    "list": lambda store, job_name, scope, args: _list(store, job_name, args.scope, args),
    "status": lambda store, job_name, scope, args: _list(store, job_name, scope, args, limit=1),
    "leases": lambda store, job_name, scope, args: _leases(store, job_name, args),
    "purge": lambda store, job_name, scope, args: _purge(store, job_name, args),
})


def run_state_command(args, open_state, *, after_retry=None):
    """
    Runs a state subcommand. `open_state(args, initialize)` is a context manager of (job name, store, scope);
    `after_retry(args)` runs the job after a retry (its exit code), unless --no-run.
    """
    command = args.state_command
    with open_state(args, command not in _NO_INITIALIZE) as (job_name, store, scope):
        if command in _JOB_ACTIONS:
            return _JOB_ACTIONS[command](store, job_name, scope, args)
        batch = store.get_batch(job_name, args.job_id)
        if batch is None:
            sys.stderr.write("no job id %s in job %s (see state list)\n" % (args.job_id, job_name))
            return FAILURE
        if command == "show":
            return _show(store, job_name, batch)
        code = _change(store, job_name, batch, args, scope=scope, retry_runs=after_retry is not None)
    if command == "retry" and after_retry is not None and not args.no_run:
        return after_retry(args)
    return code
