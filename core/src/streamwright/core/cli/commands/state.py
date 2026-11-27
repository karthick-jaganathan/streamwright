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
`streamwright state`: the bookkeeping of every job - DuckDBJob and SparkJob alike - in its state database, named by
--url (or $STREAMWRIGHT_STATE_URL): no job manifest, no config.

    list        the newest batches: job, job id, status, scope, run request id, rows written, outbox rows by status
    show        one batch (JOB_ID) or a run's batches (--request-id): pinned manifest, receipts, every row's outcome
    retry       a failed batch resumes from the status it failed in at the job's next run
    abandon     a batch is given up: its scope runs new batches again
    resend      an apply output's held rows (unknown outcome) are sent again at the next run
    skip        an apply output's held rows are marked failed (UNKNOWN_OUTCOME)
    leases      a job's lease tokens
    purge       a job's old completed, abandoned and skipped batches are deleted (sequences are kept)
    migrations  every migration of the state tables: applied or pending
    migrate     the pending migrations are applied
    check       exit 0 when the tables are current, 1 when a migration is pending
"""

import argparse
import datetime
import sys
from types import MappingProxyType

from streamwright.core.cli.help_format import DescriptionFormatter
from streamwright.core.cli.state_batches import cmd_change, cmd_leases, cmd_list, cmd_purge, cmd_show
from streamwright.core.config.inputs import InputError, parse_duration
from streamwright.core.jobs.state.url import ENV_STATE_URL, migration_runner_for_url

__all__ = ["check", "cmd_change", "cmd_check", "cmd_leases", "cmd_list", "cmd_migrate", "cmd_migrations",
           "cmd_purge", "cmd_show", "migrate", "migrations", "purge_age", "register"]

SUCCESS = 0
FAILURE = 1
INVALID_INPUT = 2


_DESCRIPTION = """\
The bookkeeping of every job - DuckDBJob and SparkJob alike - in its state database (runtime.state: the sw_* tables),
named by --url or $%s: no job manifest and no config. list and show read the batches and each batch's rows (their
outcome: applied or failed, the vendor's result or error), by job id or by the request id of the run that made them
(`req=sw_...` on its log lines); retry, abandon, resend and skip settle a batch; leases and purge look after a job;
migrations, migrate and check look after the tables (a run applies them only with --apply-state-migrations or
runtime.state.apply_migrations: true).""" % ENV_STATE_URL

_URL_HELP = ("the state database: sqlite:///state.db (relative to the working folder), sqlite:////abs/state.db, "
             "duckdb:///state.duckdb (?catalog=NAME), postgresql://user:pass@host:5432/db?schema=streamwright "
             "(other parameters, e.g. sslmode, go to the driver), mysql://user:pass@host:3306/db (the tables live in "
             "database db); percent-encode special characters in the user and password (default: $%s)"
             % ENV_STATE_URL)

_CHANGES = MappingProxyType({
    "retry": ("retry a failed batch: it resumes from the status it failed in, with the config values and lock tokens "
              "it pinned, at the job's next run (run the job to resume it); it counts one more attempt, whatever "
              "runtime.state.max_job_attempts says", "retry"),
    "abandon": ("give a batch up: its scope runs new batches again; what it wrote stays", "abandon"),
    "resend": ("send an apply output's held rows again at the next run: a run stopped mid-call, their outcome is "
               "unknown and the operation is not idempotent; check the vendor first (they may be applied twice)",
               "resend"),
    "skip": ("mark an apply output's held rows (unknown outcome) failed with UNKNOWN_OUTCOME: the next run carries on "
             "without them", "skip"),
})


def purge_age(text):
    """A purge age: days as a number (30), or a duration (30d, 12h)."""
    text = str(text).strip()
    try:
        return datetime.timedelta(days=int(text)) if text.isdigit() else parse_duration(text)
    except InputError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


# * --------
# * commands
# * --------

def register(subparsers):
    """Register the `state` command and its subcommands."""
    parser = subparsers.add_parser(
        "state",
        help="the bookkeeping of every job in its state database (--url): list and show batches and their rows, "
             "retry, abandon, resend, skip, leases, purge; the tables' migrations",
        description=_DESCRIPTION,
        formatter_class=DescriptionFormatter,
    )
    sub = parser.add_subparsers(dest="state_action", required=True, metavar="ACTION")
    raw = DescriptionFormatter

    p_list = sub.add_parser("list", help="the newest batches of every job (or of one)", formatter_class=raw,
                            description="List the newest batches, newest first: job, job id, status, scope, the run's "
                                        "request id, last update, the rows written to tables and files (WRITTEN), "
                                        "the outbox rows by status - apply rows (applied, failed, in_flight, ...) "
                                        "and events (OUTBOX) - and the error.\n\n"
                                        "  streamwright state list --url sqlite:///state.db\n"
                                        "  streamwright state list --url sqlite:///state.db --job orders --status "
                                        "failed\n"
                                        "  streamwright state list --url sqlite:///state.db --job orders --scope "
                                        "client:a --limit 1    # the newest batch of a scope\n"
                                        "  streamwright state list --url sqlite:///state.db --request-id sw_EUAh...")
    _add_url_options(p_list)
    p_list.add_argument("--job", metavar="NAME", help="only this job's batches (its `name`)")
    p_list.add_argument("--scope", metavar="TEXT", help="only this runtime.state.scope (\"\" for none)")
    p_list.add_argument("--status", metavar="STATUS",
                        help="only batches in this status (e.g. failed, completed, abandoned)")
    p_list.add_argument("--request-id", metavar="ID",
                        help="only the batches of the run with this request id (`req=sw_...` in its log lines)")
    p_list.add_argument("--limit", type=int, default=20, metavar="N", help="the most batches to list (default: 20)")
    p_list.add_argument("--json", action="store_true", help="one JSON object per batch instead of a table")
    p_list.set_defaults(command_handler=cmd_list)

    p_show = sub.add_parser("show", help="one batch and every row's outcome, by job id or run request id",
                            formatter_class=raw,
                            description="Show one batch - its pinned manifest, lock tokens, metrics, output receipts "
                                        "and outbox rows (each row's status, attempts, the vendor's result or error "
                                        "code and message) - by job id (one JSON object), or the batches of a run by "
                                        "its request id (a JSON list: a run can make several).\n\n"
                                        "  streamwright state show --url sqlite:///state.db orders-f4bab560-00000004\n"
                                        "  streamwright state show --url sqlite:///state.db --request-id sw_EUAh...")
    _add_url_options(p_show)
    _add_job_id(p_show, required=False)
    p_show.add_argument("--request-id", metavar="ID",
                        help="the request id of the run (`req=sw_...` in its log lines): its batches")
    p_show.set_defaults(command_handler=cmd_show)

    for action, (text, _) in _CHANGES.items():
        p_change = sub.add_parser(action, help=text.split(":")[0], description=text[0].upper() + text[1:] + ".",
                                  formatter_class=raw)
        _add_url_options(p_change)
        _add_job_id(p_change, required=True)
        if action in ("resend", "skip"):
            p_change.add_argument("--output", metavar="NAME",
                                  help="only this apply output's rows (default: every output)")
        p_change.set_defaults(command_handler=cmd_change)

    p_leases = sub.add_parser("leases", help="a job's lease tokens: owner, epoch, expiry", formatter_class=raw,
                              description="List every lease token of a job (scope:<scope> and lock tokens): held or "
                                          "free, its epoch, owner and expiry.")
    _add_url_options(p_leases)
    p_leases.add_argument("--job", metavar="NAME", required=True, help="the job (its `name`)")
    p_leases.add_argument("--json", action="store_true", help="the leases as a JSON list")
    p_leases.set_defaults(command_handler=cmd_leases)

    p_purge = sub.add_parser("purge", help="delete a job's old completed, abandoned and skipped batches",
                             formatter_class=raw,
                             description="Delete a job's completed, abandoned and skipped batches with their receipts "
                                         "and outbox rows; their sequences are kept.")
    _add_url_options(p_purge)
    p_purge.add_argument("--job", metavar="NAME", required=True, help="the job (its `name`)")
    p_purge.add_argument("--older-than", type=purge_age, metavar="AGE",
                         help="batches last updated longer ago than this: days (30) or a duration (30d, 12h); "
                              "default 30 days")
    p_purge.add_argument("--scope", metavar="TEXT", help="only this runtime.state.scope; default: every scope")
    p_purge.set_defaults(command_handler=cmd_purge)

    p_migrations = sub.add_parser("migrations", help="every migration: applied or not, when, and the store identity",
                                  formatter_class=raw,
                                  description="List every migration, applied or pending, with when it was applied, "
                                              "and the store id it was applied to.\n\n"
                                              "  streamwright state migrations --url mysql://sw@db:3306/etl")
    _add_url_options(p_migrations)
    p_migrations.set_defaults(command_handler=cmd_migrations)

    p_migrate = sub.add_parser("migrate", help="apply the pending migrations (idempotent; concurrent runs wait for "
                                               "one another)", formatter_class=raw,
                               description="Create the state tables, or upgrade them: apply the pending migrations "
                                           "in order, each in its own transaction. It is idempotent, and concurrent "
                                           "runs wait for one another.\n\n"
                                           "  streamwright state migrate --url postgresql://sw@db:5432/etl\n"
                                           "  streamwright state migrate --url sqlite:///state.db --dry-run")
    _add_url_options(p_migrate)
    p_migrate.add_argument("--dry-run", action="store_true",
                           help="print the SQL of the pending migrations instead of applying them")
    p_migrate.set_defaults(command_handler=cmd_migrate)

    p_check = sub.add_parser("check", help="exit 0 when every migration is applied, 1 when one is pending (for "
                                           "deployment checks)", formatter_class=raw,
                             description="Check that the state tables are current, for deployment pipelines: exit 0 "
                                         "when every migration is applied, 1 when one is pending (run migrate), 2 "
                                         "when the URL is missing or invalid.\n\n"
                                         "  streamwright state check --url \"$STREAMWRIGHT_STATE_URL\" || exit 1")
    _add_url_options(p_check)
    p_check.set_defaults(command_handler=cmd_check)


def _add_url_options(parser: argparse.ArgumentParser):
    parser.add_argument("--url", metavar="URL", help=_URL_HELP)
    parser.add_argument(
        "--schema",
        metavar="SCHEMA",
        help="the schema of the tables, overriding the URL's ?schema= (default: streamwright; MySQL: the URL's "
             "database; SQLite keeps the tables unprefixed)",
    )


def _add_job_id(parser, *, required):
    parser.add_argument("job_id", nargs=None if required else "?", metavar="JOB_ID",
                        help="the batch's job id, as `state list` shows it")
    parser.add_argument("--job", metavar="NAME", help="the job (its `name`), when a job id is in several jobs")


# * ----------
# * migrations
# * ----------

def _runner(args, existing=True):
    """The URL's MigrationRunner (None: the error is written). `existing`: a SQLite or DuckDB file must be there (only
    a real migrate creates one)."""
    try:
        return migration_runner_for_url(args.url, schema=args.schema, existing=existing)
    except (ValueError, ImportError) as exc:
        sys.stderr.write("streamwright state: %s\n" % exc)
        return None


def migrate(runner, dry_run=False) -> int:
    """Applies a store's pending migrations (or prints their SQL); the exit code."""
    if dry_run:
        plan = runner.apply(dry_run=True)
        if not plan:
            sys.stdout.write("The state tables in %s are up to date: no pending migrations.\n" % runner.target)
        else:
            sys.stdout.write("-- State database: %s\n" % runner.target)
        for item in plan:
            sys.stdout.write("-- Migration %s: %s\n" % (item["name"], item.get("description") or ""))
            sys.stdout.writelines("%s;\n\n" % statement for statement in item["statements"])
        return SUCCESS
    try:
        applied = runner.apply()
    except Exception as exc:  # the database's own error (permissions, connection, ...)
        sys.stderr.write("streamwright state migrate: %s\n" % exc)
        return FAILURE
    if not applied:
        sys.stdout.write("The state tables in %s are up to date: no migrations to apply.\n" % runner.target)
    for record in applied:
        sys.stdout.write("Applied migration %s to %s (store id %s)\n" % (record["name"], runner.target,
                                                                       record["store_id"]))
    return SUCCESS


def migrations(runner) -> int:
    """Lists every migration: applied or not, when, and the store identity."""
    records = runner.show()
    sys.stdout.write("State database: %s\n" % runner.target)
    sys.stdout.write("%-8s %-25s %-9s %-26s %s\n" % ("Version", "Name", "Applied", "Applied at", "Store id"))
    for record in records:
        sys.stdout.write("%-8s %-25s %-9s %-26s %s\n" % (
            record["version"], record["name"], "yes" if record["applied"] else "no", record["applied_at"] or "",
            record["store_id"] or ""))
    return SUCCESS


def check(runner) -> int:
    """Exit 0 when every migration is applied, 1 (listing the pending ones) otherwise."""
    pending = [record["name"] for record in runner.show() if not record["applied"]]
    if pending:
        sys.stdout.write("The state tables in %s need migrations: %s (run migrate)\n" % (runner.target,
                                                                                        ", ".join(pending)))
        return FAILURE
    sys.stdout.write("The state tables in %s are up to date.\n" % runner.target)
    return SUCCESS


def cmd_migrate(args) -> int:
    runner = _runner(args, existing=args.dry_run)
    return INVALID_INPUT if runner is None else migrate(runner, args.dry_run)


def cmd_migrations(args) -> int:
    runner = _runner(args)
    return INVALID_INPUT if runner is None else migrations(runner)


def cmd_check(args) -> int:
    runner = _runner(args)
    return INVALID_INPUT if runner is None else check(runner)
