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

"""State database CLI commands: migrate, show, check (StreamWright state management)."""

import argparse
import sys

from streamwright.core.cli.help_format import DescriptionFormatter
from streamwright.core.jobs.state.url import ENV_STATE_URL, migration_runner_for_url

__all__ = ["check", "cmd_check", "cmd_migrate", "cmd_show", "migrate", "register", "show"]

SUCCESS = 0
FAILURE = 1
INVALID_INPUT = 2


_DESCRIPTION = """\
The bookkeeping tables of runtime.state (sw_jobs, sw_leases, sw_sequences, sw_receipts, sw_events, sw_signatures,
sw_requests - the run queue of `streamwright serve` - and sw_migrations), in a database named by --url or $%s. A job
run checks them and fails while a migration is pending; it applies them itself only with --apply-state-migrations or
runtime.state.apply_migrations: true. These commands let a deployment apply them ahead of time and check them.
A job's batches are listed, retried and abandoned with `streamwright duckdb state` and `sw spark state`.""" % \
    ENV_STATE_URL

_URL_HELP = ("the state database: sqlite:///state.db (relative to the working folder), sqlite:////abs/state.db, "
             "duckdb:///state.duckdb (?catalog=NAME), postgresql://user:pass@host:5432/db?schema=streamwright "
             "(other parameters, e.g. sslmode, go to the driver), mysql://user:pass@host:3306/db (the tables live in "
             "database db); percent-encode special characters in the user and password (default: $%s)"
             % ENV_STATE_URL)


def register(subparsers):
    """Register the `state` command and its subcommands."""
    parser = subparsers.add_parser(
        "state",
        help="create, upgrade and check the state tables of runtime.state (migrate, show, check)",
        description=_DESCRIPTION,
        formatter_class=DescriptionFormatter,
    )
    sub = parser.add_subparsers(dest="state_action", required=True, metavar="ACTION")

    raw = DescriptionFormatter
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

    p_show = sub.add_parser("show", help="list every migration: applied or not, when, and the store identity",
                            formatter_class=raw,
                            description="List every migration, applied or pending, with when it was applied, and the "
                                        "store id it was applied to.\n\n"
                                        "  streamwright state show --url mysql://sw@db:3306/etl")
    _add_url_options(p_show)
    p_show.set_defaults(command_handler=cmd_show)

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


def _runner(args):
    try:
        return migration_runner_for_url(args.url, schema=args.schema)
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


def show(runner) -> int:
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
    runner = _runner(args)
    return INVALID_INPUT if runner is None else migrate(runner, args.dry_run)


def cmd_show(args) -> int:
    runner = _runner(args)
    return INVALID_INPUT if runner is None else show(runner)


def cmd_check(args) -> int:
    runner = _runner(args)
    return INVALID_INPUT if runner is None else check(runner)
