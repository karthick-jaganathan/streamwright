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
The DuckDBJob command line (registered for `kind: DuckDBJob` in the `streamwright.job_kinds` entry-point group).

  validate JOB [--graph]
  plan JOB
  run JOB [--secrets FILE] [--dry-run]
  state list|retry|abandon JOB [--batch ID] [--secrets FILE]
  (every command: --config FILE, --set NAME=VALUE, --overlay FILE, --allow-connector NAME, --strict, --log-level)

The job options, exit codes and logging are the shared ones of `streamwright.core.cli.jobs`: 0 success, 1 invalid
job or failed run, 2 usage or input error. Validation and planning never need secret values.
"""

import argparse
import sys

from streamwright.core.cli.jobs import FAILURE, INVALID_INPUT, SUCCESS, add_job_arguments
from streamwright.core.config.inputs import InputError, read_values_file
from streamwright.core.jobs.duckdb.engine import JobInvalidError, job_state, redactor, run_job, validate_job
from streamwright.core.jobs.inputs import read_config_values
from streamwright.core.jobs.manifest import JobError
from streamwright.core.jobs.runner import owner_id
from streamwright.core.jobs.state import StateError
from streamwright.core.runtime.logs import LogSetup

__all__ = ["main"]


def _err(text):
    sys.stderr.write(text + "\n")


def _config(args):
    provided = read_config_values(args.config) if args.config else {}
    provided.update(args.set)
    return provided


def _secrets(args):
    secrets = read_values_file(args.secrets, _err) if getattr(args, "secrets", None) else {}
    args.log_setup.redact = redactor(secrets)
    return secrets


def _allowed(args):
    return set(args.allow_connector) if args.allow_connector else None


def _print_issues(report):
    for issue in report.issues:
        _err(str(issue))


def _cmd_validate(args):
    report = validate_job(args.job, overlays=args.overlay, config=_config(args), strict=args.strict,
                          allowed=_allowed(args))
    _print_issues(report)
    if not report.valid:
        return FAILURE
    sys.stdout.write("%s is valid (kind: DuckDBJob)\n" % args.job)
    if args.graph:
        sys.stdout.write(report.plan + "\n")
    return SUCCESS


def _cmd_plan(args):
    report = validate_job(args.job, overlays=args.overlay, config=_config(args), strict=args.strict,
                          allowed=_allowed(args))
    _print_issues(report)
    if not report.valid:
        return FAILURE
    sys.stdout.write(report.plan + "\n")
    return SUCCESS


def _cmd_run(args):
    result = run_job(args.job, config=_config(args), secrets=_secrets(args), overlays=args.overlay,
                     allowed_connectors=_allowed(args), dry_run=args.dry_run, strict=args.strict)
    sys.stdout.write("Job: %s\n" % result.job_name)
    if args.dry_run:
        sys.stdout.write(result.plan + "\n")
        return SUCCESS
    for name, rows in sorted(result.outputs.items()):
        if name in result.skipped:
            sys.stdout.write("  %-28s already committed in an earlier attempt\n" % name)
        else:
            sys.stdout.write("  %-28s %s rows\n" % (name, "?" if rows is None else rows))
    if result.exit_code:
        _err("Run failed: %s" % (result.error or "see the log"))
    return result.exit_code


def _cmd_state(args):
    with job_state(args.job, config=_config(args), secrets=_secrets(args), overlays=args.overlay,
                   allowed_connectors=_allowed(args)) as (job_name, store):
        if args.action == "list":
            for batch in store.list_batches(job_name, limit=args.limit):
                sys.stdout.write("%-32s %-10s %s %s\n" % (batch["batch_id"], batch["status"], batch["updated_at"],
                                                          batch["error"] or ""))
            return SUCCESS
        if not args.batch:
            raise InputError("state %s needs --batch ID" % args.action)
        lease = store.acquire_lease(job_name, owner_id(), 60)
        try:
            if args.action == "retry":
                resumed = store.retry_batch(lease, args.batch)
                sys.stdout.write("%s resumes from %s\n" % (args.batch, resumed))
            else:
                batch = store.get_batch(job_name, args.batch)
                if batch is None:
                    raise StateError("batch %s does not exist" % args.batch)
                store.record_batch(lease, args.batch, "abandoned", batch["manifest"])
                sys.stdout.write("%s abandoned\n" % args.batch)
        finally:
            store.release_lease(lease)
    if args.action == "retry":
        result = run_job(args.job, config=_config(args), secrets=_secrets(args), overlays=args.overlay,
                         allowed_connectors=_allowed(args), resume_only=True)
        if result.exit_code:
            _err("Run failed: %s" % (result.error or "see the log"))
        return result.exit_code
    return SUCCESS


def _inputs(parser, secrets=False):
    add_job_arguments(parser)
    if secrets:
        parser.add_argument("--secrets", help="a YAML file of secret values (or STREAMWRIGHT_SECRET_<NAME>)")


def _build_parser(prog):
    parser = argparse.ArgumentParser(prog=prog, description="StreamWright DuckDBJob engine.")
    sub = parser.add_subparsers(dest="subcommand")
    validate = sub.add_parser("validate", help="check a DuckDBJob manifest (no secrets needed)")
    validate.add_argument("--graph", action="store_true", help="print the plan of a valid job")
    _inputs(validate)
    validate.set_defaults(func=_cmd_validate)
    plan = sub.add_parser("plan", help="print the execution plan (no secrets needed)")
    _inputs(plan)
    plan.set_defaults(func=_cmd_plan)
    run = sub.add_parser("run", help="run a DuckDBJob (unfinished batches resume first)")
    run.add_argument("--dry-run", action="store_true", help="check and plan, write nothing")
    _inputs(run, secrets=True)
    run.set_defaults(func=_cmd_run)
    state = sub.add_parser("state", help="list, retry or abandon a job's batches (runtime.state)")
    state.add_argument("action", choices=["list", "retry", "abandon"])
    state.add_argument("--batch", help="the batch id (retry, abandon)")
    state.add_argument("--limit", type=int, default=20, help="batches to list")
    _inputs(state, secrets=True)
    state.set_defaults(func=_cmd_state)
    return parser


def main(argv=None, prog="streamwright"):
    parser = _build_parser(prog)
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return INVALID_INPUT
    try:
        args.log_setup = LogSetup(args.log_level)
    except ValueError as exc:
        _err("Error: %s" % exc)
        return INVALID_INPUT
    try:
        return args.func(args)
    except (InputError, JobError) as exc:
        _err("Error: %s" % exc)
        return INVALID_INPUT
    except (JobInvalidError, StateError) as exc:
        _err("Error: %s" % exc)
        return FAILURE
    except Exception as exc:
        if args.subcommand not in ("run", "state"):
            raise
        _err("Run failed: %s: %s" % (type(exc).__name__, exc))     # run_job redacted it already
        return FAILURE
    finally:
        args.log_setup.close()


if __name__ == "__main__":
    sys.exit(main())
