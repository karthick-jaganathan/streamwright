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
  run JOB [--secrets FILE] [--dry-run] [--summary FILE]
  state list|retry|abandon JOB [--batch ID] [--secrets FILE]
  (every command: --config FILE, --set NAME=VALUE, --overlay FILE, --allow-connector NAME, --strict, and the logging
  options of `streamwright run`: --log-level, --log NAME=LEVEL, --log-format, --log-config, --log-max-chars)

The job options, exit codes and logging are the shared ones of `streamwright.core.cli.jobs`: 0 success, 1 invalid
job or failed run, 2 usage or input error. Validation and planning never need secret values.
"""

import argparse
import logging
import sys

from streamwright.core.cli.jobs import FAILURE, INVALID_INPUT, SUCCESS, add_job_arguments, resolve_job
from streamwright.core.cli.log_options import log_setup
from streamwright.core.config.inputs import InputError, read_values_file
from streamwright.core.jobs.duckdb.engine import JobInvalidError, job_state, redactor, run_job, validate_job
from streamwright.core.jobs.inputs import read_config_values
from streamwright.core.jobs.manifest import JobError
from streamwright.core.jobs.runner import owner_id
from streamwright.core.jobs.state import StateError
from streamwright.core.runtime import logs
from streamwright.core.runtime.logs.lines import iso_time, utc_now

__all__ = ["main"]

LOG = logging.getLogger("streamwright.duckdb")


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


def _run_summary(args, result, started_at, failure):
    """The --summary of a job run: status, times, job, batches, each output's rows and files, and the error."""
    finished_at = utc_now()
    outputs = [{"name": name, "rows": rows, "files": [location for location in result.locations.get(name, ())
                                                       if location],
                "already_committed": name in result.skipped}
               for name, rows in sorted(result.outputs.items())] if result is not None else []
    summary = {"status": "ok" if failure is None and result is not None and not result.exit_code else "failed",
               "started_at": iso_time(started_at), "finished_at": iso_time(finished_at),
               "duration_s": round((finished_at - started_at).total_seconds(), 3),
               "job": result.job_name if result else None, "manifest": args.job, "dry_run": args.dry_run,
               "batches": list(result.batches) if result else [], "outputs": outputs}
    if summary["status"] != "ok":
        if failure is not None and not isinstance(failure, Exception):
            error = "interrupted"
        elif failure is not None:
            error = "%s: %s" % (type(failure).__name__, failure)
        else:
            error = result.error or "the run failed"
        summary["error"] = args.log_setup.message(error)
    return summary


def _cmd_run(args):
    problem = logs.summary_problem(args.summary)
    if problem:
        _err("Error: %s" % problem)
        return INVALID_INPUT
    started_at, result, failure, summary_written = utc_now(), None, None, True
    try:
        result = run_job(args.job, config=_config(args), secrets=_secrets(args), overlays=args.overlay,
                         allowed_connectors=_allowed(args), dry_run=args.dry_run, strict=args.strict)
    except BaseException as exc:
        failure = exc
        raise
    finally:
        if args.summary == logs.LOG_SUMMARY:  # a bare --summary: a log line, whatever the outcome
            logs.log_summary(LOG, _run_summary(args, result, started_at, failure))
        elif args.summary:
            try:
                logs.write_summary(args.summary, _run_summary(args, result, started_at, failure))
            except (OSError, TypeError, ValueError) as exc:
                _err("Error: could not write the summary %s: %s" % (args.summary, exc))
                summary_written = False
    sys.stdout.write("Job: %s\n" % result.job_name)
    if args.dry_run:
        sys.stdout.write(result.plan + "\n")
        return SUCCESS if summary_written else FAILURE
    for name, rows in sorted(result.outputs.items()):
        if name in result.skipped:
            sys.stdout.write("  %-28s already committed in an earlier attempt\n" % name)
            continue
        locations = [location for location in result.locations.get(name, ()) if location]
        written = "%s rows" % ("?" if rows is None else rows)
        if len(locations) == 1:
            written += " -> %s" % locations[0]
        elif locations:
            written += " -> %d files" % len(locations)
        sys.stdout.write("  %-28s %s\n" % (name, written))
        if len(locations) > 1:
            sys.stdout.writelines("      %s\n" % location for location in locations)
    if result.exit_code:
        _err("Run failed: %s" % (result.error or "see the log"))
    return result.exit_code or (SUCCESS if summary_written else FAILURE)


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
        parser.add_argument("--secrets", metavar="FILE",
                            help="a YAML file of spec.secrets values (or STREAMWRIGHT_SECRET_<NAME> variables)")


def _build_parser(prog):
    # no prefix matching: --log must not be taken for --log-level
    parser = argparse.ArgumentParser(prog=prog, description="StreamWright DuckDBJob engine.", allow_abbrev=False)
    sub = parser.add_subparsers(dest="subcommand")
    validate = sub.add_parser("validate", allow_abbrev=False, help="check a DuckDBJob manifest (no secrets needed)")
    validate.add_argument("--graph", action="store_true", help="print the plan of a valid job")
    _inputs(validate)
    validate.set_defaults(func=_cmd_validate, parser=validate)
    plan = sub.add_parser("plan", allow_abbrev=False, help="print the execution plan (no secrets needed)")
    _inputs(plan)
    plan.set_defaults(func=_cmd_plan, parser=plan)
    run = sub.add_parser("run", allow_abbrev=False, help="run a DuckDBJob (unfinished batches resume first)")
    run.add_argument("--dry-run", action="store_true", help="check and plan, write nothing")
    run.add_argument("--summary", nargs="?", const=logs.LOG_SUMMARY, metavar="FILE",
                     help="also write the run's summary as JSON to FILE, atomically, whatever the outcome: status (ok "
                          "or failed), times, the batches, each output's rows and files, and the error; a bare "
                          "--summary logs it instead, as one INFO line of compact JSON (event run_summary)")
    _inputs(run, secrets=True)
    run.set_defaults(func=_cmd_run, parser=run)
    state = sub.add_parser("state", allow_abbrev=False, help="list, retry or abandon a job's batches (runtime.state)")
    state.add_argument("action", choices=["list", "retry", "abandon"],
                       help="list: the job's recent batches; retry: run a failed batch again from where it failed; "
                            "abandon: give a failed batch up so the job can run again")
    state.add_argument("--batch", metavar="ID", help="the batch id (retry, abandon)")
    state.add_argument("--limit", type=int, default=20, metavar="N", help="list: the number of batches (default 20)")
    _inputs(state, secrets=True)
    state.set_defaults(func=_cmd_state, parser=state)
    return parser


def main(argv=None, prog="streamwright"):
    parser = _build_parser(prog)
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return INVALID_INPUT
    resolve_job(args.parser, args)
    args.log_setup = log_setup(args)
    if args.log_setup is None:
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
