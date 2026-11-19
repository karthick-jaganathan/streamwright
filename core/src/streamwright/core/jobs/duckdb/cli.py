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
import contextlib
import logging
import sys
from types import MappingProxyType

from streamwright.core.cli.help_format import DescriptionFormatter
from streamwright.core.cli.job_state import add_state_commands, job_id_from_job, run_state_command
from streamwright.core.cli.jobs import (
    FAILURE,
    INVALID_INPUT,
    JOB_HELP,
    JOB_OPTION,
    SUCCESS,
    add_apply_migrations_argument,
    add_batch_arguments,
    add_connector_allowlist,
    add_failed_job_arguments,
    add_fan_out_arguments,
    add_input_argument,
    add_job_arguments,
    add_job_key_argument,
    add_lock_arguments,
    batch_mode,
    batch_problems,
    resolve_job,
)
from streamwright.core.cli.log_options import add_log_options, log_setup
from streamwright.core.config.inputs import InputError, parse_duration, read_values_file
from streamwright.core.jobs.duckdb.engine import JobInvalidError, job_state, redactor, run_job, validate_job
from streamwright.core.jobs.inputs import read_config_values
from streamwright.core.jobs.manifest import JobError
from streamwright.core.jobs.payload import PayloadError, read_payload
from streamwright.core.jobs.state import StateError
from streamwright.core.runtime import logs
from streamwright.core.runtime.logs.lines import fields, iso_time, utc_now

__all__ = ["main"]

LOG = logging.getLogger("streamwright.duckdb")


def _err(text):
    """Plain stderr: validation issues (file:line lines) only; a run's or state action's errors are logged."""
    sys.stderr.write(text + "\n")


def _log_error(message, *args, event="error"):
    """An error of a run or state action: a log line (its format, handlers and redaction), after log_setup."""
    LOG.error(message, *args, extra=fields(event=event, status="failed"))


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
    if args.input_source is not None:
        try:
            payload = read_payload(args.input_source, report.job)
        except PayloadError as exc:
            _err("%s: error [bad-payload] %s" % (args.job, exc))
            return FAILURE
        sys.stdout.write("%s: the payload fits spec.input (%d rows)\n" % (args.input_source, len(payload.rows)))
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
                "already_committed": name in result.skipped,
                **({"rejected": result.rejected[name]} if result.rejected.get(name) else {})}
               for name, rows in sorted(result.outputs.items())] if result is not None else []
    summary = {"status": "ok" if failure is None and result is not None and not result.exit_code else "failed",
               "started_at": iso_time(started_at), "finished_at": iso_time(finished_at),
               "duration_s": round((finished_at - started_at).total_seconds(), 3),
               "job": result.job_name if result else None, "manifest": args.job, "dry_run": args.dry_run,
               "job_ids": list(result.batches) if result else [], "outputs": outputs}
    if result is not None and result.fan_out:
        summary.update(fan_out=result.fan_out, passes=result.passes)
    if summary["status"] == "ok" and result.lock_skipped:
        summary["status"] = "skipped"            # the lock policy pruned all the run's work
    if summary["status"] == "failed":
        if failure is not None and not isinstance(failure, Exception):
            error = "interrupted"
        elif failure is not None:
            error = "%s: %s" % (type(failure).__name__, failure)
        else:
            error = result.error or "the run failed"
        summary["error"] = args.log_setup.message(error)
    return summary


def _cmd_run(args):
    problems = [problem for problem in (logs.summary_problem(args.summary), *batch_problems(args)) if problem]
    if problems:
        for problem in problems:
            _log_error("%s", problem, event="run_refused")
        return INVALID_INPUT
    started_at, result, failure, summary_written = utc_now(), None, None, True
    try:
        result = run_job(args.job, config=_config(args), secrets=_secrets(args), overlays=args.overlay,
                         allowed_connectors=_allowed(args), dry_run=args.dry_run, strict=args.strict,
                         state_lock_policy=args.state_lock_policy, state_lock_timeout=args.state_lock_timeout,
                         job_key=args.job_key, mode=batch_mode(args), max_messages=args.max_messages,
                         apply_state_migrations=args.apply_state_migrations,
                         failed_job_policy=args.failed_job_policy, max_job_attempts=args.max_job_attempts,
                         allow_lookup_exec=args.allow_lookup_exec, fan_out_concurrency=args.fan_out_concurrency,
                         fan_out_on_failure=args.fan_out_on_failure, input_source=args.input_source,
                         interval=parse_duration(args.interval or "10s").total_seconds())
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
                _log_error("could not write the summary %s: %s", args.summary, exc, event="run_summary")
                summary_written = False
    if args.dry_run:                       # the plan is what --dry-run asks for: stdout
        sys.stdout.write("Job: %s\n%s\n" % (result.job_name, result.plan))
        return SUCCESS if summary_written else FAILURE
    _log_outcome(result)
    if result.exit_code:
        _log_error("run failed: %s", result.error or "see the log", event="run_failed")
    return result.exit_code or (SUCCESS if summary_written else FAILURE)


def _log_outcome(result):
    """The run's recap as one log line (and one per runtime.fan_out item): its batches, what each output wrote."""
    for entry in result.passes:
        LOG.log(logging.INFO if entry["status"] in ("ok", "skipped") else logging.WARNING, "%s %s %s: %s",
                result.fan_out["as"], entry["item"], entry["status"].replace("_", " "),
                entry.get("error") or "%d batch(es); %s" % (len(entry.get("job_ids") or ()), ", ".join(
                    "%s %s rows" % (name, "?" if rows is None else rows)
                    for name, rows in sorted((entry.get("outputs") or {}).items())) or "nothing to write"),
                extra=fields(event="pass_end", status=entry["status"]))
    if result.lock_skipped:
        LOG.info("job %s skipped: the lock tokens it needs are held (lock policy prune)", result.job_name,
                 extra=fields(event="run_end", status="skipped"))
        return
    parts = []
    for name, rows in sorted(result.outputs.items()):
        if name in result.skipped:
            parts.append("%s already committed in an earlier attempt" % name)
            continue
        locations = [location for location in result.locations.get(name, ()) if location]
        written = "%s %s rows" % (name, "?" if rows is None else rows)
        if len(locations) == 1:
            written += " -> %s" % locations[0]
        elif locations:
            written += " -> %d files (%s)" % (len(locations), ", ".join(locations))
        parts.append(written)
    LOG.info("job %s %s: %d batch(es)%s%s", result.job_name, "failed" if result.exit_code else "finished",
             len(result.batches), " (%s)" % ", ".join(result.batches) if result.batches else "",
             "; " + "; ".join(parts) if parts else ("" if result.exit_code else "; nothing to write"),
             extra=fields(event="run_end", status="failed" if result.exit_code else "ok",
                          outputs=dict(result.outputs)))


@contextlib.contextmanager
def _open_state(args, initialize):
    with job_state(args.job, config=_config(args), secrets=_secrets(args), overlays=args.overlay,
                   allowed_connectors=_allowed(args), initialize=initialize,
                   allow_lookup_exec=args.allow_lookup_exec) as opened:
        yield opened


def _resume(args):
    result = run_job(args.job, config=_config(args), secrets=_secrets(args), overlays=args.overlay,
                     allowed_connectors=_allowed(args), resume_only=True, allow_lookup_exec=args.allow_lookup_exec)
    if result.exit_code:
        _log_error("run failed: %s", result.error or "see the log", event="run_failed")
    return result.exit_code


def _cmd_state(args):
    return run_state_command(args, _open_state, after_retry=_resume)


_STATE_DESCRIPTION = """\
A job's batches, leases and state tables, in its runtime.state (sqlite, or a connection), per runtime.state.scope.
Every action takes the job and its config values: --config/--set select the scope (status, retry) and render the
state connection; give the job's required values as a run does. Only the secrets of the state connection are needed
(--secrets or STREAMWRIGHT_SECRET_<NAME>); retry, which runs the job afterwards, needs them all, and takes that run's
--allow-connector and logging options (--no-run: only mark the batch). A batch's job id is
<job>-<sequence>, or <job>-<scope hash>-<sequence> with a scope. retry and abandon take the batch's lock tokens: they
fail while a run of its scope holds them. `streamwright state migrate|show|check --url URL` works on a database URL
instead."""


def _retry_run_options(parser):
    """The options of the run a retry starts: which connectors it may use, and its logging."""
    add_connector_allowlist(parser)
    add_log_options(parser)


def _inputs(parser, secrets=False, strict=True):
    add_job_arguments(parser, strict=strict)
    if secrets:
        parser.add_argument("--secrets", metavar="FILE", help=JOB_HELP["secrets"])


_DESCRIPTIONS = MappingProxyType({
    "duckdb": """\
Validate, plan and run kind: DuckDBJob manifests in an in-process DuckDB, and manage their batches.
`streamwright validate|plan|run JOB` routes a DuckDBJob here too; this is the explicit form.
Exit codes: 0 success, 1 invalid job or failed run, 2 usage or input error.""",
    "validate": """\
Check a DuckDBJob manifest offline: its structure, inputs, connections (against the installed connectors' contracts),
steps, outputs, references and runtime. No secret values are needed and nothing is connected.

  streamwright duckdb validate jobs/orders.yaml --set client=acme --graph""",
    "plan": """\
Print a valid job's execution plan: its steps and outputs in dependency order, what each reads and writes.

  streamwright duckdb plan jobs/orders.yaml""",
    "run": """\
Run a DuckDBJob: the batches its runtime.state scope left unfinished resume first (a failed one is abandoned by
default; --failed-job-policy retry resumes it, block stops the run), then new batches run.
A job
that consumes a broker (a step with method: consume) polls a batch of messages, commits its
outputs and outbox events, then acknowledges the messages; --drain, --available-now and --follow make more batches.

  streamwright duckdb run jobs/orders.yaml --secrets secrets.yaml --set client=acme
  streamwright duckdb run jobs/order_events.yaml --drain --max-messages 1000 --summary
  streamwright duckdb run jobs/order_events.yaml --follow --interval 30s""",
})


def build_parser(prog="streamwright duckdb"):
    """The `streamwright duckdb` argument parser (validate, plan, run, state)."""
    raw = DescriptionFormatter
    # no prefix matching: --log must not be taken for --log-level
    parser = argparse.ArgumentParser(prog=prog, description=_DESCRIPTIONS["duckdb"], formatter_class=raw,
                                     allow_abbrev=False)
    sub = parser.add_subparsers(dest="subcommand", metavar="COMMAND")
    validate = sub.add_parser("validate", allow_abbrev=False, formatter_class=raw,
                              help="check a DuckDBJob manifest offline (no secrets needed)",
                              description=_DESCRIPTIONS["validate"])
    validate.add_argument("--graph", action="store_true", help="also print the plan of a valid job")
    add_input_argument(validate, "its rows are checked against spec.input too")
    _inputs(validate)
    validate.set_defaults(func=_cmd_validate, parser=validate)
    plan = sub.add_parser("plan", allow_abbrev=False, formatter_class=raw,
                          help="print the job's execution plan (no secrets needed)", description=_DESCRIPTIONS["plan"])
    _inputs(plan)
    plan.set_defaults(func=_cmd_plan, parser=plan)
    run = sub.add_parser("run", allow_abbrev=False, formatter_class=raw,
                         help="run a DuckDBJob: resume unfinished batches, then run new ones",
                         description=_DESCRIPTIONS["run"])
    run.add_argument("--dry-run", action="store_true",
                     help="validate, resolve the inputs and print the plan; connect to nothing, write nothing")
    run.add_argument("--summary", nargs="?", const=logs.LOG_SUMMARY, metavar="FILE",
                     help="also write the run's summary as JSON to FILE, atomically, whatever the outcome: status (ok, "
                          "skipped when the lock policy pruned all its work, or failed), times, its batches' job ids, "
                          "each output's rows and files, and the error; a bare --summary logs it instead, as one INFO "
                          "line of compact JSON (event run_summary)")
    _inputs(run, secrets=True)
    add_job_key_argument(run, "; needs a persistent runtime.state")
    add_input_argument(run, "its rows are the batch's `input` table")
    add_apply_migrations_argument(run)
    add_lock_arguments(run)
    add_failed_job_arguments(run)
    add_fan_out_arguments(run)
    add_batch_arguments(run)
    run.set_defaults(func=_cmd_run, parser=run)
    state = sub.add_parser("state", allow_abbrev=False,
                           help="list, show, retry, abandon or purge a job's batches; its leases; migrate or check "
                                "its state tables")
    add_state_commands(state, description=_STATE_DESCRIPTION, add_run_options=_retry_run_options)
    state.set_defaults(func=_cmd_state)
    return parser


def main(argv=None, prog="streamwright"):
    parser = build_parser(prog)
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return INVALID_INPUT
    problem = job_id_from_job(args, getattr(args, JOB_OPTION, None))
    if problem:
        args.parser.error(problem)
    resolve_job(args.parser, args)
    args.log_setup = log_setup(args)
    if args.log_setup is None:
        return INVALID_INPUT
    try:
        return args.func(args)
    except (InputError, JobError) as exc:
        _log_error("%s", exc)
        return INVALID_INPUT
    except (JobInvalidError, StateError) as exc:
        _log_error("%s", exc, event="state_error" if isinstance(exc, StateError) else "job_invalid")
        return FAILURE
    except Exception as exc:
        if args.subcommand not in ("run", "state"):
            raise
        failed = "run" if args.subcommand == "run" else "state %s" % args.state_command
        _log_error("%s failed: %s: %s", failed, type(exc).__name__, exc, event="%s_failed" % args.subcommand)
        return FAILURE
    finally:
        args.log_setup.close()


if __name__ == "__main__":
    sys.exit(main())
