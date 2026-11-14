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

"""Shared arguments, exit codes and logging for job-kind CLIs."""

import argparse
from types import MappingProxyType

from streamwright.core.cli.log_options import add_log_options
from streamwright.core.config.inputs import parse_duration
from streamwright.core.jobs.inputs import InputError
from streamwright.core.jobs.inputs import parse_set as parse_job_set
from streamwright.core.jobs.locking import DEFAULT_MAX_JOB_ATTEMPTS, FAILED_JOB_POLICIES
from streamwright.core.jobs.runner import DEFAULT_MAX_MESSAGES, MODES

SUCCESS = 0
FAILURE = 1
INVALID_INPUT = 2
INTERRUPTED = 130


def parse_set(text):
    """Parse one argparse NAME=VALUE using the shared jobs implementation."""
    try:
        return next(iter(parse_job_set([text]).items()))
    except InputError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


JOB_OPTION = "job_option"


def resolve_job(parser, args):
    """The job path of JOB or --job JOB (one of them, or both the same); a usage error otherwise."""
    option = getattr(args, JOB_OPTION, None)
    if args.job and option and args.job != option:
        parser.error("pass the job once: JOB or --job JOB")
    args.job = args.job or option
    if not args.job:
        parser.error("a job manifest is required: JOB or --job JOB")
    return args.job


class _Renamed(argparse.Action):
    """An option that was renamed or removed: using it is a usage error that says what to use (hidden from the help)."""

    def __init__(self, option_strings, dest, message, **kwargs):
        self.message = message
        super().__init__(option_strings, dest, nargs="?", help=argparse.SUPPRESS, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        parser.error(self.message)


def add_renamed_option(parser, old, new=None, *, message=None):
    """`old`, an option renamed `new` (or removed: `message` says why): giving it is a usage error."""
    parser.add_argument(old, dest="renamed_" + old.lstrip("-").replace("-", "_"), action=_Renamed,
                        message=message or "%s was renamed %s" % (old, new))


def add_apply_migrations_argument(parser):
    """--apply-state-migrations (args.apply_state_migrations)."""
    parser.add_argument("--apply-state-migrations", action="store_true",
                        help="create or upgrade the state tables before the run (as runtime.state.apply_migrations: "
                             "true does); without it a pending migration fails the run (exit 1): apply them with "
                             "`state migrate JOB`")


def add_job_key_argument(parser, extra=""):
    """--job-key (and the old --run-key, now an error); `extra`: the engine's own note, appended to the help."""
    parser.add_argument("--job-key", metavar="KEY",
                        help="jobs without a consume step: a batch with this key runs once per runtime.state.scope, "
                             "a later run with it does nothing (overrides runtime.state.job_key)" + extra)
    add_renamed_option(parser, "--run-key", "--job-key")


def add_batch_arguments(parser):
    """How many batches a run makes (--once | --drain | --available-now | --follow) and their size."""
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true",
                      help="one batch (default): unfinished batches resume, then one new batch runs")
    mode.add_argument("--drain", action="store_true",
                      help="jobs that consume a broker: batches until a poll finds no message")
    mode.add_argument("--available-now", action="store_true",
                      help="jobs that consume a broker: batches until the messages there at the start are processed, "
                           "later ones wait for the next run (Kafka: the end offsets at the start; a broker without "
                           "them, such as SQS, runs as --drain)")
    mode.add_argument("--follow", action="store_true",
                      help="jobs that consume a broker: keep polling until interrupted, pausing --interval when a poll "
                           "finds nothing")
    parser.add_argument("--interval", metavar="DURATION",
                        help="with --follow: the pause after a poll that found nothing (default 10s; 30s, 5m, 1h)")
    parser.add_argument("--max-messages", type=int, default=DEFAULT_MAX_MESSAGES, metavar="N",
                        help="the most messages one batch takes (default %d; the consume step's max_messages, when "
                             "set, wins); 0: only resume unfinished batches and relay the outbox"
                             % DEFAULT_MAX_MESSAGES)
    return parser


def batch_mode(args):
    """The run mode of add_batch_arguments' options: once, drain, available_now or follow."""
    return next((mode for mode in MODES[1:] if getattr(args, mode, False)), "once")


def batch_problems(args):
    """Problems of add_batch_arguments' options that do not depend on the job."""
    problems = []
    if args.interval is not None:
        if not args.follow:
            problems.append("--interval is the pause of --follow")
        else:
            try:
                parse_duration(args.interval)
            except InputError as exc:
                problems.append("--interval: %s" % exc)
    if args.max_messages < 0:
        problems.append("--max-messages cannot be negative (0: only resume and relay)")
    return problems


def _attempts(text):
    try:
        value = int(text)
    except ValueError:
        value = 0
    if value < 1:
        raise argparse.ArgumentTypeError("give a whole number of at least 1, not %r" % text)
    return value


def add_failed_job_arguments(parser):
    """--failed-job-policy and --max-job-attempts (they override runtime.state.failed_job_policy, max_job_attempts)."""
    parser.add_argument("--failed-job-policy", choices=list(FAILED_JOB_POLICIES),
                        help="what the run does first with a failed job (batch) of its scope (overrides "
                             "runtime.state.failed_job_policy): abandon (default) gives it up, its error kept, and "
                             "runs on; retry resumes it, from where it failed and with its pinned inputs, while it has "
                             "been started fewer than --max-job-attempts times, then stops; block stops the run until "
                             "`state retry` or `state abandon`")
    parser.add_argument("--max-job-attempts", type=_attempts, metavar="N",
                        help="with policy retry: how many times a failed job may be started in all, the first run "
                             "included (overrides runtime.state.max_job_attempts; default %d)"
                             % DEFAULT_MAX_JOB_ATTEMPTS)
    return parser


def add_lookup_exec_argument(parser):
    """--allow-lookup-exec (args.allow_lookup_exec)."""
    parser.add_argument("--allow-lookup-exec", action="store_true",
                        help="let spec.lookups run their commands and Python functions (lookups and transforms of "
                             "type command or function); without it such a lookup stops the run (exit 2)")
    return parser


def add_fan_out_arguments(parser):
    """--allow-lookup-exec, --fan-out-concurrency, --fan-out-on-failure (they override runtime.fan_out)."""
    add_lookup_exec_argument(parser)
    parser.add_argument("--fan-out-concurrency", type=_attempts, metavar="N",
                        help="runtime.fan_out: how many items (accounts) run at once (overrides fan_out.concurrency)")
    parser.add_argument("--fan-out-on-failure", choices=["continue", "stop"],
                        help="runtime.fan_out: after an item fails, continue with the others (default) or start no "
                             "new item (overrides fan_out.on_failure)")
    return parser


def add_lock_arguments(parser):
    """The --state-lock-* options of a run (they override runtime.state.lock)."""
    parser.add_argument("--state-lock-policy", choices=["fail", "wait", "prune"],
                        help="when a lock token is held (overrides runtime.state.lock.policy): fail (stop), wait (up "
                             "to --state-lock-timeout), prune (run the lock.each items that are free, skip the held "
                             "ones; a run left with none is recorded as skipped)")
    parser.add_argument("--state-lock-timeout", metavar="DURATION",
                        help="policy wait: how long to wait for the tokens, in seconds or as a duration (30, 30s, 5m; "
                             "overrides runtime.state.lock.timeout)")
    return parser


# the help of the options every job engine's commands share (the Spark engine adds its own aliases)
JOB_HELP = MappingProxyType({
    "job": "the job manifest: a YAML file, or its folder (job.yaml)",
    "job_option": "the job manifest, as an option (instead of JOB)",
    "overlay": "a YAML overlay merged onto the job, in order (repeatable; a null deletes a key, a list replaces the "
               "job's)",
    "config": "a YAML/JSON file with a `config:` mapping of spec.config values",
    "set": "a spec.config value (repeatable; lists as a,b,c); overrides --config",
    "secrets": "a YAML/JSON file of spec.secrets values; STREAMWRIGHT_SECRET_<NAME> variables also give them",
    "allow_connector": "use only this connector (repeatable); by default any installed one",
    "strict": "treat warnings as errors",
})


def add_job_inputs(parser):
    """JOB (or --job JOB), its overlays and its config values: what every job command takes."""
    parser.add_argument("job", nargs="?", metavar="JOB", help=JOB_HELP["job"])
    parser.add_argument("--job", "-j", dest=JOB_OPTION, metavar="JOB", help=JOB_HELP["job_option"])
    parser.add_argument("--overlay", action="append", default=[], metavar="FILE", help=JOB_HELP["overlay"])
    parser.add_argument("--config", metavar="FILE", help=JOB_HELP["config"])
    parser.add_argument("--set", action="append", default=[], type=parse_set, metavar="NAME=VALUE",
                        help=JOB_HELP["set"])


def add_connector_allowlist(parser):
    parser.add_argument("--allow-connector", action="append", default=[], metavar="NAME",
                        help=JOB_HELP["allow_connector"])


def add_job_arguments(parser, strict=True):
    """The job and its inputs (and log) options of a job command; `strict`: also --strict (commands that validate)."""
    add_job_inputs(parser)
    add_connector_allowlist(parser)
    if strict:
        parser.add_argument("--strict", action="store_true", help=JOB_HELP["strict"])
    add_log_options(parser)
    return parser
