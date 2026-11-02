"""Shared arguments, exit codes and logging for job-kind CLIs."""

import argparse

from streamwright.core.cli.log_options import add_log_options
from streamwright.core.jobs.inputs import InputError
from streamwright.core.jobs.inputs import parse_set as parse_job_set

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


def add_job_arguments(parser):
    """Add common job manifest and execution options to an argparse parser."""
    parser.add_argument("job", nargs="?", metavar="JOB", help="the job manifest (a YAML file or its folder)")
    parser.add_argument("--job", "-j", dest=JOB_OPTION, metavar="JOB", help="the job manifest, as an option")
    parser.add_argument("--overlay", action="append", default=[], metavar="FILE",
                        help="a YAML overlay merged onto the job, in order (repeatable; a null deletes a key, a list "
                             "replaces the job's)")
    parser.add_argument("--config", metavar="FILE", help="a YAML/JSON file of spec.config values")
    parser.add_argument("--set", action="append", default=[], type=parse_set, metavar="NAME=VALUE",
                        help="a spec.config value (repeatable; lists as a,b,c); overrides --config")
    parser.add_argument("--allow-connector", action="append", default=[], metavar="NAME",
                        help="allow only this connector (repeatable); by default any installed connector")
    parser.add_argument("--strict", action="store_true", help="treat warnings as errors")
    add_log_options(parser)
    return parser
