"""Job-kind commands: a manifest's kind runs its installed JobEngine, and `duckdb` is the DuckDBJob shorthand."""

import argparse
import sys

from streamwright.core.jobs.engine import load_job_engine
from streamwright.core.runtime import plugins


def register(subparsers):
    parser = subparsers.add_parser("duckdb", help="validate, plan and run DuckDBJob manifests and manage their "
                                                  "batches (the explicit form of run|validate|plan JOB)",
                                   add_help=False)
    parser.add_argument("-h", "--help", action="store_true", help="show the streamwright duckdb help")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    parser.set_defaults(command_handler=lambda args: execute(["--help"] if args.help else args.arguments))


def execute(arguments):
    return run_job_kind("DuckDBJob", arguments, prog="streamwright duckdb")


def run_job_kind(kind, argv, prog=None):
    """Runs the installed JobEngine of manifest `kind` on the original arguments (no engine is imported by core)."""
    try:
        job_engine = load_job_engine(kind)
    except plugins.PluginError as exc:
        sys.stderr.write("streamwright: %s\n" % exc)
        return 2
    # `prog`: the command's name in usage lines (`streamwright duckdb`), for engines whose main takes it
    return (job_engine.main(argv, prog=prog) if prog else job_engine.main(argv)) or 0
