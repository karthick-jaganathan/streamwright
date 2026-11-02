"""Shared arguments, exit codes and logging for job-kind CLIs."""

import argparse

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


def add_job_arguments(parser):
    """Add common job manifest and execution options to an argparse parser."""
    parser.add_argument("job", metavar="JOB")
    parser.add_argument("--overlay", action="append", default=[], metavar="FILE")
    parser.add_argument("--config", metavar="FILE")
    parser.add_argument("--set", action="append", default=[], type=parse_set, metavar="NAME=VALUE")
    parser.add_argument("--allow-connector", action="append", default=[], metavar="NAME")
    parser.add_argument("--log-level", metavar="LEVEL")
    parser.add_argument("--strict", action="store_true")
    return parser
