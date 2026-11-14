#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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

"""The `streamwright`/`sw` CLI: source commands plus installed command and job-kind plug-ins."""

import argparse
import os
import sys

import yaml

from streamwright.core.cli.commands import jobs
from streamwright.core.cli.commands.connectors import run_connectors_command
from streamwright.core.cli.commands.source import run
from streamwright.core.cli.jobs import JOB_OPTION
from streamwright.core.cli.log_options import log_options, log_setup
from streamwright.core.cli.parser import build_parser, build_plan_parser, build_validate_parser, command_parsers
from streamwright.core.jobs.engine import engine_name
from streamwright.core.validation import engine
from streamwright.core.validation.engine import NON_STRICT_CODES, check_components

__all__ = ["main"]


def _subcommand(parser, name):
    for action in parser._actions if parser is not None else ():
        if isinstance(action, argparse._SubParsersAction):
            return action.choices.get(name)
    return None


def _options(parser):
    return {option: action for action in (parser._actions if parser is not None else ())
            for option in action.option_strings}


def _extract_all_paths(args, command="run"):
    """
    The positional paths of a job command's arguments, read with the options of the command, of every job command
    and of every engine's command of that name (so `--interval 10s` or `--state-lock-timeout 5m` is not a path).
    """
    parsers = command_parsers()
    options = {}
    for parser in (*(_subcommand(engine_parser, command) for engine_parser in parsers.values()), parsers["job"],
                   parsers.get(command)):
        options.update(_options(parser))
    paths = []
    index = 0
    while index < len(args):
        item = args[index]
        option, equals, value = item.partition("=")
        action = options.get(option)
        if action is not None:
            if action.dest == JOB_OPTION:  # --job JOB: a manifest path too
                if not equals and index + 1 < len(args):
                    value = args[index + 1]
                if value:
                    paths.append(value)
            if action.nargs == "?" and not equals and (index + 1 >= len(args) or args[index + 1].startswith("-")):
                index += 1  # an optional value that is not given (a bare --summary)
                continue
            if not equals and action.nargs != 0:
                index += 1
                if action.nargs in ("+", "*"):
                    while index < len(args) and not args[index].startswith("-"):
                        index += 1
                    continue
                if isinstance(action.nargs, int) and action.nargs > 1:
                    index += action.nargs - 1
            index += 1
            continue
        if item.startswith("-"):
            index += 1
            continue
        paths.append(item)
        index += 1
    return paths


def detect_kind(path):
    """Detects the `kind:` of a manifest at `path` (file or directory with job.yaml / source.yaml)."""
    if not path or not isinstance(path, str):
        return None
    expanded = os.path.expanduser(path)
    target_file = None
    if os.path.isfile(expanded):
        target_file = expanded
    elif os.path.isdir(expanded):
        for candidate in ("job.yaml", "job.yml", "source.yaml", "source.yml"):
            p = os.path.join(expanded, candidate)
            if os.path.isfile(p):
                target_file = p
                break
    if not target_file:
        return None
    try:
        with open(target_file, encoding="utf-8") as f:
            chunk = f.read(65536)
        docs = yaml.safe_load_all(chunk)
        first = next(docs, None)
        if isinstance(first, dict):
            return first.get("kind")
    except Exception:
        try:
            with open(target_file, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if isinstance(data, dict):
                return data.get("kind")
        except Exception:
            return None
    return None


def _validate_command(argv):
    """`streamwright validate`: every other argument is validation's, including options before paths."""
    options, rest = log_options().parse_known_args(argv[1:])
    if any(argument in ("-h", "--help") for argument in rest):
        build_validate_parser().print_help()
        return 0
    setup = log_setup(options)
    if setup is None:
        return 2
    try:
        return engine.main(rest, source_check=check_components, prog="streamwright validate",
                           non_strict_codes=NON_STRICT_CODES)
    finally:
        setup.close()


def _foreign_options(kind, argv):
    """The options of `argv` that the root command has (for a source) but the job engine's command does not."""
    parsers = command_parsers()
    engine = _subcommand(parsers.get(engine_name(kind)), argv[0])
    if engine is None:
        return []
    known = set(_options(engine))
    root = set(_options(build_validate_parser() if argv[0] == "validate" else parsers.get(argv[0])))
    return sorted({item.partition("=")[0] for item in argv[1:]
                   if item.partition("=")[0] in root - known and item.startswith("-")})


def _job_kind(argv):
    """The manifest kind of a job command's path (None: a source, or not a job command); raises ValueError."""
    if argv[0] not in ("run", "validate", "plan"):
        return None
    paths = list(dict.fromkeys(_extract_all_paths(argv[1:], argv[0])))  # JOB and --job JOB may name the same
    kinds = {detect_kind(path) for path in paths} - {None, "source"}
    if len(kinds) > 1 or (kinds and len(paths) > 1):
        raise ValueError("a job command requires one manifest kind and path")
    return kinds.pop() if kinds else None


def _run_job(kind, argv):
    """A job command on its engine, after refusing the options only a source takes."""
    foreign = _foreign_options(kind, argv)
    if foreign:
        sys.stderr.write("streamwright %s: %s only %s to sources, not to a %s (its options: streamwright %s %s "
                         "--help)\n" % (argv[0], ", ".join(foreign), "applies" if len(foreign) == 1 else "apply",
                                         kind, engine_name(kind), argv[0]))
        return 2
    return jobs.run_job_kind(kind, argv)


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    command = argv[0] if argv else None
    try:
        kind = _job_kind(argv) if argv else None
    except ValueError as exc:
        sys.stderr.write("streamwright: %s\n" % exc)
        return 2
    if kind is not None:
        return _run_job(kind, argv)
    if command == "validate":
        return _validate_command(argv)
    if command == "plan":
        if any(argument in ("-h", "--help") for argument in argv[1:]):
            build_plan_parser().print_help()
            return 0
        sys.stderr.write("streamwright: 'plan' needs a job manifest (JOB or --job JOB) of an installed engine's "
                         "kind; see streamwright plan -h\n")
        return 2
    args = build_parser().parse_args(argv)
    if args.command == "connectors":
        return run_connectors_command(args)
    if hasattr(args, "command_handler"):
        return args.command_handler(args)
    return _run_source(args)


def _run_source(args):
    """`streamwright run SOURCE`: a job manifest (JOB or --job JOB) was routed to its engine before."""
    if getattr(args, JOB_OPTION, None):
        sys.stderr.write("streamwright: --job %s is not a job manifest (kind: DuckDBJob, SparkJob); run a source "
                         "as: streamwright run SOURCE\n" % getattr(args, JOB_OPTION))
        return 2
    if not args.source:
        sys.stderr.write("streamwright: run needs a SOURCE, or --job JOB\n")
        return 2
    setup = log_setup(args)
    if setup is None:
        return 2
    try:
        return run(args, setup)
    finally:
        setup.close()


if __name__ == "__main__":
    sys.exit(main())
