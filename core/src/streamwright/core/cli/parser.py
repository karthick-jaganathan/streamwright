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

"""The argument parsers of the built-in commands and of installed command plug-ins."""

import argparse

from streamwright.core.cli.commands import jobs, state
from streamwright.core.cli.help_format import DescriptionFormatter
from streamwright.core.cli.jobs import JOB_HELP, JOB_OPTION, add_job_arguments
from streamwright.core.cli.log_options import add_log_options, key_value
from streamwright.core.config.inputs import SECRET_ENV_PREFIX
from streamwright.core.runtime import logs, plugins
from streamwright.core.validation import engine


def build_validate_parser():
    """`streamwright validate --help`: validation options, then the logging options (as streamwright run's)."""
    parser = engine._parser("streamwright validate")
    parser.epilog = ("A job manifest (kind: DuckDBJob, SparkJob) is checked by its engine, with that engine's options: "
                     "streamwright duckdb validate -h, streamwright spark validate -h.")
    add_log_options(parser.add_argument_group("logging options"))
    return parser


_CONNECTORS_DESCRIPTION = """\
Show the installed connectors, or list, find and install connectors from the hub's catalog.

  streamwright connectors                     the installed connectors: version, libraries, DuckDB extensions, loggers
  streamwright connectors postgres            one or more installed connectors, in detail (like pip show)
  streamwright connectors ls                  the catalog and what is installed (-ls, --list)
  streamwright connectors find ads            search the catalog (-s, -f, --search, --find)
  streamwright connectors -i postgres --yes   install from the hub (i KEY ...)"""


def build_plan_parser():
    """`streamwright plan --help`: the command takes a job manifest; its engine's plan command takes the options."""
    parser = argparse.ArgumentParser(
        prog="streamwright plan",
        description="Show a job's execution plan - its steps, outputs and the order they run in - without running it "
                    "or needing secrets. The manifest's kind picks the engine, and its options are that engine's: "
                    "streamwright duckdb plan -h (kind: DuckDBJob), streamwright spark plan -h (kind: SparkJob).")
    parser.add_argument("job", nargs="?", metavar="JOB", help=JOB_HELP["job"])
    parser.add_argument("--job", "-j", dest=JOB_OPTION, metavar="JOB", help=JOB_HELP["job_option"])
    return parser


def build_parser():
    parser = argparse.ArgumentParser(prog="streamwright",
                                     description="Run and check StreamWright sources (`kind: source`) and jobs "
                                                 "(`kind: DuckDBJob`, `kind: SparkJob`).")
    commands = parser.add_subparsers(dest="command", metavar="COMMAND")
    commands.required = True

    run = commands.add_parser("run", help="run a source and write its records, or a job (JOB or --job JOB)",
                              description="Run a source. Records go to stdout as Singer messages unless --output "
                                          "names a folder for files or a warehouse; logs go to stderr. A job "
                                          "manifest (kind: DuckDBJob, SparkJob) runs on its engine, with that "
                                          "engine's options (--secrets, --summary, --apply-state-migrations, the state "
                                          "lock options, --failed-job-policy, --max-job-attempts, and "
                                          "--once/--drain/--available-now/--follow, --interval, --max-messages for "
                                          "jobs that consume a broker): streamwright duckdb run -h, streamwright "
                                          "spark run -h. Options of sources only (--output, --stream, --state, ...) "
                                          "are refused for a job.")
    run.add_argument("source", nargs="?", metavar="SOURCE",
                     help="a source folder (source.yaml and one file per stream in streams/) or a source file; or a "
                          "job manifest (kind: DuckDBJob, SparkJob), which runs on its engine")
    run.add_argument("--job", "-j", dest=JOB_OPTION, metavar="JOB",
                     help="a job manifest (kind: DuckDBJob, SparkJob) to run on its engine, e.g. "
                          "sw run --job jobs/campaigns.yaml (the same as passing it as the path); the job engine's "
                          "options apply: sw duckdb run --help, sw spark run --help")
    run.add_argument("--config", metavar="FILE",
                     help="YAML/JSON file with `config` (values for spec.config) and `streams` (streams/exports to "
                          "run), e.g. one client's settings for the source")
    run.add_argument("--set", action="append", default=[], type=key_value, metavar="NAME=VALUE",
                     help="a spec.config value (repeatable; lists as a,b,c); overrides --config")
    run.add_argument("--secrets", metavar="FILE",
                     help="YAML/JSON file with values for spec.secrets; overrides %s<NAME> environment variables"
                          % SECRET_ENV_PREFIX)
    run.add_argument("--state", metavar="FILE", help="state from a previous run (a JSON file) for incremental streams")
    run.add_argument("--timezone", metavar="TZ",
                     help="the client's IANA time zone (e.g. America/New_York) for `today` and incremental windows, "
                          "so a daily run reads the client's local day; default: the machine's local date")
    run.add_argument("--stream", action="append", dest="streams", metavar="NAME",
                     help="run only this stream, or the stream of this export (repeatable; replaces the --config "
                          "file's `streams`); the streams its `from_stream` partitions come from run too, and every "
                          "stream that runs writes all its exports")
    run.add_argument("--output", default="singer", metavar="singer|jsonl|csv|tsv|parquet|duckdb|ducklake",
                     help="singer (default): messages on stdout; jsonl:DIR, csv:DIR, tsv:DIR or parquet:DIR: one file "
                          "per export plus state.json, written atomically (parquet: typed columns, zstd); "
                          "duckdb:PATH[:SCHEMA] or ducklake:CATALOG[:SCHEMA]: load into a DuckDB file or DuckLake "
                          "catalog (a file, or postgres:DSN for a Postgres catalog; data files on S3 with "
                          "STREAMWRIGHT_DUCKLAKE_DATA_PATH)")
    run.add_argument("--file-name", metavar="TEMPLATE",
                     help="with --output jsonl:DIR, csv:DIR, tsv:DIR or parquet:DIR: the path of each export's file "
                          "inside DIR, with {{ export }}, {{ source }}, {{ today }} (YYYY-MM-DD), {{ timestamp }} "
                          "(the run's start, UTC, YYYYMMDDTHHMMSSZ), {{ format }} (jsonl, csv, tsv or parquet) and "
                          "{{ config.NAME }}, e.g. \"{{ config.client }}/{{ export }}_{{ today }}.{{ format }}\"; "
                          "subfolders are made, every export needs its own path, and a file of the same name is "
                          "replaced; it replaces the `file_name` of the exports in the YAML (default: "
                          "EXPORT.TIMESTAMP.UNIQUE.jsonl, or .csv, .tsv, .parquet)")
    run.add_argument("--allow-connector", action="append", dest="allowed_connectors", metavar="NAME",
                     help="allow only this connector (repeatable); by default any installed connector can be used")
    run.add_argument("--summary", nargs="?", const=logs.LOG_SUMMARY, metavar="FILE",
                     help="also write the run's summary as JSON to FILE, atomically, whatever the outcome: status "
                          "(ok or failed), times, the streams' counts (partitions, windows, pages, requests, retries, "
                          "records read and written), the outputs written, the state's bookmarks and the error; a "
                          "bare --summary logs it instead, as one INFO line of compact JSON (event run_summary)")
    add_log_options(run)

    conn = commands.add_parser("connectors",
                               help="show, list, find or install connectors (bare or NAME ...: the installed "
                                    "connectors, their libraries, DuckDB extensions and loggers)",
                               formatter_class=DescriptionFormatter,
                               description=_CONNECTORS_DESCRIPTION)
    flags = conn.add_mutually_exclusive_group()
    flags.add_argument("-ls", "--list", action="store_true", dest="flag_list",
                       help="list catalog and installed connectors")
    flags.add_argument("-i", "--install", nargs="+", dest="flag_install", metavar="KEY",
                       help="install one or more connector keys")
    flags.add_argument("-s", "-f", "--search", "--find", nargs="+", dest="flag_search", metavar="TEXT",
                       help="find/search connectors in the catalog by keyword")
    conn.add_argument("action", nargs="?", metavar="{ls,i,find}",
                      help="ls (catalog + installed), -i KEY (install), find TEXT (search catalog), or installed "
                           "connector names (show them, like pip show)")
    conn.add_argument("query", nargs="*", metavar="KEY|TEXT",
                      help="connector keys (install, show: one or more) or search words")
    conn.add_argument("--yes", action="store_true", help="install: skip the confirmation prompt")
    conn.add_argument("--hub-url", metavar="URL",
                      help="list, find, install: the connector hub (default: $STREAMWRIGHT_HUB_URL, else the live "
                           "StreamWright hub; when it is unreachable, the cached catalog is used, with a warning)")

    validate = commands.add_parser("validate", help="check sources (schema, references, SQL) with the checks of the "
                                                     "installed connectors and query builders, or a job manifest",
                                   add_help=False)
    validate.add_argument("arguments", nargs=argparse.REMAINDER)

    plan_cmd = commands.add_parser("plan", help="show a job's execution plan: its steps, outputs and their order "
                                                 "(no secrets needed)", add_help=False,
                                   description=build_plan_parser().description)
    plan_cmd.add_argument("arguments", nargs=argparse.REMAINDER)
    installed_commands = {item["name"] for item in plugins.discover(plugins.COMMANDS)}
    if "duckdb" not in installed_commands:  # editable installs have stale entry-point metadata
        jobs.register(commands)
    if "state" not in installed_commands:
        state.register(commands)
    for name in sorted(installed_commands):
        plugin = plugins.load_plugin(plugins.COMMANDS, name)
        if not callable(plugin):
            raise plugins.PluginError("command %r must expose register(subparsers)" % name)
        plugin(commands)
    return parser


def command_parsers():
    """
    Public map of the commands' argument parsers, for clients that inspect options (the MCP server's CLI reference).
    A command that forwards its arguments (`duckdb`, `spark`) is given as its own full parser, with its subcommands;
    `job` holds the options every job command (validate, plan, run, state) takes; --strict is validate, plan and
    run's.
    """
    root = build_parser()
    parsers = next(action.choices for action in root._actions if isinstance(action, argparse._SubParsersAction))
    parsers = dict(parsers)
    for name, parser in list(parsers.items()):
        full = parser.get_default("full_parser")
        if full is not None:
            try:
                parsers[name] = full()
            except Exception as exc:  # an engine that cannot load keeps its forwarding parser
                parsers[name].epilog = "(its options are unavailable: %s)" % exc
    parsers["validate"] = build_validate_parser()
    job = argparse.ArgumentParser(add_help=False)
    add_job_arguments(job, strict=False)
    parsers["job"] = job
    return parsers
