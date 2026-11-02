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

"""
The logging options of the commands (--log-level, --log, --log-format, --log-config, --log-max-chars)
and the command's LogSetup made from them.
"""

import argparse
import sys

import yaml

from streamwright.core.runtime import logs


def key_value(text):
    if "=" not in text:
        raise argparse.ArgumentTypeError("expected NAME=VALUE, got %r" % text)
    name, _, value = text.partition("=")
    return name.strip(), value


def _max_chars(text):
    try:
        value = int(text)
    except ValueError:
        value = -1
    if value < 0:
        raise argparse.ArgumentTypeError("expected a number of characters (0: no limit), got %r" % text)
    return value


def _logger_level(text):
    name, _, level = text.partition("=")
    name = name.strip()
    try:
        number = logs.level_number(level) if name and level.strip() else None
    except ValueError:
        number = None
    if number is None:
        raise argparse.ArgumentTypeError("expected NAME=LEVEL, a logger (e.g. streamwright.network, or root) and a "
                                         "level (%s), got %r" % (", ".join(logs.LEVELS), text))
    return name, number


def add_log_options(parser):
    """The logging options of streamwright run and streamwright validate."""
    parser.add_argument("--log-level", type=str.upper, choices=logs.LEVELS, metavar="LEVEL",
                        help="the level of streamwright's loggers (streamwright.source, streamwright.output, ...): "
                             "%s; default INFO. streamwright.network stays at WARNING (or this level, when higher) "
                             "unless --log names it"
                             % ", ".join(logs.LEVELS))
    parser.add_argument("--log", action="append", default=[], type=_logger_level, metavar="NAME=LEVEL",
                        help="the level of any logger by its name (repeatable; root: the root logger, so every "
                             "logger without a level of its own), e.g. streamwright.network=INFO (a line per API "
                             "call), streamwright.network=DEBUG (also headers and bodies), "
                             "google.ads.googleads.client=DEBUG (an SDK's own logs)")
    parser.add_argument("--log-format", choices=logs.FORMATS,
                        help="text (default): [time] LEVEL logger: message; json: one JSON object per line with time "
                             "(UTC), level, logger, message and the line's fields (stream, partition, window, "
                             "request, export, records, pages, duration_ms, status, attempt, bytes, path, table, ...)")
    parser.add_argument("--log-config", metavar="FILE",
                        help="a Python logging configuration (logging.config.dictConfig's schema, version: 1) in YAML "
                             "or JSON, for handlers and formatters of your own (files, syslog, ...); it replaces "
                             "streamwright's handler, and --log-level and --log apply on top. Every handler's lines "
                             "are redacted and cut too. JSON lines: a formatter with \"()\": "
                             "streamwright.core.runtime.logs.JsonFormatter")
    parser.add_argument("--log-max-chars", type=_max_chars, default=logs.DEFAULT_MAX_CHARS, metavar="N",
                        help="cut log messages longer than N characters (bodies, SDK payloads), marking them "
                             "`... [truncated N chars]` (default %d; 0: never)" % logs.DEFAULT_MAX_CHARS)


def log_options():
    """The logging options `streamwright validate` takes, before or after its validation options."""
    parser = argparse.ArgumentParser(prog="streamwright validate", add_help=False, allow_abbrev=False)
    add_log_options(parser)
    return parser


def _read_log_config(path):
    """A --log-config file: a logging.config.dictConfig mapping, in YAML or JSON."""
    with open(path, encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    if not isinstance(config, dict):
        raise ValueError("expected a mapping in logging.config.dictConfig's schema (version: 1, handlers, ...)")
    return config


def log_setup(options):
    """The command's logs.LogSetup from its logging options; None when they cannot be used (reported on stderr)."""
    if options.log_config and options.log_format:
        return _log_error("--log-format is the format of streamwright's own handler, which --log-config replaces: set "
                          "the formatters in the config (JSON lines: \"()\": "
                          "streamwright.core.runtime.logs.JsonFormatter)")
    try:
        config = _read_log_config(options.log_config) if options.log_config else None
        return logs.LogSetup(options.log_level, options.log_format or "text", options.log, config,
                             max_chars=options.log_max_chars)
    except (OSError, ValueError, TypeError, AttributeError, ImportError, yaml.YAMLError) as exc:
        # (what reading the file and logging.config report; its errors name the part, their causes what is wrong)
        cause = " (%s)" % exc.__cause__ if exc.__cause__ is not None else ""
        return _log_error("%s%s%s" % ("--log-config %s: " % options.log_config if options.log_config else "", exc,
                                      cause))


def _log_error(message):
    sys.stderr.write("streamwright: error: %s\n" % message)  # (logging is not set up)
