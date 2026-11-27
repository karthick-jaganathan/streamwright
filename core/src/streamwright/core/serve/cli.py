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
`streamwright serve SERVE_YAML` and `streamwright worker SERVE_YAML`: the job CLI over HTTP (serve) and the processes
that run what it queues (worker). Both read the same serve.yaml; run as many workers (or worker processes) as runs
should go at once. A new queue's tables are created at start; an older one is upgraded by `streamwright queue migrate`.
"""

import argparse
import importlib
import logging
import signal
import threading

from streamwright.core.cli.help_format import DescriptionFormatter
from streamwright.core.cli.log_options import add_log_options, log_setup, read_log_config
from streamwright.core.jobs.state import MigrationsPendingError, StateError
from streamwright.core.runtime.logs.sql import sql_values
from streamwright.core.serve.config import ServiceError, load_service
from streamwright.core.serve.queue_store import open_queue
from streamwright.core.serve.service import Service
from streamwright.core.serve.worker import Worker, default_worker_id

__all__ = ["register_serve", "register_worker"]

LOG = logging.getLogger("streamwright.serve")
SUCCESS, FAILURE, INVALID_INPUT = 0, 1, 2
_EXTRA = 'the serve extra is not installed: pip install "streamwright[serve]"'
_SERVE = """\
Serve the jobs SERVE_YAML lists over HTTP: the job CLI as a REST API (POST /v1/jobs/JOB/runs is `duckdb run`,
its body's `set` is --set and `input` is --input). Callers authenticate with an API key or a JWT; serve.yaml says
which jobs each may call and which inputs a request may set (every other value is the service's). Runs are queued
in the state store and run by `streamwright worker`. A non-loopback `bind` needs an `auth` section.

  streamwright serve serve.yaml
  curl -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \\
       -d '{"set": {"client_id": "1001"}}' http://127.0.0.1:8080/v1/jobs/client_feed_upload/runs"""
_WORKER = """\
Run the runs `streamwright serve` queued for the jobs SERVE_YAML lists, --processes at a time (each in a process of
its own), until stopped (SIGTERM or Ctrl-C: the runs under way are finished first, as their processes ignore them;
SIGKILL stops all at once). Run workers on several hosts for more runs at once; limits.concurrency caps a job across
all of them. A worker that dies leaves its runs to another worker, which resumes them.

  streamwright worker serve.yaml --processes 8"""


def _common(parser):
    parser.add_argument("serve_yaml", metavar="SERVE_YAML", help="the service: its jobs, callers and limits")
    add_log_options(parser.add_argument_group("logging options"))


def register_serve(subparsers):
    parser = subparsers.add_parser("serve", help="serve jobs over HTTP: the job CLI as a REST API",
                                   description=_SERVE, formatter_class=DescriptionFormatter)
    _common(parser)
    parser.set_defaults(command_handler=cmd_serve)


def _positive(text):
    if not text.isdigit() or int(text) < 1:
        raise argparse.ArgumentTypeError("expected a whole number of at least 1, got %r" % text)
    return int(text)


def register_worker(subparsers):
    parser = subparsers.add_parser("worker", help="run the jobs `streamwright serve` queued",
                                   description=_WORKER, formatter_class=DescriptionFormatter)
    parser.add_argument("--processes", type=_positive, default=1, metavar="N",
                        help="runs at once in this worker, each in a process of its own (default 1)")
    _common(parser)
    parser.set_defaults(command_handler=cmd_worker)


def _service(args):
    """(Service, None) or (None, exit code) with the problems logged."""
    try:
        config = load_service(args.serve_yaml)
        store = open_queue(config.state)
        if not any(record["applied"] for record in store.migrations().show()):
            LOG.info("creating the run queue's tables in %s", store.target)     # a new queue; an older one waits
            store.initialize()                                                   # for `state migrate`
        store.prepare(migrate=False)
        return Service(config, store), None
    except ServiceError as exc:
        for problem in exc.problems:
            LOG.error("%s", problem, extra={"event": "serve_invalid"})
        return None, INVALID_INPUT
    except (MigrationsPendingError, ValueError) as exc:
        LOG.error("%s", exc)
        return None, INVALID_INPUT
    except StateError as exc:
        LOG.error("the state store: %s", exc)
        return None, FAILURE


def _with_logging(args, command):
    setup = log_setup(args)
    if setup is None:
        return INVALID_INPUT
    try:
        return command(args)
    finally:
        setup.close()


def _serve(args):
    service, failed = _service(args)
    if service is None:
        return failed
    try:
        uvicorn = importlib.import_module("uvicorn")
        app = importlib.import_module("streamwright.core.serve.app").create_app(service)
    except ImportError:
        LOG.error("%s", _EXTRA)
        return INVALID_INPUT
    LOG.info("serving %d job(s) on http://%s:%d", len(service.jobs), service.config.host, service.config.port)
    with sql_values(False):                       # serve logs no row values: SQL lines keep their placeholders
        uvicorn.run(app, host=service.config.host, port=service.config.port, log_config=None, access_log=False,
                    server_header=False, date_header=False)
    return SUCCESS


def _worker(args):
    service, failed = _service(args)
    if service is None:
        return failed
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    log_settings = {"level": args.log_level, "log_format": args.log_format or "text", "loggers": args.log,
                    "config": read_log_config(args.log_config) if args.log_config else None,
                    "max_chars": args.log_max_chars}
    base = default_worker_id()
    workers = [Worker(service.jobs, service.store, log_settings=log_settings,
                      worker_id=None if args.processes == 1 else "%s/%d" % (base, number))
               for number in range(1, args.processes + 1)]
    threads = [threading.Thread(target=worker.run, args=(stop,), name=worker.worker_id) for worker in workers[1:]]
    for thread in threads:
        thread.start()
    workers[0].run(stop)                          # the main thread too: it is the one signals reach
    for thread in threads:
        thread.join()
    return SUCCESS


def cmd_serve(args):
    return _with_logging(args, _serve)


def cmd_worker(args):
    return _with_logging(args, _worker)

