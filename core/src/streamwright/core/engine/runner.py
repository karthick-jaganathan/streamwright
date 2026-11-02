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
Runs a validated source: each selected stream reads its requests, runs its SQL steps (sql.py) and writes its exports
to the output. Streams are self-contained: a step reads only its own stream's requests and earlier steps. A stream
runs after the streams its `from_stream` partitions come from, which run too and write their exports.
"""

import contextlib
import datetime
import logging
import queue
import threading
import time
from dataclasses import dataclass, field

import requests

from streamwright.core.engine import sql
from streamwright.core.engine.bookmarks import BookmarkStore
from streamwright.core.engine.builders import QueryBuilders
from streamwright.core.engine.errors import SourceError
from streamwright.core.engine.execution import PageModeExecutor, RunModeExecutor
from streamwright.core.engine.factories import AuthFactory, ClientFactory
from streamwright.core.engine.partitions import PartitionPlanner
from streamwright.core.engine.planning import prepare_run
from streamwright.core.engine.session import ConnectorSession
from streamwright.core.engine.values import legacy_partition_key, partition_key, partition_value
from streamwright.core.engine.writers import ExportWriter
from streamwright.core.net.http import Redactor
from streamwright.core.outputs.output import BaseOutput
from streamwright.core.runtime import components, logs
from streamwright.core.runtime.connector_types import ConnectorContext
from streamwright.core.runtime.logs import RunMetrics
from streamwright.core.runtime.source_connectors import select_transport
from streamwright.core.runtime.templates import json_numbers

__all__ = ["CollectedStream", "SourceError", "SourceRunner", "partition_key"]

LOG = logging.getLogger("streamwright.source")


@dataclass(frozen=True)
class RunContext:
    source: dict
    config: dict
    secrets: dict
    today: datetime.date
    clock: object
    sleep: object
    redact: Redactor
    metrics: RunMetrics


@dataclass
class CollectedStream:
    """Single-use lazy stream; state and partial become available on exhaustion."""

    produce: object
    state: dict = None
    partial: bool = None
    _used: bool = field(default=False, init=False)
    _result: tuple = field(default=None, init=False)

    def __iter__(self):
        if self._used:
            raise RuntimeError("a collected stream can only be iterated once")
        self._used = True
        return self._records()

    def _records(self):
        pending = queue.Queue(maxsize=64)
        stopped = threading.Event()
        done = object()
        errors = []

        def enqueue(record):
            while not stopped.is_set():
                try:
                    pending.put(record, timeout=0.1)
                    return
                except queue.Full:
                    continue
            raise _CollectionStoppedError()

        def run():
            try:
                self.produce(enqueue, self)
            except _CollectionStoppedError:
                return
            except BaseException as exc:
                # Interruptions must reach the consuming thread, not disappear in the producer.
                errors.append(exc)
            finally:
                with contextlib.suppress(_CollectionStoppedError):
                    enqueue(done)

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        completed = False
        try:
            while True:
                record = pending.get()
                if record is done:
                    completed = True
                    if errors:
                        raise errors[0]
                    self.state, self.partial = self._result
                    return
                yield record
        finally:
            stopped.set()
            worker.join(timeout=None if completed else 0.1)


class _CollectionStoppedError(Exception):
    pass


class _CollectOutput(BaseOutput):
    def __init__(self, enqueue, exports):
        self.enqueue = enqueue
        self.exports = exports
        self.counts = {}
        self.partial = set()

    def write_schema(self, name, schema, keys):
        pass

    def write_record(self, name, record):
        if name in self.exports:
            self.enqueue((name, record))
            self.counts[name] = self.counts.get(name, 0) + 1

    def mark_partial(self, name):
        self.partial.add(name)

    def write_state(self, state):
        pass

    def summary(self):
        return [{"export": name, "records": count} for name, count in self.counts.items()]

    def close(self, failed=False):
        pass


class SourceRunner:
    """
    Runs a source's streams into `output`. `metrics` (logs.RunMetrics; by default one of its own) gets the run's
    counts and timings and logs its progress.
    """

    def __init__(self, source, config, secrets, state=None, output=None, today=None, session=None,  # noqa: PLR0917 (public API)
                 clock=time.monotonic, sleep=time.sleep, redact=None, allowed_connectors=None, metrics=None, plan=None):
        self.source = source
        self.config = config
        self.secrets = secrets
        self.output = output
        self.today = today or datetime.date.today()
        self.redact = redact or Redactor()
        for value in secrets.values():
            self.redact.add(value)
        self.session = session or requests.Session()
        self.clock = clock
        self.sleep = sleep
        self.metrics = metrics if metrics is not None else RunMetrics(clock=clock)
        self.context = RunContext(source, config, secrets, self.today, clock, sleep, self.redact, self.metrics)
        self.bookmarks = BookmarkStore(state, output, self.metrics, self.today, key=partition_key,
                                       legacy_key=legacy_partition_key)
        self.state = self.bookmarks.state
        self.writer = ExportWriter(output, partition_value)
        if self.metrics.source is None:
            self.metrics.source = source.get("name")
        self.streams = {}
        for stream in source["streams"]:
            self.streams.setdefault(stream["name"], stream)
        self.stream_names = {name.lower(): name for name in self.streams}
        self.export_owner = {}  # export -> its stream
        for stream in source["streams"]:
            for export in sql.exports_of(stream):
                self.export_owner.setdefault(export, stream["name"])
        self.plan = plan  # an already compiled plan may be passed by the CLI
        self.allowed_connectors = allowed_connectors
        self.sandbox = None  # the run's DuckDB, opened when the first stream runs
        self.incomplete = {}  # stream -> why it lacks partitions: it skipped some, or a `from_stream` parent lost some
        self.collected = {}  # parent stream -> (fields) -> Distinct rows of their values, for `from_stream`
        self.partitions_planner = PartitionPlanner(self)
        self.page_executor = PageModeExecutor(self)
        self.run_executor = RunModeExecutor(self)
        self.connector_session = ConnectorSession(self)
        self.provider, self.connector, self.connector_auth, self.authenticator = AuthFactory.build(
            source, self._scopes(), secrets, session=self.session, redact=self.redact, allowed=allowed_connectors,
            error_type=SourceError)
        self.transport = select_transport(self.connector) if self.connector is not None else None
        self.query_builders = QueryBuilders(self.allowed_connectors)
        self.http = AuthFactory.render_http(source, self._scopes(), secrets)
        self.clients = ClientFactory(self.context, self.session, self.authenticator, self.http)
        self.shared_limiter = self.clients.shared_limiter

    def _scopes(self, partition=None, window=None):
        scopes = {"config": self.config, "today": self.today}
        if partition:
            scopes["partition"] = partition
        if window:
            scopes["window"] = {"start": window[0], "end": window[1]}
        return scopes

    def run(self, selected=None):
        """
        Runs the `selected` streams (stream or export names; default: every stream) and the streams their
        `from_stream` partitions come from, parents first. Every stream that runs writes all its exports. Every
        stream is checked (SQL, connectors, query builders) before the first request.
        """
        selected = list(selected or list(self.streams))
        try:
            for name in prepare_run(self, selected):
                self._run_stream(self.streams[name])
        finally:
            self.connector_session.close()
            if self.sandbox is not None:
                self.sandbox.close()
                self.sandbox = None
        return self.state

    def collect(self, stream, *, state=None):
        """Lazily yield one stream's ``(export, record)`` pairs, then expose state and partial.

        ``state`` is explicit input; this method does not modify the runner's output or state.
        Exhaust the single-use result to make its final state and partial flag available.
        """
        name = self.export_owner.get(stream, stream)
        if name not in self.streams:
            raise SourceError("unknown stream %r" % stream)
        exports = set(sql.exports_of(self.streams[name]))

        def produce(enqueue, collected):
            output = _CollectOutput(enqueue, exports)
            child = SourceRunner(self.source, self.config, self.secrets, state=state, output=output, today=self.today,
                                 session=self.session, clock=self.clock, sleep=self.sleep, redact=self.redact,
                                 allowed_connectors=self.allowed_connectors, plan=self.plan)
            final_state = child.run([name])
            collected._result = (final_state, name in child.incomplete)

        return CollectedStream(produce)

    def _workspace(self):
        """The run's DuckDB (sql.Sandbox), opened when the first stream runs."""
        if self.sandbox is None:
            self.sandbox = sql.Sandbox()
        return self.sandbox

    def _run_stream(self, stream):
        """Runs one stream (`transform.mode`: page or run) and writes its exports."""
        name = stream["name"]
        plan = self.plan.streams[name]
        for _, _, parent in plan.parents:
            if parent in self.incomplete:  # it gave this stream fewer partitions (parents run first)
                self.incomplete.setdefault(name, "lacks the partitions its parent %r lost" % parent)
        partitions = self.partitions_planner.stream_partitions(stream)
        stats = self.metrics.start_stream(name, plan.mode, len(partitions),
                                          [item["name"] for item in stream.get("requests") or []])
        completed = False
        try:
            if plan.mode == "page":
                self.page_executor.execute(stream, plan, partitions, stats)
            else:
                self.run_executor.execute(stream, plan, partitions, stats)
            completed = True
        finally:
            self.metrics.end_stream(name, completed)

    def _request_setup(self, stream):
        """The stream's HTTP client and connector context (the connector connects once per run, if a request calls
        it)."""
        limiter = self.clients.stream_limiter(stream)
        client = self.clients.client(stream, limiter)
        context = None
        if any(components.sdk_calls(components.request_body(item)) for item in stream.get("requests") or []) and \
                self.connector is not None and self.transport != "http":
            context = ConnectorContext(self.provider, self.redact, retry=self.clients.retry_policy(stream),
                                       rate_limiter=limiter, sleep=self.sleep, metrics=self.metrics, clock=self.clock,
                                       default_codes=self.connector.retry_codes)
            self.connector_session.connect(context)
        return client, context

    def _parameters(self, plan, step):
        """The `$name` parameters of a step: the config values it reads (null when not given)."""
        return {name: self.config.get(name) for name in plan.parameters.get(step, ())}

    def _skip_partition(self, stream, partition, error, stats):
        """Called while handling a partition's error: fails the stream, unless it can skip or warn."""
        name = stream["name"]
        policy = stream.get("on_error", stream.get("on_partition_error", "fail"))
        if policy == "fail":
            raise SourceError(self.redact("stream %r, partition %s: %s" % (name, partition_key(partition), error)))
        stats.failed_partitions += 1
        if policy == "skip":
            self.incomplete.setdefault(name, "skipped partitions")       # warn tolerates without marking it partial
        verb = "skipping" if policy == "skip" else "warning on"
        LOG.warning(self.redact("stream %r: %s partition %s: %s" % (name, verb, partition_key(partition), error)),
                    extra=logs.fields(event="partition_%s" % ("skipped" if policy == "skip" else "warned"),
                                      stream=name, partition=json_numbers(partition)))
