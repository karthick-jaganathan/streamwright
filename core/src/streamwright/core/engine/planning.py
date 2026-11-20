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

"""Compile and validate a source run before its first request."""

from streamwright.core.engine import rules, sql
from streamwright.core.engine.errors import SourceError
from streamwright.core.engine.values import Distinct
from streamwright.core.runtime import components


def stream_problems(runner, stream):
    """Request, connector and query-builder diagnostics for one stream."""
    problems = []
    for position, item in enumerate(stream.get("requests") or []):
        request, where = components.request_body(item), components.path_text(("requests", position))
        calls = components.sdk_calls(request)
        if calls and runner.connector is None:
            problems.append("%s: an operation that is a call needs a connector `auth.provider`" % where)
            continue
        problems += ["%s: %s" % (where, problem) for call in calls
                     for problem in components.check_call(runner.connector, call, runner.query_builders.builders)]
        problems += ["%s: %s" % (components.path_text(("requests", position, *path)), message)
                     for path, message, _ in components.check_queries(request, runner.allowed_connectors,
                                                                      runner.query_builders.builders)]
        problem = _transport_problem(runner, request, calls)
        if problem:
            problems.append("%s: %s" % (where, problem))
    return problems


def _transport_problem(runner, request, calls):
    if runner.connector is None:
        return None
    transport = runner.transport
    if transport == "http" and calls:
        return rules.transport_problem("sdk", transport, runner.provider)
    if transport != "http" and _http_requests(request):
        return rules.transport_problem("http", transport, runner.provider)
    return None


def _http_requests(request):
    job = request.get("async_job") or {}
    parts = (request, job.get("submit") or {}, job.get("poll") or {}, job.get("results") or {})
    return [part["http"] for part in parts if isinstance(part, dict) and "http" in part]


def prepare_run(runner, selected):
    """Return prerequisite-ordered streams after compiling and validating the selection."""
    unknown = [name for name in selected if name not in runner.streams and name not in runner.export_owner]
    if unknown:
        raise SourceError("unknown stream(s): %s (streams/exports: %s)" % (
            ", ".join(unknown), ", ".join(sorted(set(runner.streams) | set(runner.export_owner)))))
    if runner.plan is None:
        planner = sql.Sandbox()
        try:
            runner.plan = sql.SourcePlan(planner, runner.source)
        finally:
            planner.close()
    running = runner.plan.closure(selected)
    problems = runner.plan.messages(running) + ["stream %r: %s" % (name, problem) for name in running
                                                 for problem in stream_problems(runner, runner.streams[name])]
    if problems:
        raise SourceError("; ".join(problems))
    for name in running:
        for item in runner.streams[name].get("partitions") or []:
            if "from_stream" in item:
                fields, _ = sql.partition_fields(item)
                parent = runner.partitions_planner.parent(item)
                runner.collected.setdefault(parent, {}).setdefault(fields, Distinct())
    return running
