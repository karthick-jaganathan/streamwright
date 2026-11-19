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
GET /metrics (serve.yaml `metrics: {enabled: true}`): Prometheus text. The queue's gauges are read from the state
store, so every host reports the same values (the jobs the caller may call); the HTTP counters are this process's.

    sw_queue_runs{job,status}                    runs queued or running now
    sw_queue_oldest_seconds{job}                 age of the oldest queued run (0: none)
    sw_recent_runs{job,status}                   runs that ended in the last WINDOW_SECONDS
    sw_recent_queue_wait_seconds_avg|_max{job}   their wait: queued until first claimed
    sw_recent_run_seconds_avg|_max{job}          their run: first claimed until ended
    sw_http_requests_total{route,method,status}  requests this process answered
    sw_http_request_seconds_sum|_count{route,method}
"""

import collections
import datetime
import threading

__all__ = ["CONTENT_TYPE", "WINDOW_SECONDS", "HttpMetrics", "render"]

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
WINDOW_SECONDS = 300
_ACTIVE = ("queued", "running")
_ENDED = ("succeeded", "failed")


class HttpMetrics:
    """This process's request counts and durations, by route template (never a raw path: run ids)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counts, self._seconds = collections.Counter(), collections.defaultdict(lambda: [0.0, 0])

    def record(self, route, method, status, seconds):
        with self._lock:
            self._counts[(route, method, int(status))] += 1
            total = self._seconds[(route, method)]
            total[0] += seconds
            total[1] += 1

    def snapshot(self):
        with self._lock:
            return dict(self._counts), {key: tuple(value) for key, value in self._seconds.items()}


def _label(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _line(name, labels, value):
    inner = ",".join('%s="%s"' % (key, _label(item)) for key, item in labels)
    return "%s{%s} %s" % (name, inner, ("%.6f" % value).rstrip("0").rstrip(".") if isinstance(value, float)
                          else value)


def _metric(lines, name, kind, text, samples):
    lines.extend(("# HELP %s %s" % (name, text), "# TYPE %s %s" % (name, kind)))
    lines.extend(_line(name, labels, value) for labels, value in samples)


def _seconds(start, end):
    if not start or not end:
        return None
    return (datetime.datetime.fromisoformat(end) - datetime.datetime.fromisoformat(start)).total_seconds()


def _spread(values):
    values = [value for value in values if value is not None]
    return (sum(values) / len(values), max(values)) if values else (0.0, 0.0)


def _queue(lines, jobs, stats, now):
    active = {(job, status): (runs, oldest) for job, status, runs, oldest in stats["active"]}
    _metric(lines, "sw_queue_runs", "gauge", "Runs queued or running now, by job and status.",
            [((("job", job), ("status", status)), active.get((job, status), (0, None))[0])
             for job in jobs for status in _ACTIVE])
    _metric(lines, "sw_queue_oldest_seconds", "gauge", "Age of the oldest queued run, by job (0: none).",
            [((("job", job),), round(_seconds(oldest, now) or 0.0, 3) if oldest else 0.0)
             for job in jobs for oldest in [active.get((job, "queued"), (0, None))[1]]])
    ended = collections.defaultdict(list)
    for job, status, created, started, finished in stats["finished"]:
        ended[job].append((status, _seconds(created, started), _seconds(started, finished)))
    _metric(lines, "sw_recent_runs", "gauge", "Runs that ended in the last %d s, by job and status." % WINDOW_SECONDS,
            [((("job", job), ("status", status)), sum(1 for item in ended[job] if item[0] == status))
             for job in jobs for status in _ENDED])
    for name, index, text in (("sw_recent_queue_wait_seconds", 1, "queued until first claimed"),
                              ("sw_recent_run_seconds", 2, "first claimed until ended")):
        spreads = {job: _spread(item[index] for item in ended[job]) for job in jobs}
        for suffix, position in (("avg", 0), ("max", 1)):
            _metric(lines, "%s_%s" % (name, suffix), "gauge", "The %s of runs that ended in the last %d s: %s." % (
                "average" if suffix == "avg" else "longest", WINDOW_SECONDS, text),
                [((("job", job),), round(spreads[job][position], 3)) for job in jobs])


def _http(lines, http):
    counts, seconds = http.snapshot()
    _metric(lines, "sw_http_requests_total", "counter", "Requests this process answered.",
            [((("route", route), ("method", method), ("status", status)), count)
             for (route, method, status), count in sorted(counts.items())])
    lines.extend(("# HELP sw_http_request_seconds Time this process took to answer requests.",
                  "# TYPE sw_http_request_seconds summary"))
    for (route, method), (total, count) in sorted(seconds.items()):
        labels = (("route", route), ("method", method))
        lines.extend((_line("sw_http_request_seconds_sum", labels, round(total, 6)),
                      _line("sw_http_request_seconds_count", labels, count)))


def render(store, jobs, http, now=None):
    """The metrics text for `jobs` (the caller's job names) from `store` (the run queue) and `http`."""
    now = now or datetime.datetime.now(datetime.UTC)
    stats = store.queue_stats(now - datetime.timedelta(seconds=WINDOW_SECONDS))
    lines = []
    _queue(lines, sorted(jobs), stats, now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    _http(lines, http)
    return "\n".join(lines) + "\n"
