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

"""RunMetrics: a run's counts and timings, and the progress and summary lines they make."""


import collections
import time

from streamwright.core.runtime.logs.lines import (
    LOG,
    PROGRESS_PAGES,
    PROGRESS_SECONDS,
    describe,
    fields,
    iso_time,
    milliseconds,
    thousands,
    utc_now,
)


def _retries(count):
    return "%s retr%s" % (thousands(count), "y" if count == 1 else "ies")


def _breakdown(counts):
    """" (a: 1, b: 2,345)" for a mapping of counts."""
    return " (%s)" % ", ".join("%s: %s" % (name, thousands(count)) for name, count in counts.items()) if counts else ""


def _seconds(value):
    return None if value is None else round(value, 3)


class StreamMetrics:
    """One stream's counts (RunMetrics.start_stream); as_dict() is its entry in the run summary."""

    def __init__(self, name, mode, partitions, started):
        self.name = name
        self.mode = mode
        self.partitions = partitions
        self.failed_partitions = 0
        self.windows = 0  # the windows read, over all partitions (incremental streams)
        self.pages = 0
        self.requests = collections.OrderedDict()  # request name -> HTTP requests or SDK calls sent, retries included
        self.retries = 0
        self.records_read = 0  # the records of all its requests' pages
        self.exports = collections.OrderedDict()  # export -> records written
        self.started = started
        self.duration = None

    def as_dict(self):
        return collections.OrderedDict([
            ("name", self.name), ("mode", self.mode), ("partitions", self.partitions),
            ("failed_partitions", self.failed_partitions), ("windows", self.windows), ("pages", self.pages),
            ("requests", dict(self.requests)), ("retries", self.retries), ("records_read", self.records_read),
            ("exports", dict(self.exports)), ("duration_s", _seconds(self.duration))])


class _Read:
    """
    The pages one request reads for a unit of work - a window or partition of a page-mode stream, a stream partition
    of a run-mode request: progress lines while it goes, a line when it completes.
    """

    def __init__(self, metrics, where):
        self.metrics = metrics
        self.where = where
        self.stats = metrics.streams.get(where.get("stream"))
        self.pages = self.records = 0
        self.started = self.reported_at = metrics.clock()
        self.reported_pages = 0

    def count(self, pages):
        """The pages (lists of records), counted as they go."""
        for page in pages:
            self.page(len(page))
            yield page

    def page(self, records):
        self.pages += 1
        self.records += records
        if self.stats is not None:
            self.stats.pages += 1
            self.stats.records_read += records
        now = self.metrics.clock()
        if self.pages - self.reported_pages >= self.metrics.progress_pages or \
                now - self.reported_at >= self.metrics.progress_seconds:
            self.reported_pages, self.reported_at = self.pages, now
            LOG.info("stream %r, request %r: %s page(s), %s record(s) so far", self.where.get("stream"),
                     self.where.get("request"), thousands(self.pages), thousands(self.records),
                     extra=fields(event="progress", pages=self.pages, records=self.records,
                                  duration_ms=milliseconds(now - self.started), **self.where))

    def done(self, request=True):
        """
        Logs the completed unit: its pages, records and time (`request`: whether the line names the request). A
        stream's only unit - no partition values, no window - has none: the stream's end says it all.
        """
        if not self.where.get("partition") and not self.where.get("window"):
            return
        seconds = self.metrics.clock() - self.started
        LOG.info("%s: %s page(s), %s record(s), %.1f s", describe(self.where, request), thousands(self.pages),
                 thousands(self.records), seconds, extra=fields(event="read", pages=self.pages, records=self.records,
                                                             duration_ms=milliseconds(seconds), **self.where))


class RunMetrics:
    """
    A run's counts and timings, in one place. The runner reports its streams (start_stream, end_stream), the reads of
    their requests (read), records written and bookmarks; the HTTP clients and the connector context count the requests
    they send (request). It logs at INFO each stream's start and end, completed windows and partitions, the progress
    of reads that keep paging - every `progress_seconds` or `progress_pages` pages - and the run's end (finish), and
    summary() is the run summary (--summary FILE). `clock`: monotonic seconds.
    """

    def __init__(self, clock=time.monotonic, progress_seconds=PROGRESS_SECONDS, progress_pages=PROGRESS_PAGES):
        self.clock = clock
        self.progress_seconds = progress_seconds
        self.progress_pages = progress_pages
        self.started_at = utc_now()
        self.started = clock()
        self.finished_at = None
        self.duration = None
        self.status = None
        self.source = None
        self.streams = collections.OrderedDict()  # name -> StreamMetrics
        self.outputs = []  # the output's summary() entries
        self.state = None  # the last state the run wrote
        self.error = None

    def start_stream(self, name, mode, partitions, requests):
        """A stream starts: `partitions` (a number) and the names of its `requests`."""
        stats = self.streams[name] = StreamMetrics(name, mode, partitions, self.clock())
        LOG.info("stream %r: starting (mode %s, %d partition(s), requests: %s)", name, mode, partitions,
                 ", ".join(requests) or "none", extra=fields(event="stream_start", stream=name))
        return stats

    def end_stream(self, name, completed=True):
        """A stream ends; one that `completed` logs its records written, requests, retries and failed partitions."""
        stats = self.streams.get(name)
        if stats is None:
            return
        stats.duration = self.clock() - stats.started
        if not completed:
            return
        written, requests = sum(stats.exports.values()), sum(stats.requests.values())
        LOG.info("stream %r: %s record(s) written%s, %s request(s)%s, %s, %s failed partition(s), %.1f s", name,
                 thousands(written), _breakdown(stats.exports), thousands(requests), _breakdown(stats.requests),
                 _retries(stats.retries), thousands(stats.failed_partitions), stats.duration,
                 extra=fields(event="stream_end", stream=name, records=written, exports=dict(stats.exports),
                              pages=stats.pages, requests=requests, retries=stats.retries,
                              failed_partitions=stats.failed_partitions, duration_ms=milliseconds(stats.duration)))

    def read(self, where):
        """A unit of work of a request starts (`where`: its stream, request, partition and window)."""
        return _Read(self, where)

    def request(self, where, retry=False):
        """An HTTP request or SDK call was sent for `where` (its stream and request); `retry`: it retries one."""
        stats = self.streams.get((where or {}).get("stream"))
        if stats is None:
            return
        name = where.get("request")
        stats.requests[name] = stats.requests.get(name, 0) + 1
        if retry:
            stats.retries += 1

    def exports(self):
        """Records written per export, over all streams."""
        written = collections.OrderedDict()
        for stats in self.streams.values():
            for export, count in stats.exports.items():
                written[export] = written.get(export, 0) + count
        return written

    def finish(self, status, log=True):
        """The run ends: `status` is ok, failed or interrupted; `log`: log the run's end line."""
        self.status = status
        self.finished_at = utc_now()
        self.duration = self.clock() - self.started
        if not log:
            return
        streams = list(self.streams.values())
        written = self.exports()
        requests = sum(sum(stats.requests.values()) for stats in streams)
        retries = sum(stats.retries for stats in streams)
        failed = sum(stats.failed_partitions for stats in streams)
        LOG.info("run %s %.1f s: %d stream(s), %s record(s) written%s, %s request(s), %s, %s failed partition(s), "
                 "%d output(s) written", "finished in" if status == "ok" else status + " after", self.duration,
                 len(streams), thousands(sum(written.values())), _breakdown(written), thousands(requests),
                 _retries(retries), thousands(failed), len(self.outputs),
                 extra=fields(event="run_end", outcome=status, streams=len(streams), records=sum(written.values()),
                              exports=dict(written), requests=requests, retries=retries, failed_partitions=failed,
                              outputs=len(self.outputs), duration_ms=milliseconds(self.duration)))

    def summary(self):
        """
        The run summary: status (ok or failed), started_at, finished_at, duration_s, source, streams (StreamMetrics),
        outputs (the output's summary() entries), state (the bookmarks the run wrote), and error when it failed.
        """
        if self.finished_at is None:
            self.finish(self.status or "failed", log=False)
        result = collections.OrderedDict([
            ("status", "ok" if self.status == "ok" else "failed"), ("started_at", iso_time(self.started_at)),
            ("finished_at", iso_time(self.finished_at)), ("duration_s", _seconds(self.duration)),
            ("source", self.source),
            ("streams", [stats.as_dict() for stats in self.streams.values()]), ("outputs", list(self.outputs)),
            ("state", self.state)])
        if self.status != "ok":
            result["error"] = self.error or ("interrupted" if self.status == "interrupted" else "the run failed")
        return result
