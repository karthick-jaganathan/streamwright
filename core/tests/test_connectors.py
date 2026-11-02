import datetime
import gzip
import io
import json
import logging
import sys
import zipfile
from unittest.mock import MagicMock

import pytest
import yaml

from streamwright.core import cli
from streamwright.core.engine.runner import SourceError, SourceRunner
from streamwright.core.io.api_calls import make_caller
from streamwright.core.net import downloads
from streamwright.core.runtime import components, plugins
from streamwright.core.runtime.components import ComponentLoadError, ConnectorError, QueryBuilder
from streamwright.core.runtime.testing import FakeClock, MemoryOutput, page_stream
from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation
from streamwright.core.validation import engine

TODAY = datetime.date(2026, 10, 3)
VENDOR = None  # the FakeVendor of the running test (the `fake` fixture sets it)


# * ---------------------------
# * a fake connector and a source
# * ---------------------------

class FakeSdkError(Exception):

    def __init__(self, message, code):
        super().__init__(message)
        self.code = code


class FakeVendor:
    """Answers `handlers[method](arguments) -> responses`; `failures[method]` are raised by the next calls."""

    def __init__(self):
        self.handlers, self.failures, self.calls, self.connected, self.streams = {}, {}, [], [], []


def translate_error(exc):
    if isinstance(exc, FakeSdkError):
        return ConnectorError(str(exc), code=exc.code, retryable=exc.code == "BUSY")
    return None


class FakeSession:
    def __init__(self, vendor):
        self.vendor = vendor

    def read(self, operation, arguments, context):
        vendor = self.vendor
        vendor.calls.append({"service": operation.service, "method": operation.method, "arguments": dict(arguments)})
        vendor.streams.append((context.where or {}).get("stream"))

        def send():
            if vendor.failures.get(operation.method):
                raise vendor.failures[operation.method].pop(0)
            return vendor.handlers[operation.method](arguments)
        yield from make_caller(context, translate_error)(send)

    def close(self):
        self.vendor = None


class FakeSourceAdapter:
    def open(self, connection, context):
        VENDOR.connected.append(dict(connection))
        if connection["token"] == "expired-token":
            raise ConnectorError("token %s was rejected" % connection["token"], code="AUTH")
        return FakeSession(VENDOR)


def check_region(connection):
    region = connection.get("region")
    return [] if region in (None, "eu", "us") else ["region %r is not served" % region]


class FakeQueryBuilder(QueryBuilder):
    """`{fake_query: {select: [...], where: {field: value}}}` -> SELECT a, b WHERE field = 'value' (quotes doubled)."""

    def check(self, spec):
        if not isinstance(spec, dict):
            return [((), "expected a mapping with `select`")]
        return [] if isinstance(spec.get("select"), list) else [(("select",), "`select` must be a list of fields")]

    def build(self, spec):
        if spec.get("fail"):
            raise ValueError("cannot build %s" % spec["fail"])
        if spec.get("not_text"):
            return 42
        where = " AND ".join("%s = '%s'" % (key, str(value).replace("'", "''"))
                             for key, value in (spec.get("where") or {}).items())
        return "SELECT %s%s" % (", ".join(spec["select"]), " WHERE " + where if where else "")


def _reports(method, *arguments):
    return Operation(service="Reports", method=method, arguments=arguments)


FAKE = ConnectorDefinition(
    name="fake", title="Fake", family="test",
    connection=(Field("token", "string", required=True, secret=True), Field("region", "string")),
    operations=(_reports("list", Field("account", ("string", "mapping")), Field("query", "string")),
                _reports("submit", Field("account", "string")), _reports("status", Field("id", ("string", "integer"))),
                _reports("rows", Field("report", "string"))),
    adapters={"source": __name__ + ":FakeSourceAdapter"},
    validator=__name__ + ":check_region",
    query_builders={"fake_query": __name__ + ":FakeQueryBuilder"},
)


@pytest.fixture
def fake(monkeypatch):
    vendor = FakeVendor()
    monkeypatch.setattr(sys.modules[__name__], "VENDOR", vendor)
    components.register(FAKE)
    yield vendor
    components.unregister("fake")


SUPPORTED = "Reports.list, Reports.submit, Reports.status, Reports.rows"
ROWS = "SELECT (record->>'id')::BIGINT AS id, partition->>'account' AS account FROM records"


def fake_stream(name="rows", request=None, select=ROWS, records=("rows",), **stream):
    """A page-mode stream: one sdk request `records` per account (records at `records[0]`, None: the responses)."""
    request = request or {"sdk": "fake", "service": "Reports", "method": "list", "arguments": {
        "account": "{{ partition.account }}",
        "query": {"fake_query": {"select": ["clicks"], "where": {"name": "{{ config.name }}"}}}}}
    stream.setdefault("partitions", [{"name": "account", "values": ["a1", "a2"]}])
    return page_stream(name, request, select, records={"path": records[0]} if records else None, **stream)


def fake_source(**stream):
    return {"kind": "source", "name": "demo", "auth": {"provider": "fake", "token": "{{ secrets.token }}"},
            "streams": [fake_stream(**stream)]}


def request_of(source, stream=0, position=0):
    return source["streams"][stream]["requests"][position]


def run(source, token="secret-token-1", clock=None):
    output, clock = MemoryOutput(), clock or FakeClock()
    SourceRunner(source, {"name": "it's"}, {"token": token}, output=output, today=TODAY, clock=clock,
                 sleep=clock.sleep).run()
    return output


# * ------------
# * sdk requests
# * ------------

def test_sdk_requests_are_rendered_and_queries_built(fake):
    fake.handlers["list"] = lambda arguments: [{"rows": [{"id": "1"}]}, {"rows": []}, {"rows": {"id": 2}}]
    output = run(fake_source())
    assert [r for _, r in output.records] == [{"id": 1, "account": "a1"}, {"id": 2, "account": "a1"},
                                              {"id": 1, "account": "a2"}, {"id": 2, "account": "a2"}]
    assert fake.calls[0] == {"service": "Reports", "method": "list", "arguments": {
        "account": "a1", "query": "SELECT clicks WHERE name = 'it''s'"}}
    assert fake.connected == [{"token": "secret-token-1"}]  # one connection per run


def test_rendered_dates_in_text_arguments_are_iso_text(fake):
    """`{{ today }}` renders to a date: a text argument gets its ISO text (it failed the rendered-argument check)."""
    fake.handlers["submit"] = lambda arguments: [{"rows": [{"id": 1}]}]
    source = fake_source()
    request_of(source).update({"method": "submit", "arguments": {"account": "{{ today }}"}})
    run(source)
    assert fake.calls[0]["arguments"] == {"account": TODAY.isoformat()}


def test_each_request_reads_with_its_own_streams_context(fake):
    """The session opens once, with the first stream's context; each read uses the context of its own stream."""
    fake.handlers["list"] = lambda arguments: [{"rows": [{"id": 1}]}]
    source = fake_source()
    source["streams"].append(fake_stream("quota", retry={"codes": ["QUOTA"], "max_attempts": 2,
                                                         "backoff": "constant"}))
    output, clock = MemoryOutput(), FakeClock()
    runner = SourceRunner(source, {"name": "n"}, {"token": "t"}, output=output, today=TODAY, clock=clock,
                          sleep=clock.sleep)
    runner.run(["rows"])
    fake.failures["list"] = [FakeSdkError("over quota", "QUOTA")]  # retried only by the `quota` stream's policy
    runner.run(["quota"])
    assert fake.streams == ["rows", "rows", "quota", "quota"] and clock.sleeps == [1]
    stats = runner.metrics.streams
    assert stats["rows"].requests == {"records": 2} and (stats["quota"].requests, stats["quota"].retries) == (
        {"records": 3}, 1)  # the `requests` of the run summary, counted for the stream that reads


def test_connector_errors_follow_the_retry_policy(fake):
    fake.handlers["list"] = lambda arguments: [{"rows": [{"id": 1}]}]
    fake.failures["list"] = [FakeSdkError("busy", "BUSY"), FakeSdkError("over quota", "QUOTA")]
    clock = FakeClock()
    output = run(fake_source(retry={"codes": ["BUSY", "QUOTA"], "max_attempts": 3, "backoff": "constant"}),
                 clock=clock)
    assert len(output.records) == 2 and clock.sleeps == [1, 1]

    # a stream's codes replace the defaults: BUSY, flagged transient by the connector, is no longer retried
    fake.failures["list"] = [FakeSdkError("busy", "BUSY")]
    with pytest.raises(SourceError, match="busy"):
        run(fake_source(retry={"codes": ["QUOTA"]}))

    fake.failures["list"] = [FakeSdkError("bad request for secret-token-1", "INVALID")]
    with pytest.raises(SourceError) as error:
        run(fake_source())
    assert "partition" in str(error.value) and "bad request for ***" in str(error.value)

    fake.failures["list"] = [FakeSdkError("bad request", "INVALID")]
    output = run(fake_source(on_partition_error="skip"))
    assert [r["account"] for _, r in output.records] == ["a2"]


def test_exceptions_that_are_not_api_errors_fail_the_run(fake):
    fake.failures["list"] = [ZeroDivisionError("a bug")]
    with pytest.raises(ZeroDivisionError):
        run(fake_source(on_partition_error="skip"))


def test_run_time_checks_the_declared_auth_and_the_resolved_values_before_connecting(fake):
    fake.handlers["list"] = lambda arguments: [{"rows": [{"id": 1}]}]
    source = fake_source()
    source["auth"]["region"] = "{{ config.region }}"

    def run_in(region, secrets):
        SourceRunner(source, {"name": "n", "region": region}, secrets, output=MemoryOutput(), today=TODAY).run()
    run_in("eu", {"token": "secret-token-1"})
    assert fake.connected == [{"token": "secret-token-1", "region": "eu"}]
    with pytest.raises(SourceError, match="region 'mars' is not served"):
        run_in("mars", {"token": "secret-token-1"})
    assert len(fake.connected) == 1
    source["auth"]["token"] = "secret-token-1"                 # a written credential is refused at run time too
    with pytest.raises(SourceError, match=r"auth.token: must be one \{\{ secrets.NAME \}\} reference"):
        run_in("eu", {})


def test_connect_failures_stop_the_run(fake):
    with pytest.raises(SourceError) as error:
        run(fake_source(on_partition_error="skip"), token="expired-token")
    assert "fake: cannot connect: token *** was rejected" in str(error.value) and fake.calls == []


def test_requests_and_auth_must_match(fake):
    with pytest.raises(SourceError, match=r"http requests need a built-in `auth.type`"):
        run(fake_source(request={"http": {"path": "/rows"}}))
    with pytest.raises(SourceError, match=r"auth.token: is required"):
        SourceRunner(dict(fake_source(), auth={"provider": "fake"}), {"name": "x"}, {}, output=MemoryOutput()).run()


# * ---------------------
# * registry and checks
# * ---------------------

def test_connectors_are_found_through_entry_points(tmp_path, monkeypatch):
    (tmp_path / "streamwright_test_connector.py").write_text(
        "from streamwright.core.sdk.contract import ConnectorDefinition\n\n"
        "CONNECTOR = ConnectorDefinition('entry_test', 'Entry', 'test', connection=(), operations=(), adapters={})\n\n"
        "class OldConnector:\n    name = 'old'\n")
    dist = tmp_path / "streamwright_test_connector-0.1.dist-info"
    dist.mkdir()
    (dist / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: streamwright-test-connector\nVersion: 0.1\n"
        "StreamWright-SDK-Version: 1.0\n")
    (dist / "entry_points.txt").write_text(
        "[streamwright.connectors]\nentry_test = streamwright_test_connector:CONNECTOR\n"
        "broken = streamwright_test_connector:Missing\nwrong = streamwright_test_connector:CONNECTOR\n"
        "old = streamwright_test_connector:OldConnector\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        assert {"entry_test", "broken", "wrong", "old"} <= set(components.available())
        definition = components.load("entry_test")
        assert definition.name == "entry_test" and components.load("entry_test") is definition
        with pytest.raises(ComponentLoadError, match=r"could not be loaded .*AttributeError"):
            components.load("broken")
        with pytest.raises(ComponentLoadError, match="'wrong' defines the connector 'entry_test'"):
            components.load("wrong")
        with pytest.raises(ComponentLoadError, match=r"old \(streamwright-test-connector\) targets the pre-contract "
                                                     r"connector API; it needs an update to connector contract v1"):
            components.load("old")
        with pytest.raises(ComponentLoadError, match="install it with: pip install streamwright-not-there"):
            components.load("not_there")
        with pytest.raises(ComponentLoadError, match="not in the allowed list"):
            components.load("entry_test", allowed=["google_ads"])
    finally:
        components.unregister("entry_test")


def test_query_builders_of_connectors_that_are_not_allowed_are_never_imported(fake, monkeypatch):
    untrusted = MagicMock()
    untrusted.name = "untrusted"
    monkeypatch.setattr(plugins, "entry_point_source", lambda: {plugins.CONNECTORS: [untrusted]})
    assert components.query_builders(allowed=["fake"]) == {"fake_query": FAKE}
    untrusted.load.assert_not_called()
    assert components.query_builders(allowed=["other"]) == {}


def test_sources_are_checked_against_their_connector(fake):
    source = fake_source()
    source["auth"] = {"provider": "fake", "tokn": "x"}
    request_of(source)["service"] = "Admin"
    assert components.check_source(source) == [
        "auth.token: is required",
        "auth.tokn: unknown vendor argument (did you mean 'token'?)",
        "stream 'rows': requests[0]: fake: Admin.list is not supported (supported: " + SUPPORTED + ")"]
    assert components.check_source(fake_source(), allowed=["other"]) == [
        "connector 'fake' is not in the allowed list (other)"]
    assert components.check_source(fake_source()) == []


def test_every_request_of_a_stream_is_checked_as_streamwright_run_checks_it(fake):
    source = fake_source(request={"sdk": "fake", "service": "Reports", "method": "list", "arguments": {}})
    stream = source["streams"][0]
    stream["transform"]["mode"] = "run"
    stream["requests"].append({"name": "more", "sdk": "fake", "service": "Admin", "method": "list",
                               "records": {"path": "rows"}, "partitions": [{"name": "kind", "values": ["x"]}],
                               "arguments": {"query": {"fake_query": {"where": {}}}}})
    assert components.component_problems(source) == [
        (("streams", 0, "requests", 1, "arguments", "query", "fake_query", "select"),
         "fake_query: `select` must be a list of fields", False),
        (("streams", 0, "requests", 1), "fake: Admin.list is not supported (supported: " + SUPPORTED + ")", False)]
    messages = components.check_source(source)
    assert messages == [
        "stream 'rows': requests[1].arguments.query.fake_query.select: fake_query: `select` must be a list of fields",
        "stream 'rows': requests[1]: fake: Admin.list is not supported (supported: " + SUPPORTED + ")"]
    with pytest.raises(SourceError) as error:  # a run checks the same, before any call
        run(source)
    assert all(message in str(error.value) for message in messages) and fake.calls == []


# * ----------
# * async jobs
# * ----------

def zipped(files):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, text in files.items():
            archive.writestr(name, text.encode("utf-8-sig"))
    return buffer.getvalue()


def job_source(**job):
    spec = {"submit": {"sdk": "fake", "service": "Reports", "method": "submit",
                       "arguments": {"account": "{{ partition.account }}"}},
            "poll": {"method": "status", "arguments": {"id": "{{ submit.result }}"}, "every": "10s",
                     "timeout": "1m", "done_when": {"path": "Status", "equals": "Success"},
                     "fail_when": {"path": "Status", "in": ["Error", "Expired"]}},
            "download": {"url": "{{ poll.Url }}", "format": "csv", "compression": "zip"}}
    spec.update(job)
    return fake_source(request={"async_job": spec}, partitions=[{"name": "account", "values": ["a1"]}], records=None,
                       select="""SELECT record->>'AccountId' AS account, (record->>'TimePeriod')::DATE AS day,
                                        coalesce((record->>'Clicks')::BIGINT, 0) AS clicks FROM records""")


def test_sdk_requests_take_no_headers(fake):
    """Per-request values are an operation's arguments: `headers` on an sdk request stop the run before any call."""
    source = fake_source()
    request_of(source)["headers"] = {"Account": "a1"}
    problem = ("stream 'rows': requests[0]: fake: Reports.list: sdk requests take no `headers`; per-request values "
               "are the operation's `arguments`")
    assert components.check_source(source) == [problem]
    with pytest.raises(SourceError, match="sdk requests take no `headers`"):
        run(source)
    assert fake.calls == []


def test_async_jobs_submit_poll_and_download(api, fake):
    url = api.url + "/files/report.zip?sig=SAS-SECRET-123"
    statuses = [{"Status": "Pending"}, {"Status": "Success", "Url": url}]
    fake.handlers["submit"] = lambda arguments: ["job-" + arguments["account"]]
    fake.handlers["status"] = lambda arguments: [statuses.pop(0)]
    api.routes[("GET", "/files/report.zip")] = lambda request: (200, zipped({
        "b.csv": "AccountId,TimePeriod,Clicks\r\na1,2026-10-02,\r\n",
        "a.csv": "AccountId,TimePeriod,Clicks\r\na1,2026-10-01,5\r\n"}))
    clock = FakeClock()
    output = run(job_source(), clock=clock)
    assert [r for _, r in output.records] == [{"account": "a1", "day": "2026-10-01", "clicks": 5},
                                              {"account": "a1", "day": "2026-10-02", "clicks": 0}]
    assert [(c["method"], c["arguments"]) for c in fake.calls] == [
        ("submit", {"account": "a1"}), ("status", {"id": "job-a1"}), ("status", {"id": "job-a1"})]
    assert clock.sleeps == [10.0]
    assert api.requests[0]["params"] == {"sig": "SAS-SECRET-123"} and "Authorization" not in api.requests[0]["headers"]


@pytest.mark.parametrize("statuses,problem", [
    ([{"Status": "Error", "Message": "column for secret-token-1"}], "the job failed: {\"Message\": \"column for ***"),
    ([{"Status": "Pending"}] * 10, "did not finish within 1m"),
])
def test_async_jobs_fail_or_time_out(fake, statuses, problem):
    fake.handlers["submit"] = lambda arguments: [{"id": 7}]
    fake.handlers["status"] = lambda arguments: [statuses.pop(0)]
    clock = FakeClock()
    with pytest.raises(SourceError) as error:
        run(job_source(poll=dict(request_of(job_source())["async_job"]["poll"],
                                 arguments={"id": "{{ submit.id }}"})), clock=clock)
    assert problem in str(error.value)
    if "did not finish" in problem:
        assert clock.sleeps == [10.0] * 6 and len(fake.calls) == 1 + 7


def test_async_jobs_without_a_file_and_download_errors(api, fake, caplog):
    fake.handlers["submit"] = lambda arguments: ["job"]
    fake.handlers["status"] = lambda arguments: [{"Status": "Success", "Url": None}]
    with caplog.at_level(logging.DEBUG, logger="streamwright.source"):
        assert run(job_source()).records == [] and api.requests == []
    assert "stream 'rows': submitted the job job" in caplog.text and "job status: Status=Success" in caplog.text
    assert "the job finished without a file to download, which usually means no data" in \
        caplog.text

    fake.handlers["status"] = lambda arguments: [{"Status": "Success",
                                                   "Url": api.url + "/files/x.zip?sig=SAS-SECRET-123"}]
    api.routes[("GET", "/files/x.zip")] = lambda request: (403, {"error": "expired"})
    with pytest.raises(SourceError) as error:
        run(job_source())
    assert "HTTP 403" in str(error.value) and "SAS-SECRET-123" not in str(error.value)

    api.routes[("GET", "/files/x.zip")] = lambda request: (200, b"not a zip")
    with pytest.raises(SourceError, match="not a zip archive"):
        run(job_source())

    fake.handlers["status"] = lambda arguments: [{"Status": "Success", "Url": {"href": "x"}}]
    with pytest.raises(SourceError, match="the download url is dict, expected text"):
        run(job_source())


def test_async_job_results_requests(fake):
    fake.handlers["submit"] = lambda arguments: ["job"]
    fake.handlers["status"] = lambda arguments: [{"Status": "Success", "ReportId": "r9"}]
    fake.handlers["rows"] = lambda arguments: [{"data": [{"AccountId": "a1", "Clicks": arguments["report"]}]}]
    source = job_source(results={"sdk": "fake", "service": "Reports", "method": "rows",
                                 "arguments": {"report": "{{ poll.ReportId }}"}})
    del request_of(source)["async_job"]["download"]
    request_of(source)["records"] = {"path": "data"}  # the results' records, as the request's own
    with pytest.raises(SourceError, match="step 'rows': Conversion Error: Could not convert string 'r9'"):
        run(source)
    fake.handlers["rows"] = lambda arguments: [{"data": [{"AccountId": "a1", "Clicks": "3"}]}]
    assert [r for _, r in run(source).records] == [{"account": "a1", "day": None, "clicks": 3}]
    source["streams"][0]["transform"]["mode"] = "run"  # the same in run mode
    assert [r for _, r in run(source).records] == [{"account": "a1", "day": None, "clicks": 3}]


# * ---
# * CLI
# * ---

def write_source(tmp_path, monkeypatch, source):
    path = tmp_path / "source.yaml"
    path.write_text(json.dumps(dict(source, spec={"config": {"name": {"type": "string", "default": "x"}},
                                                  "secrets": {"token": {"type": "string"}}})))
    monkeypatch.setenv("STREAMWRIGHT_SECRET_TOKEN", "secret-token-1")
    return path


def test_cli_runs_connectors_and_checks_them_first(fake, tmp_path, monkeypatch, capsys, caplog):
    fake.handlers["list"] = lambda arguments: [{"rows": [{"id": 1}]}]
    path = write_source(tmp_path, monkeypatch, fake_source())
    assert cli.main(["run", str(path)]) == 0
    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [m["record"] for m in records if m["type"] == "RECORD"] == [{"id": 1, "account": "a1"},
                                                                       {"id": 1, "account": "a2"}]

    assert cli.main(["run", str(path), "--allow-connector", "google_ads"]) == 2
    assert "connector 'fake' is not in the allowed list (google_ads)" in caplog.text

    source = fake_source()
    request_of(source)["service"] = "Admin"
    caplog.clear()
    assert cli.main(["run", str(write_source(tmp_path, monkeypatch, source))]) == 2
    assert "stream 'rows': requests[0]: fake: Admin.list is not supported" in caplog.text and len(fake.calls) == 2

    source["auth"]["provider"] = "not_installed"
    for item in source["streams"]:
        item["requests"][0]["sdk"] = "not_installed"
    caplog.clear()
    assert cli.main(["run", str(write_source(tmp_path, monkeypatch, source))]) == 2
    assert "pip install streamwright-not-installed" in caplog.text

    assert cli.main(["connectors"]) == 0
    assert "fake" in capsys.readouterr().out.split()


# * --------------
# * query builders
# * --------------

def test_query_builders_come_with_their_connector(fake, capsys):
    assert components.query_builders()["fake_query"] is FAKE and "fake_query" not in components.available()
    assert cli.main(["connectors"]) == 0
    assert "fake_query" not in capsys.readouterr().out  # streamwright connectors lists connectors only
    source = fake_source()
    query = request_of(source)["arguments"]["query"]
    query["fake_query"] = {"where": {}}
    assert components.check_source(source) == [
        "stream 'rows': requests[0].arguments.query.fake_query.select: fake_query: `select` must be a list of fields"]
    with pytest.raises(SourceError, match=r"stream 'rows': requests\[0\].arguments.query.fake_query.select: "
                                          r"fake_query: `select`"):
        run(source)
    assert fake.calls == []


def test_an_allowed_connector_brings_its_query_builders(fake):
    fake.handlers["list"] = lambda arguments: [{"rows": [{"id": 1}]}]
    source = fake_source()
    assert components.check_source(source, allowed=["fake"]) == []
    output, clock = MemoryOutput(), FakeClock()
    SourceRunner(source, {"name": "test"}, {"token": "t"}, output=output, today=TODAY, clock=clock,
                 sleep=clock.sleep, allowed_connectors=["fake"]).run()
    assert len(output.records) == 2


def test_runs_check_every_stream_before_the_first_request(fake):
    """A stream that cannot run stops the run before any stream calls the API (also from Python)."""
    fake.handlers["list"] = lambda arguments: [{"rows": [{"id": 1}]}]
    source = fake_source()
    source["streams"] = [fake_stream("first"), fake_stream("second")]
    request_of(source, stream=1)["arguments"]["query"] = {"fake_query": {"where": {}}}
    output, clock = MemoryOutput(), FakeClock()
    with pytest.raises(SourceError, match=r"stream 'second': .*fake_query: `select` must be a list of fields"):
        SourceRunner(source, {"name": "x"}, {"token": "t"}, output=output, today=TODAY, clock=clock,
                     sleep=clock.sleep, allowed_connectors=["fake"]).run()
    request_of(source, stream=1)["service"] = "Admin"
    with pytest.raises(SourceError, match=r"stream 'second': requests\[0\]: fake: Admin.list is not supported"):
        run(source)
    assert fake.calls == [] and output.records == [] and fake.connected == []


def test_query_builders_in_http_requests(api, fake):
    api.routes[("GET", "/search")] = lambda request: (200, {"data": [{"id": 1}]})
    source = {"kind": "source", "name": "demo", "auth": {"type": "bearer", "token": "{{ secrets.token }}"},
              "http": {"base_url": api.url}, "streams": [page_stream(
                  "items", {"http": {"path": "/search", "params": {
                      "q": {"fake_query": {"select": ["id"], "where": {"name": "{{ config.name }}"}}}}}},
                  "SELECT (record->>'id')::BIGINT AS id FROM records", records={"path": "data"})]}
    assert [r for _, r in run(source).records] == [{"id": 1}]
    assert api.calls("/search")[0]["params"]["q"] == "SELECT id WHERE name = 'it''s'"


def test_values_never_become_query_builder_calls(fake):
    """Only calls written in the source are built: a value that looks like one (from inputs or records) is data."""
    fake.handlers["list"] = lambda arguments: [{"rows": [{"id": 1}]}]
    source = fake_source(partitions=[{"name": "filter", "values": [{"fake_query": {"select": ["x"]}}]}])
    request_of(source)["arguments"]["account"] = "{{ partition.filter }}"
    run(source)
    assert fake.calls[0]["arguments"]["account"] == {"fake_query": {"select": ["x"]}}


def test_query_builder_failures_fail_the_partition(fake):
    fake.handlers["list"] = lambda arguments: [{"rows": [{"id": 1}]}]
    source = fake_source()
    query = request_of(source)["arguments"]["query"]
    query["fake_query"]["fail"] = "{{ partition.account }}"
    with pytest.raises(SourceError, match="request 'records': fake_query: ValueError: cannot build a1"):
        run(source)
    source["streams"][0]["on_partition_error"] = "skip"
    assert run(source).records == [] and fake.calls == []
    del query["fake_query"]["fail"], source["streams"][0]["on_partition_error"]
    query["fake_query"]["not_text"] = True
    with pytest.raises(SourceError, match="fake_query: the query builder returned int, not text"):
        run(source)


def test_streamwright_validate_runs_the_component_checks(fake, tmp_path, capsys):
    path = tmp_path / "source.yaml"
    source = fake_source()
    source["spec"] = {"config": {"name": {"type": "string"}}, "secrets": {"token": {"type": "string"}}}
    request_of(source)["arguments"]["query"] = {"fake_query": {"where": {}}}
    path.write_text(yaml.safe_dump(source, sort_keys=False))
    line, column = next((number, text.index("fake_query") + 1)
                        for number, text in enumerate(path.read_text().splitlines(), 1) if "fake_query:" in text)
    assert engine.main([str(path)]) == 0  # `streamwright validate`: the builder's mapping is just data to it
    capsys.readouterr()
    assert cli.main(["validate", str(path)]) == 1
    out = capsys.readouterr().out
    assert "%s:%d:%d: error [connector-check] streams[0].requests[0].arguments.query.fake_query.select: fake_query: " \
        "`select` must be a list of fields" % (path, line, column) in out

    source["auth"]["provider"] = request_of(source)["sdk"] = "not_installed"
    path.write_text(yaml.safe_dump(source, sort_keys=False))
    assert cli.main(["validate", str(path)]) == 1  # the builder's own check still runs
    out = capsys.readouterr().out
    assert "warning [connector-not-installed] auth.provider: connector 'not_installed' is not installed" in out
    request_of(source)["arguments"]["query"]["fake_query"]["select"] = ["clicks"]
    path.write_text(yaml.safe_dump(source, sort_keys=False))
    assert cli.main(["validate", str(path)]) == 0 and cli.main(["validate", "--strict", str(path)]) == 1


def test_downloads_are_read_in_pages():
    csv_text = "Id,Name\n" + "".join("%d,n%d\n" % (i, i) for i in range(5))
    pages = list(downloads.read(io.BytesIO(gzip.compress(csv_text.encode())), "csv", "gzip", page_size=2))
    assert [len(page) for page in pages] == [2, 2, 1] and pages[0][1] == {"Id": "1", "Name": "n1"}
    jsonl = io.BytesIO(b'{"a": 1}\n\n{"a": 2}\n')
    assert list(downloads.read(jsonl, "jsonl")) == [[{"a": 1}, {"a": 2}]] and not jsonl.closed
    assert list(downloads.read(io.BytesIO(b'{"data": [1]}'), "json", "none")) == [{"data": [1]}]
    with pytest.raises(downloads.DownloadError, match="cannot read the downloaded jsonl file"):
        list(downloads.read(io.BytesIO(b"{oops\n"), "jsonl"))
    with pytest.raises(downloads.DownloadError, match=r"\(gzip\)"):
        list(downloads.read(io.BytesIO(gzip.compress(b"a,b\n1,2\n")[:15]), "csv", "gzip"))
