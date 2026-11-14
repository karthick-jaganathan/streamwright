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

"""DuckDBJob consume steps and publish outputs, through the shared Runner, with an in-memory broker."""

from types import MappingProxyType, SimpleNamespace

import duckdb
import pytest

from streamwright.core.jobs.duckdb import engine
from streamwright.core.jobs.duckdb.adapter import ProviderAdapter
from streamwright.core.jobs.duckdb.loading import load_adapters
from streamwright.core.jobs.duckdb.rules import DuckDBRules
from streamwright.core.jobs.inputs import InputError
from streamwright.core.jobs.types import Broker, Message


class FakeBus(Broker):
    def __init__(self, messages=()):
        self.queue, self.acked, self.published, self.closed = list(messages), [], [], False

    def poll(self, arguments, limit, max_wait_seconds, watermarks=None):
        taken, self.queue = self.queue[:limit], self.queue[limit:]
        return taken

    def ack(self, messages, arguments):
        self.acked.extend(m.message_id for m in messages)

    def publish(self, arguments, events, dedupe_key):
        self.published.append((arguments["destination"], dedupe_key, list(events)))

    def destination(self, arguments):
        return arguments["destination"]

    def close(self):
        self.closed = True


class FakeBusAdapter(ProviderAdapter):
    provider = "fakebus"
    messaging = MappingProxyType({("message", "consume"): SimpleNamespace(kind="consume"),
                                  ("message", "publish"): SimpleNamespace(kind="publish")})

    def __init__(self, bus):
        self.bus = bus

    def open_broker(self, connection):
        return self.bus


def _message(number, **headers):
    return Message("m%d" % number, str(number), '{"order_id": %d}' % number, headers, "0")


@pytest.fixture
def bus(monkeypatch):
    found = FakeBus([_message(n, source="test") for n in range(1, 6)])

    def rules(job, allowed=None, entry_points=None):
        adapters, _ = load_adapters(["duckdb"])
        return DuckDBRules(dict(adapters, fakebus=FakeBusAdapter(found)))
    monkeypatch.setattr(engine, "duckdb_rules", rules)
    return found


def _job(tmp_path, **changes):
    job = {"kind": "DuckDBJob", "name": "bus_job",
           "connections": {
               "bus": {"provider": "fakebus"},
               "wh": {"provider": "duckdb", "path": str(tmp_path / "w.db")},
           },
           "steps": [{"name": "orders", "connection": "bus", "service": "message", "method": "consume",
                      "arguments": {"destination": "orders"},
                      "sql": "SELECT message_id, headers['source'] AS source, (body->>'$.order_id')::INT AS order_id "
                             "FROM input"}],
           "outputs": [{"name": "rows", "connection": "wh", "step": "orders", "table": "orders", "mode": "append"},
                       {"name": "done", "connection": "bus", "service": "message", "method": "publish",
                        "arguments": {"destination": "orders.done"},
                        "sql": "SELECT output, rows, batch_id FROM _commits"}],
           "runtime": {"state": {"sqlite": str(tmp_path / "state.db"), "apply_migrations": True}}}
    job.update(changes)
    return job


def test_drain_consumes_in_batches_writes_publishes_and_acks(tmp_path, bus):
    result = engine.run_job(_job(tmp_path), mode="drain", max_messages=2)
    assert result.exit_code == 0, result.error
    assert result.batches == ["bus_job-00000001", "bus_job-00000002", "bus_job-00000003"]
    assert result.outputs == {"rows": 5}
    assert bus.acked == ["m1", "m2", "m3", "m4", "m5"] and bus.closed
    assert [(dest, key, [e["rows"] for e in events]) for dest, key, events in bus.published] == [
        ("orders.done", "bus_job:bus_job-0000000%d:done" % n, [rows]) for n, rows in ((1, 2), (2, 2), (3, 1))]
    with duckdb.connect(str(tmp_path / "w.db"), read_only=True) as con:
        assert con.execute("SELECT count(*), min(source), max(order_id) FROM orders").fetchall() == [(5, "test", 5)]


def test_once_takes_one_batch_and_follow_needs_a_consumer(tmp_path, bus):
    assert engine.run_job(_job(tmp_path), max_messages=3).batches == ["bus_job-00000001"]
    assert bus.queue == [_message(4, source="test"), _message(5, source="test")]
    plain = _job(tmp_path, steps=[{"name": "s", "sql": "SELECT 1 AS n"}],
                 outputs=[{"name": "o", "connection": "wh", "step": "s", "table": "t", "mode": "overwrite"}])
    with pytest.raises(InputError, match="--follow needs a job that consumes a broker"):
        engine.run_job(plain, mode="follow")


def test_broker_steps_and_outputs_are_checked(tmp_path, bus):
    def problems(**changes):
        return [issue.message for issue in engine.validate_job(_job(tmp_path, **changes)).issues
                if issue.severity == "error"]

    consume = _job(tmp_path)["steps"][0]
    assert problems() == []
    assert any("one broker step" in p for p in problems(steps=[consume, dict(consume, name="again")]))
    publish = _job(tmp_path)["outputs"][1]
    for output, message in ((dict(publish, mode="append"), "takes no mode"),
                            (dict(publish, retry={"max_attempts": 2}), "takes no retry"),
                            ({k: v for k, v in publish.items() if k != "sql"}, "needs `sql` or `step`"),
                            (dict(publish, method="consume"), "it is a step, not an output"),
                            ({k: v for k, v in _job(tmp_path)["outputs"][0].items() if k != "mode"}, "needs `mode`")):
        assert any(message in p for p in problems(outputs=[_job(tmp_path)["outputs"][0], output])), message
    assert any("it is an output, not a step" in p
               for p in problems(steps=[dict(consume, method="publish")]))
