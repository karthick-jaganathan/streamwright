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

"""Kafka source session delegates vendor reads through its source context."""

from types import SimpleNamespace

from streamwright.connectors.kafka import CONNECTOR, client
from streamwright.core.runtime.source_connectors import check_call
from streamwright.core.sdk.engines.source import SourceAdapter, SourceSession


def test_source_adapter_opens_a_protocol_session(monkeypatch):
    monkeypatch.setattr(client, "peek", lambda connection, arguments, context: iter([[{"offset": 1}]]))
    session = CONNECTOR.load_adapter("source").open({"bootstrap_servers": "broker"}, SimpleNamespace())
    assert isinstance(CONNECTOR.load_adapter("source"), SourceAdapter)
    assert isinstance(session, SourceSession)
    pages = session.read(CONNECTOR.operation("message", "read"), {"destination": "arrivals"}, SimpleNamespace())
    assert list(pages) == [[{"offset": 1}]]
    session.close()


def test_source_engine_accepts_read_only_message_calls():
    connector = CONNECTOR
    assert check_call(connector, {"service": "message", "method": "read",
                                    "arguments": {"destination": "arrivals", "max_wait": "1s"}}) == []
    assert check_call(connector, {"service": "message", "method": "consume",
                                    "arguments": {"destination": "arrivals", "group_id": "workers"}})
