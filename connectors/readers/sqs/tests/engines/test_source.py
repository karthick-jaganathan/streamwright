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

"""The SQS source session follows the source adapter protocol."""

from types import SimpleNamespace

from streamwright.connectors.sqs import CONNECTOR, client
from streamwright.core.sdk.engines.source import SourceAdapter, SourceSession


def test_source_session_opens_and_reads(monkeypatch):
    monkeypatch.setattr(client, "peek", lambda connection, arguments, context: iter([[{"message_id": "one"}]]))
    session = CONNECTOR.load_adapter("source").open({"region": "us-east-1"}, SimpleNamespace())
    assert isinstance(CONNECTOR.load_adapter("source"), SourceAdapter)
    assert isinstance(session, SourceSession)
    assert list(session.read(CONNECTOR.operation("message", "read"), {"destination": "queue"}, SimpleNamespace())) == [
        [{"message_id": "one"}]]
    session.close()
