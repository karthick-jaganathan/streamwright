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

"""SQS Spark broker is loaded from its definition."""

from streamwright.connectors.sqs import CONNECTOR, client
from streamwright.connectors.sqs.client import SqsClient
from streamwright.spark.runtime.definition import bind


def test_spark_definition_opens_broker(monkeypatch):
    monkeypatch.setattr(client.boto3, "client", lambda service, **kwargs: object())
    adapter = bind(CONNECTOR, "sqs")
    assert adapter.operation("message", "publish")
    assert adapter.operation("message", "read") is None
    assert isinstance(adapter.adapter.open_broker({"region": "us-east-1"}), SqsClient)
