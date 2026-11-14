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

"""Kafka Spark broker is loaded from the definition."""

from streamwright.connectors.kafka import CONNECTOR
from streamwright.connectors.kafka.client import KafkaClient
from streamwright.spark.runtime.definition import bind


def test_spark_definition_opens_broker():
    adapter = bind(CONNECTOR, "kafka")
    assert adapter.operation("message", "consume")
    assert adapter.operation("message", "read") is None
    broker = adapter.adapter.open_broker({"bootstrap_servers": "broker"})
    assert isinstance(broker, KafkaClient)
    assert broker.destination({"destination": "commits"}) == "commits"
