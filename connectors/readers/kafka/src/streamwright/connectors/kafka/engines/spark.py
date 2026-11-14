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

"""Kafka broker binding for the Spark batch runner."""

from streamwright.connectors.kafka import client

__all__ = ["KafkaSparkAdapter"]


class KafkaSparkAdapter:
    def hadoop_settings(self, name, connection):
        return {}

    def open_broker(self, connection):
        return client.KafkaClient(connection)
