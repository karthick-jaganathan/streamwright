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
Messaging for connectors of the message family: the broker client protocol a connector's client implements
(BrokerClient, Message) and the shared polling policy of source reads and job brokers.
"""

from streamwright.core.io.message_rules import MESSAGE_DUCKDB_CAPABILITIES as DUCKDB_CAPABILITIES
from streamwright.core.io.message_rules import MESSAGE_SOURCE_CAPABILITIES as SOURCE_CAPABILITIES
from streamwright.core.io.message_rules import check_poll, job_capabilities, poll_limit, poll_wait_seconds
from streamwright.core.jobs.types import Broker as BrokerClient
from streamwright.core.jobs.types import Message

__all__ = ["DUCKDB_CAPABILITIES", "SOURCE_CAPABILITIES", "BrokerClient", "Message", "check_poll", "job_capabilities",
           "poll_limit", "poll_wait_seconds"]
