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

"""Sqs broker binding for DuckDBJob: consume steps read a batch of messages, publish outputs send its events."""

from streamwright.connectors.sqs import client

__all__ = ["SqsDuckDBAdapter"]


class SqsDuckDBAdapter:
    def configure(self, session, name, connection):
        """Nothing to attach in DuckDB: the broker is opened by the job's runner, outside the session."""

    def open_broker(self, connection):
        return client.SqsClient(connection)
