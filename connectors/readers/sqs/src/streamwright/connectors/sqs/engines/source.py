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

"""Read-only SQS queue inspection on the source engine."""

from streamwright.connectors.sqs import client

__all__ = ["SqsSourceAdapter"]


class SqsSourceSession:
    def __init__(self, connection):
        self.connection = connection

    def read(self, operation, arguments, context):
        yield from client.peek(self.connection, arguments, context)

    def close(self):
        pass


class SqsSourceAdapter:
    def open(self, connection, context):
        return SqsSourceSession(connection)
