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

"""A source's connector session: opened by the first sdk request of a run, read by each request, closed at the end."""

from streamwright.core.engine.errors import SourceError
from streamwright.core.engine.readers import READERS
from streamwright.core.runtime import logs, source_connectors
from streamwright.core.runtime.components import ConnectorError, declared_auth


class ConnectorSession:
    def __init__(self, runner):
        self.runner = runner
        self.client = None  # the connector's SourceSession

    def connect(self, context):
        """Opens the connector's session (once per run) after the declared and resolved checks of the auth block."""
        runner = self.runner
        if self.client is None:
            definition, auth = runner.connector, dict(runner.connector_auth)
            problems = source_connectors.check_declared_auth(definition, declared_auth(runner.source)) + \
                source_connectors.check_resolved_auth(definition, auth)
            if problems:
                raise SourceError(runner.redact("; ".join(problems)))
            started = runner.clock()
            try:
                self.client = context.call(source_connectors.open_session, definition, auth, context)
            except ConnectorError as exc:
                raise SourceError(runner.redact("%s: cannot connect: %s" % (runner.provider, exc))) from exc
            seconds = runner.clock() - started
            logs.NETWORK.info("%s: connected in %.2f s", runner.provider, seconds,
                              extra=logs.fields(event="connect", connector=runner.provider,
                                                duration_ms=round(seconds * 1000)))
        return self.client

    def read(self, call, context):
        """The responses of a rendered call, read with the context of the request that makes it."""
        return source_connectors.read_call(self.runner.connector, self.client, call, context)

    def responses(self, call, context, scopes, where=None, select=None):
        return READERS["sdk"].responses(self.runner, call, context, scopes, where=where, select=select)

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None
