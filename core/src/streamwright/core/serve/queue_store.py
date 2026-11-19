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
The run queue's store, from serve.yaml's `state`: a state database URL, or a connection as a job declares one,
opened by its connector's own state support, as a job's runtime.state.connection is.
"""

from streamwright.core.jobs.duckdb.adapter import AdapterError
from streamwright.core.jobs.duckdb.loading import load_adapters
from streamwright.core.jobs.state.dialects import DEFAULT_SCHEMA
from streamwright.core.jobs.state.target import describe_connection
from streamwright.core.jobs.state.url import open_url

__all__ = ["open_queue"]

_NAME = "state"                                   # the connection's name, in messages


def open_queue(spec):
    """The state store of a StateSpec; raises ValueError when it cannot be opened (the reason)."""
    if spec.url is not None:
        return open_url(spec.url)
    connection = dict(spec.connection)
    provider = connection["provider"]
    adapters, missing = load_adapters([provider])
    if provider in missing:
        raise ValueError("state.connection: %s" % missing[provider])
    adapter = adapters[provider]
    try:
        if provider == "duckdb" or not adapter.holds_state:   # a DuckDB catalog lives in a job's own session
            raise ValueError("state.connection: provider %s cannot hold the run queue (use a provider whose "
                             "connector holds state, or state.url)" % provider)
        problems = [message for _, message, warning in adapter.validate(_NAME, connection) if not warning]
        if problems:
            raise ValueError("state.connection: %s" % "; ".join(problems))
        schema = spec.schema or DEFAULT_SCHEMA
        store = adapter.open_state(None, _NAME, connection, schema)
    except AdapterError as exc:
        raise ValueError("state.connection: %s" % exc) from None
    store.target = describe_connection(_NAME, provider, connection, schema)
    return store
