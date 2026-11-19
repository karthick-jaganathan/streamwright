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
The names in a source's streams that the engine (its plan and runner) and the outputs (their dispositions) both read:
which names can name a table, a stream's exports, and the exports of incremental streams.
"""

from streamwright.core.sql.quote import IDENTIFIER

__all__ = ["exports_of", "incremental_exports", "is_table_name"]


def is_table_name(name):
    """Whether a stream, request, step or export name can name a table (`streamwright validate` reports the others)."""
    return isinstance(name, str) and IDENTIFIER.match(name) is not None


def exports_of(stream):
    """A stream's export names (those that can be names: `streamwright validate` reports the others)."""
    export = stream.get("export") if isinstance(stream, dict) else None
    return [name for name in export if is_table_name(name)] if isinstance(export, dict) else []


def incremental_exports(source):
    """
    The exports of the streams with `incremental`: a run writes a window of their data, so outputs append them when
    they have no primary_key (the others replace their tables).
    """
    return {export for stream in source.get("streams") or [] if isinstance(stream, dict) and
               stream.get("incremental") for export in exports_of(stream)}
