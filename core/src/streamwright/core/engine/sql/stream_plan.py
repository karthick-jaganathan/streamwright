#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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

"""One stream of a source's plan (StreamPlan), and the names of its requests, steps, exports and partitions."""


from streamwright.core.engine.sql.tables import _NAME


def mapping_entries(value):
    """(position, item) of the mappings in a list (anything else has none: `streamwright validate` reports it)."""
    if not isinstance(value, list):
        return []
    return [(position, item) for position, item in enumerate(value) if isinstance(item, dict)]


def is_table_name(name):
    """Whether a stream, request, step or export name can name a table (`streamwright validate` reports the others)."""
    return isinstance(name, str) and _NAME.match(name) is not None


def partition_fields(item):
    """(fields read, partition names) of a partition item: `field` and `name`, or `fields` (named after them)."""
    if isinstance(item.get("fields"), list):
        return tuple(item["fields"]), tuple(item["fields"])
    return (item.get("field"),), (item.get("name"),)


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


class StreamPlan:
    """
    One stream of a SourcePlan:

    - mode: its `transform.mode`; requests / steps: (position, item) of its named requests and steps
    - queries / parameters / columns[step]: the step's query (its `$name` parameters typed), the config inputs it
      binds and its columns; conform[step]: the columns a run gives its table (when its parameters are plain casts);
      reads[step]: the tables it reads (lowercase)
    - order: ("request", item) and ("step", step), in the order a run runs them
    - exports: (export, spec, step) of its exports; parents: (position, item, stream) of its `from_stream` partitions
    """

    def __init__(self, name, index, stream):
        self.name, self.index, self.stream = name, index, stream
        transform = stream.get("transform") if isinstance(stream.get("transform"), dict) else {}
        self.mode = transform.get("mode")
        self.requests = [(position, item) for position, item in mapping_entries(stream.get("requests"))
                         if is_table_name(item.get("name"))]
        self.steps = [(position, step) for position, step in mapping_entries(transform.get("steps"))
                      if is_table_name(step.get("name"))]
        self.queries, self.parameters, self.columns, self.reads, self.conform = {}, {}, {}, {}, {}
        self.order, self.exports, self.parents = [], [], []

    def export_columns(self, export):
        """The columns of an export (of its step), or None if they are not known (its step has problems)."""
        for name, _, step in self.exports:
            if name == export:
                return self.columns.get(step)
        return None
