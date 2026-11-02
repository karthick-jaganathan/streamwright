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

"""Request-level `partitions`: values, `from` and partition sources."""


from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.checker import describe, suggest
from streamwright.core.validation.partitions import PartitionChecker
from streamwright.core.validation.sections.requests import RequestSection
from streamwright.core.validation.sections.stream_context import StreamContext, lower_name
from streamwright.core.validation.sections.templates import REFERENCE_HEAD, template_expressions


class RequestPartitionSection(RequestSection):
    """Request partitions and their source references."""

    def check_request_partitions(self, partitions, path, request, info, *, index, stream_info, stream):
        """Request `index`'s own partitions (`info`: what the request sees; `stream_info`: its stream's tables)."""
        if not self._expect(isinstance(partitions, list) and len(partitions) > 0,
                            "a non-empty list of request partitions", partitions, path, container=request,
                            key="partitions"):
            return
        # a `from:` item can name a stream partition: it then uses only the rows with that partition's value
        stream_partitions = set(info.partitions)
        for k, item in enumerate(partitions):
            ppath = (*path, k)
            if not self._expect(isinstance(item, dict), "a mapping", item, ppath, container=partitions, key=k):
                continue
            names = []
            batched = (spec.BATCH_SIZE,)  # reported on its own below, not as an unknown key
            if "from" in item:
                names.extend(self._check_partition_from(item, ppath, partitions, k, index=index,
                                                        stream_info=stream_info, stream=stream))
            else:
                self.check_mapping(item, spec.REQUEST_PARTITION_VALUES_KEYS + batched, ppath, container=partitions,
                                   key=k, what="a request partition", required=spec.REQUEST_PARTITION_VALUES_KEYS)
                values = item.get("values")
                if "values" in item and not (isinstance(values, list) or (isinstance(values, str) and "{{" in values)):
                    self.error("bad-value", (*ppath, "values"), "expected a list or a reference such as "
                               "\"{{ config.ids }}\", got %s" % describe(values), item, "values")
                self.check_partition_values(values, (*ppath, "values"), item, info, stream_partitions)
            if spec.BATCH_SIZE in item:
                self.check_batch_size(item, (*ppath, spec.BATCH_SIZE), stream_partitions, stream_info)
            if "name" in item and "fields" not in item:
                self.check_name(item["name"], (*ppath, "name"), item, "name")
                names.append((item["name"], (*ppath, "name"), item, "name"))
            for name, npath, container, key in names:
                if not isinstance(name, str) or ("from" in item and name in stream_partitions):
                    continue
                if name in info.partitions:
                    self.error("duplicate-partition", npath, "partition %r is defined twice" % name, container, key)
                else:
                    info.partitions.append(name)

    def _check_partition_from(self, item, ppath, partitions, position, *, index, stream_info, stream):
        """A `from:` request partition; returns the (name, path, container, key) of its `fields`."""
        names = []
        batched = (spec.BATCH_SIZE,)
        if "fields" in item:
            self.check_mapping(item, spec.REQUEST_PARTITION_RECORD_KEYS + batched, ppath, container=partitions,
                               key=position, what="a request partition with `fields` (named after the fields)",
                               required=spec.REQUEST_PARTITION_RECORD_KEYS)
            fields = item.get("fields")
            if self._expect(isinstance(fields, list) and len(fields) > 0, "a non-empty list of field names", fields,
                            (*ppath, "fields"), container=item, key="fields"):
                for j, field in enumerate(fields):
                    self.check_name(field, (*ppath, "fields", j), fields, j)
                    names.append((field, (*ppath, "fields", j), fields, j))
        else:
            self.check_mapping(item, spec.REQUEST_PARTITION_SOURCE_KEYS + batched, ppath, container=partitions,
                               key=position, what="a request partition", required=spec.REQUEST_PARTITION_SOURCE_KEYS)
            self.check_strings(item, ("field",), ppath, "a name")
        self.check_strings(item, ("from",), ppath, "a name")
        self.check_partition_source(item.get("from"), (*ppath, "from"), item, index=index, info=stream_info,
                                    stream=stream)
        return names

    def check_batch_size(self, item, path, stream_partitions, stream_info):
        PartitionChecker(self, stream_info).check_batch_size(item, path, stream_partitions)

    def check_partition_values(self, values, path, item, info, stream_partitions):
        """
        A request partition's `values`, which a run makes once per stream partition, before the request: their
        references can use config, today and the stream's partitions, not the window nor the request's own partitions
        (`info.partitions`: the stream's and the request's earlier ones).
        """
        unavailable = False
        for inner in template_expressions(values):
            match = REFERENCE_HEAD.match(spec.split_outside_quotes(inner, "|")[0])
            scope, name = (match.group(1), (match.group(2) or "").split(".")[0]) if match else (None, None)
            if scope == "window":
                why = "no window"
            elif scope == "partition" and name in info.partitions and name not in stream_partitions:
                why = "%r is a partition of this request" % name
            else:
                continue
            self.error("unavailable-reference", path, "{{ %s }} cannot be used in request partition `values`: a run "
                       "makes them once per stream partition, with config, today and the stream's partitions only "
                       "(%s)" % (inner.strip(), why), item, "values")
            unavailable = True
        if not unavailable:
            runtime = StreamContext(info.name)
            runtime.partitions = [partition for partition in info.partitions if partition in stream_partitions]
            self.check_templates(values, path, container=item, key="values", scopes=self.scopes(runtime),
                                 stream=runtime)

    def check_partition_source(self, source, path, item, *, index, info, stream):
        """`from:` of request `index`: an earlier request or a step of the same stream."""
        if not isinstance(source, str):
            return
        key = source.lower()
        table = info.tables.get(key)
        if table is not None:
            kind, position, _ = table
            if kind == "request" and position == index:
                self.error("bad-value", path, "a request cannot partition on itself: use an earlier request or a "
                           "step", item, "from")
            elif kind == "request" and position > index:
                self.error("bad-value", path, "request partition source %r is a later request: use an earlier "
                           "request or a step" % source, item, "from")
            return
        exports = stream.get("export") if isinstance(stream.get("export"), dict) else {}
        own = [definition for name, definition in exports.items() if lower_name(name) == key]
        other = self.export_owners.get(key)
        if own:
            step = own[0].get("step") if isinstance(own[0], dict) else None
            self.error("bad-value", path, "%r is this stream's export, which is written after its requests and steps "
                       "run%s" % (source, "; read its step %r" % step if isinstance(step, str) else ""), item, "from")
        elif (key in self.stream_names and key != lower_name(stream.get("name"))) or other is not None:
            parent = self.streams[self.stream_names[key] if key in self.stream_names else other[1]].get("name")
            self.error("bad-value", path, "%r is not a request or step of this stream: requests read only their own "
                       "stream's requests and steps; to partition by stream %r, use a stream partition "
                       "`{from_stream: %s, ...}`" % (source, parent, parent), item, "from")
        elif info.steps_known:
            choices = [name for kind, position, name in info.tables.values() if kind == "step" or position < index]
            self.error("unknown-source", path, "unknown request partition source %r%s: use an earlier request or a "
                       "step of this stream" % (source, suggest(source, choices)), item, "from")
