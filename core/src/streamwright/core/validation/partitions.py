"""Request partition checks shared through the source issue sink."""

from streamwright.core.engine.rules import valid_batch_size
from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.checker import describe
from streamwright.core.validation.sections import SectionChecker


class PartitionChecker(SectionChecker):
    def check_batch_size(self, item, path, stream_partitions):
        size = item[spec.BATCH_SIZE]
        scoping = [name for name in self.stream.partitions if isinstance(name, str) and name in stream_partitions]
        fields = item.get("fields") if "from" in item else None
        if (fields is None and "name" not in item) or ("fields" in item and not (isinstance(fields, list) and fields)):
            return
        if not valid_batch_size(size):
            self.error("bad-value", path, "`batch_size` must be a whole number of at least 1 (the most values in one "
                       "request's list), got %s" % (repr(size) if isinstance(size, (int, float, str)) else
                                                    describe(size)), item, spec.BATCH_SIZE)
            return
        if fields is not None:
            batching = self._batched_field(item, path, fields, stream_partitions, scoping)
        elif "from" in item:
            batching = self._batched_name(item, path, stream_partitions)
        else:
            return
        if batching is not None:
            batched, listed = batching
            self._check_step_scoping(item, path, size, scoping=scoping, batched=batched, listed=listed)

    def _batched_field(self, item, path, fields, stream_partitions, scoping):
        """(batched field, listed fields) of a `{from, fields}` item, or None after reporting why it cannot batch."""
        own = [field for field in fields if not (isinstance(field, str) and field in stream_partitions)]
        if not own:
            one = len(fields) == 1
            self.error("bad-value", path, "`batch_size`: %s %s stream partition%s, so this item only keeps the "
                       "rows with %s: it has no values of its own to batch; add the field to batch to `fields`" % (
                           ", ".join(repr(field) for field in fields), "is a" if one else "are",
                           "" if one else "s", "its value" if one else "their values"),
                       item, spec.BATCH_SIZE)
            return None
        if len(own) > 1:
            self.error("bad-value", path, "`batch_size` on `{from, fields}` batches one field into one list, but "
                       "%s are not stream partitions: list only the stream partitions (%s) and the one field to "
                       "batch" % (", ".join(repr(field) for field in own),
                                  ", ".join(scoping) or "the stream has none"), item, spec.BATCH_SIZE)
            return None
        return own[0], fields

    def _batched_name(self, item, path, stream_partitions):
        """(batched field, ()) of a `{from, field, name}` item, or None after reporting why it cannot batch."""
        if item.get("name") in stream_partitions:
            self.error("bad-value", path, "`batch_size`: %r is a stream partition, so this item only keeps the "
                       "rows with its value: it has no values of its own to batch" % item["name"], item,
                       spec.BATCH_SIZE)
            return None
        return item.get("field"), ()

    def _check_step_scoping(self, item, path, size, *, scoping, batched, listed):
        """A step's values batched in a partitioned stream must be scoped by every stream partition."""
        table = self.stream.tables.get(item["from"].lower()) if isinstance(item.get("from"), str) else None
        if table is None or table[0] != "step" or all(name in listed for name in scoping):
            return
        self.error("bad-value", path, "`batch_size`: step %r has the rows of every stream partition, so each list "
                   "would mix them: batching a step's values in a partitioned stream must scope each partition: "
                   "use `{from: %s, fields: [%s], batch_size: %s}` (the list is named after the field)" % (
                       item["from"], item["from"],
                       ", ".join([*scoping, batched if isinstance(batched, str) else "<field>"]), size),
                   item, spec.BATCH_SIZE)
