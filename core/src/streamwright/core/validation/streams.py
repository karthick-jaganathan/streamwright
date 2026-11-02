"""Stream-wide consistency checks using the shared issue sink."""

from streamwright.core.engine import rules
from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.checker import DURATION, describe, is_date
from streamwright.core.validation.sections import SectionChecker
from streamwright.core.validation.sections.templates import REFERENCE_HEAD, TEMPLATE


class StreamChecker(SectionChecker):
    def check_incremental(self, stream, path):
        incremental = self.check_mapping(stream["incremental"], spec.INCREMENTAL_KEYS, (*path, "incremental"),
                                         container=stream, key="incremental", what="`incremental`",
                                         required=("cursor_field", "start"))
        if incremental is None:
            return
        self.stream.incremental = True
        ipath = (*path, "incremental")
        if "cursor_field" in incremental:
            self._expect(isinstance(incremental["cursor_field"], str), "a field name",
                         incremental["cursor_field"], (*ipath, "cursor_field"), container=incremental,
                         key="cursor_field")
        for duration in ("window", "lookback"):
            if duration in incremental:
                self.check_duration(incremental[duration], (*ipath, duration), incremental, duration)
                value = incremental[duration]
                minimum = 1 if duration == "window" else 0
                if isinstance(value, str) and DURATION.match(value) and \
                        (value[-1] != "d" or int(value[:-1]) < minimum):
                    self.error("bad-value", (*ipath, duration), "incremental `%s` is whole days%s, got %r" % (
                        duration, " (at least 1d)" if minimum else "", value), incremental, duration)
        if "start" in incremental:
            self.check_templates(incremental["start"], (*ipath, "start"), container=incremental, key="start",
                                 scopes=self.scopes())
            start = incremental["start"]
            if not (isinstance(start, str) and "{{" in start) and not is_date(start):
                self.error("bad-value", (*ipath, "start"), "expected a date (YYYY-MM-DD, today or -30d) "
                           "or a reference, got %s" % describe(start), incremental, "start")
            elif isinstance(start, str) and "{{" in start:
                self.check_date_reference_is_defined(start, (*ipath, "start"), incremental, "start")

    def check_date_reference_is_defined(self, value, path, container, key):
        """
        An incremental `start` that resolves to a `{{ config.X }}` input fails at run time ("invalid date None") when
        X is optional and has no default. A `default` filter supplies a value, so it is allowed.
        """
        for inner in TEMPLATE.findall(value):
            parts = spec.split_outside_quotes(inner, "|")
            if any(text.strip().split("(")[0].strip() == "default" for text in parts[1:]):
                continue
            match = REFERENCE_HEAD.match(parts[0].strip())
            if match is None or match.group(1) != "config" or not match.group(2):
                continue
            name = match.group(2).split(".")[0]
            definition = self.inputs["config"].get(name)
            if not isinstance(definition, dict):
                continue                         # an unknown config input is already reported by check_templates
            if not definition.get("required", True) and "default" not in definition:
                self.error("bad-value", path, "incremental `start` references optional config %r, which has no "
                           "default and resolves to no date; make it required, give it a default, or add a "
                           "`| default(...)` filter" % name, container, key)

    def check_request_rules(self, stream, path):
        requests = stream.get("requests") if isinstance(stream.get("requests"), list) else []
        transform = stream.get("transform")
        if isinstance(transform, dict) and transform.get("mode") == "page":
            if len(requests) > 1:
                self.error("bad-value", (*path, "transform", "mode"), "`mode: page` runs the steps on each page of "
                           "one request, and this stream has %d: use `mode: run` to read several requests" %
                           len(requests), transform, "mode")
            for index, request in enumerate(requests):
                if isinstance(request, dict) and "partitions" in request:
                    self.error("bad-value", (*path, "requests", index, "partitions"), "request-level partitions need "
                               "`transform.mode: run`", request, "partitions")
        bodies = [
            {key: value for key, value in item.items() if key not in ("name", "records", "paginator", "partitions")}
            for item in requests if isinstance(item, dict)
        ]
        if self.stream.incremental and bodies and not any(rules.uses_window(body) for body in bodies):
            self.error("bad-value", (*path, "incremental"), "`incremental` needs a request that uses "
                       "`{{ window.start }}` or `{{ window.end }}`", stream, "incremental")
