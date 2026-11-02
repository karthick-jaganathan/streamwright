"""Structured diagnostics shared by StreamWright validators and engines."""

from dataclasses import dataclass

# the keys of Issue.to_dict(), in their order
_KEYS = ("code", "column", "file", "line", "message", "path", "severity")


@dataclass(slots=True, eq=False, repr=False)
class Issue:
    severity: str
    code: str
    message: str
    file: str | None = None
    path: str = ""
    line: int | None = None
    column: int | None = None

    def to_dict(self):
        return {key: getattr(self, key) for key in _KEYS}

    def __str__(self):
        where = self.file or "<input>"
        if self.line is not None:
            where += ":%d:%d" % (self.line, self.column)
        path = "%s: " % self.path if self.path else ""
        return "%s: %s [%s] %s%s" % (where, self.severity, self.code, path, self.message)

    def github_annotation(self):
        """GitHub Actions workflow command, shown inline on pull requests."""
        def escape(text, property_value=False):
            text = str(text).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
            return text.replace(":", "%3A").replace(",", "%2C") if property_value else text
        props = ["file=%s" % escape(self.file or "", True)]
        if self.line is not None:
            props += ["line=%d" % self.line, "col=%d" % self.column]
        props.append("title=%s" % escape("streamwright validate " + self.code, True))
        message = "%s: %s" % (self.path, self.message) if self.path else self.message
        return "::%s %s::%s" % (self.severity, ",".join(props), escape(message))

    def __repr__(self):
        return "Issue(%s)" % self
