"""Secret masking shared by connector calls, HTTP and logging."""

import json
import threading
from urllib.parse import quote, quote_plus


class Redactor:
    """Replaces secret values and their encoded forms in diagnostic text."""

    def __init__(self, values=()):
        self.values = []
        self._lock = threading.Lock()
        for value in values:
            self.add(value)

    def add(self, value):
        if value is None or isinstance(value, bool):
            return
        text = str(value)
        forms = {form for form in (text, text.strip(), quote(text, safe=""), quote_plus(text),
                                      repr(text)[1:-1], json.dumps(text)[1:-1]) if len(form) >= 4}
        with self._lock:
            if forms - set(self.values):
                self.values = sorted(set(self.values) | forms, key=lambda form: (-len(form), form))

    def __call__(self, text):
        text = str(text)
        for value in self.values:
            text = text.replace(value, "***")
        return text
