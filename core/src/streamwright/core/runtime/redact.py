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
