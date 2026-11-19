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
Text helpers shared by the validators, the runtime, the outputs and connectors: did-you-mean hints (suggest; connectors
reach it as `streamwright.core.sdk.suggest`) and splitting template expressions outside quotes.
"""

import difflib

__all__ = ["split_outside_quotes", "suggest"]


def suggest(word, candidates, *, style="inline"):
    """
    A did-you-mean hint for an unknown `word` among `candidates` (their strings), or "" when none is close. `style`:
    "inline" - the closest, quoted, to follow the word: " (did you mean 'name'?)"; "list" - up to three, unquoted, as
    a clause: "; did you mean a, b?".
    """
    if style not in ("inline", "list"):
        raise ValueError("style is inline or list, not %r" % (style,))
    if not isinstance(word, str):
        return ""
    names = [candidate for candidate in candidates if isinstance(candidate, str)]
    if style == "list":
        matches = difflib.get_close_matches(word, names, n=3)
        return "; did you mean %s?" % ", ".join(matches) if matches else ""
    match = difflib.get_close_matches(word, names, n=1, cutoff=0.6)
    return " (did you mean %r?)" % match[0] if match else ""


def split_outside_quotes(text, separator):
    """Splits on `separator` outside single or double quotes; parts are stripped. Shared by checks and runtime."""
    parts, current, quote = [], "", None
    for char in text:
        if quote:
            quote = None if char == quote else quote
        elif char in "'\"":
            quote = char
        elif char == separator:
            parts.append(current.strip())
            current = ""
            continue
        current += char
    parts.append(current.strip())
    return parts
