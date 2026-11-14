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
The error of an HTTP request or of its paging (re-exported by streamwright.core.net.http, its public path), and the
excerpts of response bodies error messages show.
"""

from streamwright.core.runtime import logs

__all__ = ["HttpError", "error_excerpt"]

# the characters of a response body an error message shows
ERROR_BODY_CHARS = 300


def error_excerpt(text, redact, limit=ERROR_BODY_CHARS):
    """
    The start of a response body (or other text) for an error message: redacted (`redact`, then credentials masked
    by name) before it is cut to `limit` characters, so a secret that crosses the cut is never shown in part.
    """
    return logs.mask_text(redact(text))[:limit]


class HttpError(Exception):
    """An HTTP request that failed (`status`: its response's status, None without a response)."""

    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status
