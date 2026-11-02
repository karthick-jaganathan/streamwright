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
