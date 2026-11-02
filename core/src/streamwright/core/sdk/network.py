"""
HTTP for connectors with an SDK transport that call a REST API themselves: auth, rate limit, retries and network
logging.
"""

from streamwright.core.net.http import HttpClient, HttpError
from streamwright.core.runtime.logs import http_details

__all__ = ["HttpClient", "HttpError", "http_details"]
