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
The apply session of declarative HTTP connectors (`http.send`, streamwright.core.runtime.operations.http): an apply
output whose `operation:` is a request sends its rows to the connection's API with the connection's auth, base URL and
headers. Each row is one request (its JSON body the row, or `json` rendered with `{{ row }}`), so a rejected row fails
alone; a request cut short raises PartialApplyError, keeping the outcomes of the rows sent before it. When `json`
holds `{{ rows }}`, a chunk is one request and every row gets its outcome. The output's context applies the rate limit
and retries (a POST or PATCH is sent once).
"""

from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any, Final

from streamwright.core.jobs.refs import references, walk_strings
from streamwright.core.net.auth import Authenticator
from streamwright.core.net.encoding import encode_params
from streamwright.core.net.errors import HttpError
from streamwright.core.net.http import HttpClient
from streamwright.core.runtime.api_calls import make_caller
from streamwright.core.runtime.apply import ACTION_COLUMN, PartialApplyError, RowResult
from streamwright.core.runtime.connector_types import ConnectorError
from streamwright.core.runtime.templates import TemplateError, get_path, render

__all__ = ["HttpApplySession", "sends_chunk"]

_ROWS: Final = "rows"
# retries are the output's (its context), not the client's
_ONE_ATTEMPT: Final = MappingProxyType({"max_attempts": 1})
_TRANSIENT_FROM: Final = 500


def sends_chunk(request: Mapping[str, Any]) -> bool:
    """Whether a request sends a chunk of rows as one request: its `json` refers to `{{ rows }}`."""
    return any(expression == _ROWS for text in walk_strings(request.get("json")) for expression in references(text))


def _translate(error: Exception) -> ConnectorError | None:
    """An HTTP failure as a ConnectorError coded by its status (none: no response); 429 and 5xx are transient."""
    if not isinstance(error, HttpError):
        return None
    status = error.status
    transient = getattr(error, "retryable", status is None or status == 429 or status >= _TRANSIENT_FROM)
    return ConnectorError(str(error), code=status, retryable=bool(transient))


def _payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key != ACTION_COLUMN}


def _result(response: Any, records: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    """What a row's result keeps of a response: its `records.path` part, as a mapping."""
    if records and records.get("path") and isinstance(response, Mapping):
        try:
            response = get_path(response, records["path"])
        except KeyError:
            return None
    if response is None or isinstance(response, Mapping):
        return response
    return {"response": response}


class HttpApplySession:
    """Sends an apply output's rows through one HTTP client (the connection's auth, base URL and headers)."""

    def __init__(self, auth: Mapping[str, Any], http: Mapping[str, Any], redact: Any):
        self.client = HttpClient(http.get("base_url"), http.get("headers"), retry=dict(_ONE_ATTEMPT), redact=redact)
        self.client.authenticator = Authenticator(dict(auth), self.client.session, redact)
        self.params_encoding = http.get("params_encoding") or "plain"

    def apply(self, operation: Any, arguments: Mapping[str, Any], rows: Sequence[Mapping[str, Any]],
              context: Any) -> list[RowResult]:
        call = make_caller(context, _translate)
        if sends_chunk(arguments):
            return self._chunk(arguments, rows, call)
        results: list[RowResult] = []
        for row in rows:
            try:
                request = self._rendered(arguments, {"row": _payload(row)})
            except TemplateError as exc:
                results.append(RowResult.failed("INVALID_ROW", str(exc)))
                continue
            try:
                response = call(self._send, request, _payload(row))
            except ConnectorError as exc:
                if exc.retryable or context.retryable(exc):
                    raise PartialApplyError(str(exc), results, code=exc.code) from None
                results.append(RowResult.failed(None if exc.code is None else str(exc.code), str(exc)))
                continue
            results.append(RowResult.applied(_result(response, arguments.get("records"))))
        return results

    def _chunk(self, arguments: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], call: Any) -> list[RowResult]:
        """A chunk as one request: every row applied (with its item of `records.path`, when it has one per row)."""
        payloads = [_payload(row) for row in rows]
        try:
            request = self._rendered(arguments, {_ROWS: payloads})
        except TemplateError as exc:
            raise ConnectorError(str(exc), code="INVALID_ROW") from None
        response = call(self._send, request, None)
        found = _result(response, arguments.get("records"))
        items = found.get("response") if isinstance(found, Mapping) and set(found) == {"response"} else None
        if isinstance(items, list) and len(items) == len(rows):
            return [RowResult.applied(item if isinstance(item, Mapping) else {"response": item}) for item in items]
        return [RowResult.applied(found) for _ in rows]

    def _rendered(self, arguments: Mapping[str, Any], scope: Mapping[str, Any]) -> dict[str, Any]:
        keys = ("path", "params", "headers", "json")
        rendered = {key: render(arguments[key], scope) for key in keys if arguments.get(key) is not None}
        rendered["method"] = str(arguments.get("method") or "POST").upper()
        return rendered

    def _send(self, request: Mapping[str, Any], row: Mapping[str, Any] | None) -> Any:
        body = request.get("json", row)
        params = encode_params(dict(request.get("params") or {}), self.params_encoding)
        return self.client.request(request["method"], str(request["path"]), params=params or None,
                                   headers=request.get("headers"), json_body=body)

    def close(self) -> None:
        self.client.session.close()
