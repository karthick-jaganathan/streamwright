"""Authentication and HTTP client construction for source runs."""

from streamwright.core.config.inputs import parse_duration
from streamwright.core.net.http import Authenticator, HttpClient, RateLimiter
from streamwright.core.runtime import components
from streamwright.core.runtime.components import ComponentLoadError
from streamwright.core.runtime.source_connectors import select_transport
from streamwright.core.runtime.templates import render


class AuthFactory:
    @staticmethod
    def build(source, scopes, secrets, *, session, redact, allowed, error_type):
        """(provider, its ConnectorDefinition, the rendered auth of an sdk connector, the HTTP authenticator)."""
        auth = source.get("auth") or {}
        provider = auth.get("provider")
        connector = connector_auth = authenticator = None
        if provider:
            try:
                connector = components.load(provider, allowed)
            except ComponentLoadError as exc:
                raise error_type(str(exc)) from exc
            rendered = render({key: value for key, value in auth.items() if key != "provider"},
                              dict(scopes, secrets=secrets))
            if select_transport(connector) == "http":
                authenticator = Authenticator(rendered, session, redact)
            else:
                connector_auth = rendered
        else:
            authenticator = Authenticator(render(auth, dict(scopes, secrets=secrets)), session, redact)
        return provider, connector, connector_auth, authenticator

    @staticmethod
    def render_http(source, scopes, secrets):
        source_http = dict(source.get("http") or {})
        headers = source_http.pop("headers", None)
        http = render(source_http, scopes)
        if headers is not None:
            values = render(headers, dict(scopes, secrets=secrets))
            http["headers"] = {key: value for key, value in values.items() if value not in ("", None)} \
                if isinstance(values, dict) else values
        return http


class ClientFactory:
    def __init__(self, context, session, authenticator, http):
        self.context = context
        self.session = session
        self.authenticator = authenticator
        self.http = http
        self.shared_limiter = self.limiter(http.get("rate_limit"))

    def limiter(self, limit):
        if not limit:
            return None
        return RateLimiter(limit["requests"], parse_duration(limit["per"]).total_seconds(),
                           self.context.clock, self.context.sleep)

    def stream_limiter(self, stream):
        return self.limiter(stream["rate_limit"]) if stream.get("rate_limit") else self.shared_limiter

    def retry_policy(self, stream):
        return stream.get("retry") or self.http.get("retry")

    def stream_timeout(self, stream):
        """A stream's per-call `timeout` in seconds (number or duration), else the HTTP default."""
        value = stream.get("timeout")
        if value is None:
            return None
        return float(value) if isinstance(value, (int, float)) else parse_duration(value).total_seconds()

    def client(self, stream, limiter=None):
        return HttpClient(base_url=self.http.get("base_url"), headers=self.http.get("headers"),
                          authenticator=self.authenticator, retry=self.retry_policy(stream),
                          rate_limiter=limiter if limiter is not None else self.stream_limiter(stream),
                          redact=self.context.redact, session=self.session, sleep=self.context.sleep,
                          metrics=self.context.metrics, clock=self.context.clock, timeout=self.stream_timeout(stream))

    def download(self, stream):
        return HttpClient(retry=self.retry_policy(stream), redact=self.context.redact, session=self.session,
                          sleep=self.context.sleep, metrics=self.context.metrics, clock=self.context.clock,
                          timeout=self.stream_timeout(stream))
