"""HTTP and SDK request sections of source validation."""

from streamwright.core.engine import rules
from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.sections import SectionChecker


class HttpChecker(SectionChecker):
    def check_request(self, http, path, container, scopes, info):
        http = self.check_mapping(http, spec.HTTP_REQUEST_KEYS, path, container=container, key="http",
                                  what="an http request", required=("path",))
        if http is None:
            return
        if self.auth_provider is not None and self.auth_type is None:
            self.error("bad-value", path, "http requests need a built-in `auth.type`; auth provider %r "
                       "authenticates sdk requests only" % self.auth_provider, container, "http")
        target = http.get("path")
        if "path" in http and self._expect(isinstance(target, str), "a path", target, (*path, "path"), container=http,
                                           key="path") \
                and not target.startswith(("http://", "https://")) and self.base_url is None:
            self.error("missing-key", (*path, "path"), "relative path %r needs `http.base_url` at the top "
                       "level" % target, http, "path")
        if "method" in http:
            self.check_choice(http["method"], spec.HTTP_METHODS, (*path, "method"), container=http, key="method",
                              what="HTTP method")
        if "headers" in http:
            self.check_headers(http["headers"], (*path, "headers"), http)
            headers_scopes = dict(scopes, secrets=set(self.inputs["secrets"]))
            self.check_templates(http["headers"], (*path, "headers"), container=http, key="headers",
                                 scopes=headers_scopes, stream=info)
        if "params" in http:
            self._expect(isinstance(http["params"], dict), "a mapping", http["params"],
                         (*path, "params"), container=http, key="params")
        if "params_encoding" in http:
            self.check_choice(http["params_encoding"], spec.PARAMS_ENCODING_TYPES,
                              (*path, "params_encoding"), container=http, key="params_encoding", what="encoding")
        encoding = http.get("params_encoding") or self.params_encoding
        if encoding == "plain" and isinstance(http.get("params"), dict):
            for key, value in http["params"].items():
                if isinstance(value, (dict, list)):
                    self.error("bad-value", (*path, "params", key),
                               "nested params need `params_encoding: dotted`", http, "params")
                    break
        paginator = container.get("paginator") if isinstance(container, dict) else None
        if paginator is None:
            paginator = self.http_paginator
        if isinstance(paginator, dict) and paginator.get("in") == "body" and not isinstance(http.get("json"), dict):
            self.error("bad-value", path, "`in: body` paginator requires `json` to be a mapping", container, "http")
        body = {key: value for key, value in http.items() if key != "headers"}
        self.check_templates(body, path, container=container, key="http", scopes=scopes, stream=info)


class RequestChecker(SectionChecker):
    def check_sdk(self, request, path, scopes, info, extra_allowed=()):
        self.unknown_keys(request, spec.SDK_REQUEST_KEYS + tuple(extra_allowed), path, "error",
                          "sdk requests do not support it")
        self.require_keys(request, ("method",), path, "an sdk request")
        problem = rules.transport_problem("sdk", self.auth_transport, self.auth_provider)
        if problem:
            self.error("bad-value", path, problem, request, "sdk" if "sdk" in request else "method")
        sdk = request.get("sdk")
        if self._expect(isinstance(sdk, str), "a connector name", sdk, (*path, "sdk"), container=request, key="sdk"):
            self.check_connector(sdk, (*path, "sdk"), request, "sdk")
            if self.auth_provider is None:
                self.error("bad-value", (*path, "sdk"), "sdk requests use the client built by a connector "
                           "`auth.provider`; set `auth: {provider: %s, ...}`" % sdk, request, "sdk")
            elif sdk != self.auth_provider:
                self.error("bad-value", (*path, "sdk"), "sdk %r does not match auth.provider %r" % (
                    sdk, self.auth_provider), request, "sdk")
        for name in ("service", "method"):
            if name in request:
                self._expect(isinstance(request[name], str), "a name", request[name], (*path, name), container=request,
                             key=name)
        if "arguments" in request:
            self._expect(isinstance(request["arguments"], dict), "a mapping", request["arguments"],
                         (*path, "arguments"), container=request, key="arguments")
        body = {key: request[key] for key in spec.SDK_REQUEST_KEYS if key in request}
        if id(request) in self.nodes:
            self.nodes[id(body)] = self.nodes[id(request)]
        self.check_templates(body, path, container=None, key=None, scopes=scopes, stream=info)
