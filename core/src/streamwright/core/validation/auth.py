"""Authentication section of source configuration validation."""

from streamwright.core.validation import vocabulary as spec
from streamwright.core.validation.sections import SectionChecker


class AuthChecker(SectionChecker):
    """Checks auth while reporting through the shared source issue sink."""

    def check(self, data):
        if "auth" not in data:
            return
        auth = data["auth"]
        path = ("auth",)
        if not self._expect(isinstance(auth, dict), "a mapping", auth, path, container=data, key="auth"):
            return
        self.sink.auth_type = auth.get("type")
        if "provider" in auth:
            provider = auth["provider"]
            self.check_name(provider, (*path, "provider"), auth, "provider")
            if isinstance(provider, str):
                self.sink.auth_provider = provider
                self.check_connector(provider, (*path, "provider"), auth, "provider")
                metadata = self.options.connector_metadata(provider)
                self.sink.auth_transport = metadata["transport"] if metadata is not None else None
                if self.sink.auth_transport == "http" and "type" not in auth:
                    self.error("missing-key", path, "`auth` for http connector %r needs `type` (one of: %s)" %
                               (provider, ", ".join(spec.AUTH_TYPES)), data, "auth")
                elif self.sink.auth_transport == "sdk" and "type" in auth:
                    self.error("bad-value", path, "connector %r is an sdk connector; do not set `type`" % provider,
                               data, "auth")
        elif "type" not in auth:
            self.error("missing-key", path, "`auth` needs `type` (one of: %s) or a connector `provider`" %
                       ", ".join(spec.AUTH_TYPES), data, "auth")

        if "type" in auth and self.check_choice(auth["type"], spec.AUTH_TYPES, (*path, "type"),
                                                 container=auth, key="type", what="auth type"):
            keys = spec.AUTH_TYPES[auth["type"]]
            self.require_keys(auth, keys["required"], path, "`auth: %s`" % auth["type"])
            allowed = ("type",) + keys["required"] + keys["optional"]
            if "provider" in auth:
                allowed = ("provider", *allowed)
            self.unknown_keys(auth, allowed, path, "error", "`auth: %s` does not support it" % auth["type"])
            self.check_strings(auth, keys["required"], path, "text or a reference")
            if "in" in auth:
                self.check_choice(auth["in"], spec.API_KEY_LOCATIONS, (*path, "in"), container=auth, key="in",
                                  what="location")
            if "scopes" in auth:
                self._expect(isinstance(auth["scopes"], list), "a list", auth["scopes"], (*path, "scopes"),
                             container=auth, key="scopes")
        self.check_templates(auth, path, container=data, key="auth", scopes=dict(self.scopes(),
                                                                                 secrets=set(self.inputs["secrets"])))
        for key, value in auth.items():
            lowered = str(key).lower()
            secret_like = key == "value" or (any(hint in lowered for hint in spec.SECRET_KEY_HINTS)
                                             and not lowered.endswith("url"))
            if secret_like and isinstance(value, str) and "{{" not in value:
                self.warn("literal-secret", (*path, key), "`%s` is written in the file; reference a secret "
                          "instead, e.g. \"{{ secrets.%s }}\"" % (key, key), auth, key)
