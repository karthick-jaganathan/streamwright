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
`GET /docs`: Swagger UI over the caller's `/openapi.json`, served by the service itself so "Try it out" calls the
same origin. The page holds no data: it asks for the caller's API key or token (kept in the browser tab's session
storage), then loads the API document and makes its calls with it. Swagger UI comes from a pinned CDN release,
locked by Subresource Integrity, and the page's Content-Security-Policy allows only that origin, its own inline
script (by nonce) and calls to the service.
"""

import secrets

from starlette.responses import HTMLResponse

__all__ = ["SWAGGER_UI", "docs_page"]

SWAGGER_UI = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14"
_SCRIPT_SRI = "sha384-wmyclcVGX/WhUkdkATwhaK1X1JtiNrr2EoYJ+diV3vj4v6OC5yCeSu+yW13SYJep"
_STYLE_SRI = "sha384-wxLW6kwyHktdDGr6Pv1zgm/VGJh99lfUbzSn6HNHBENZlCN7W602k9VkGdxuFvPn"
_CDN = "https://cdn.jsdelivr.net"
_POLICY = ("default-src 'none'; script-src 'nonce-%s' " + _CDN + "; style-src 'unsafe-inline' " + _CDN + "; "
           "img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")

_PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>StreamWright API</title>
<link rel="stylesheet" href="%(ui)s/swagger-ui.css" integrity="%(style_sri)s" crossorigin="anonymous">
<style>#key{padding:10px 20px;font-family:sans-serif;border-bottom:1px solid #ddd}#key input{width:28em}</style>
</head><body>
<div id="key"%(key_hidden)s><label>API key or token <input id="token" type="password" autocomplete="off"></label>
<button id="use">Use</button> <button id="forget">Forget</button></div>
<div id="ui"><p id="ask"%(ask_hidden)s style="padding:20px;font-family:sans-serif">Enter your API key or token
above to see the jobs you may call.</p></div>
<script src="%(ui)s/swagger-ui-bundle.js" integrity="%(script_sri)s" crossorigin="anonymous"></script>
<script nonce="%(nonce)s">
(function () {
  var NAME = "streamwright-token", token = sessionStorage.getItem(NAME) || "";
  document.getElementById("use").onclick = function () {
    sessionStorage.setItem(NAME, document.getElementById("token").value.trim()); location.reload();
  };
  document.getElementById("forget").onclick = function () { sessionStorage.removeItem(NAME); location.reload(); };
  if (!token && !document.getElementById("ask").hidden) { return; }
  var ui = SwaggerUIBundle({
    url: "/openapi.json", dom_id: "#ui", validatorUrl: null, persistAuthorization: false,
    tryItOutEnabled: true, displayRequestDuration: true,
    requestInterceptor: function (request) {
      if (token && !request.headers.Authorization) { request.headers.Authorization = "Bearer " + token; }
      return request;
    },
    onComplete: function () { if (token) { ui.preauthorizeApiKey("bearer", token); } }
  });
})();
</script>
</body></html>
"""


def docs_page(authenticated):
    """The page (a starlette HTMLResponse); `authenticated`: the service has an `auth` section (the key field)."""
    nonce = secrets.token_urlsafe(16)
    body = _PAGE % {"ui": SWAGGER_UI, "style_sri": _STYLE_SRI, "script_sri": _SCRIPT_SRI, "nonce": nonce,
                    "key_hidden": "" if authenticated else " hidden", "ask_hidden": "" if authenticated else " hidden"}
    return HTMLResponse(body, headers={"Content-Security-Policy": _POLICY % nonce, "Cache-Control": "no-store",
                                       "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer"})
