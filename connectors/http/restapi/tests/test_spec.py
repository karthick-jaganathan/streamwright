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

"""The restapi spec: certified, and the built-in authentication it takes."""

from streamwright.connectors.restapi import CONNECTOR
from streamwright.core.runtime.source_connectors import select_transport
from streamwright.core.sdk.contract import check_connection
from streamwright.core.sdk.testing import certify, components

TestCertification = certify(CONNECTOR)


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_the_connector_declares_http_requests_and_sends_run_by_the_engine():
    assert select_transport(components.load("restapi")) == "http"
    assert [operation.name for operation in CONNECTOR.operations] == ["http.request", "http.send"]


def test_every_credential_of_the_built_in_authentication_is_a_secret_reference():
    assert sorted(CONNECTOR.describe()["secret_keys"]) == [
        "client_secret", "password", "refresh_token", "token", "value"]
    written = {"basic": {"type": "basic", "username": "u", "password": "written-in-the-file"},
               "api_key": {"type": "api_key", "name": "X-Key", "value": "written-in-the-file"}}
    assert codes(check_connection(CONNECTOR, written["basic"])) == [("literal-secret", "password")]
    assert codes(check_connection(CONNECTOR, written["api_key"])) == [("literal-secret", "value")]
    assert check_connection(CONNECTOR, {"type": "api_key", "name": "X-Key", "value": "{{ secrets.key }}",
                                        "in": "query"}) == []


def test_an_unknown_authentication_type_is_refused():
    assert codes(check_connection(CONNECTOR, {"type": "digest"})) == [("bad-value", "type")]
