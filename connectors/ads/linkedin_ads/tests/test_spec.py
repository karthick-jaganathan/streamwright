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

"""The linkedin_ads spec: certified, and the authentication it takes."""

from streamwright.connectors.linkedin_ads import CONNECTOR
from streamwright.core.runtime.source_connectors import select_transport
from streamwright.core.sdk.contract import check_connection
from streamwright.core.sdk.testing import certify, components

TestCertification = certify(CONNECTOR)


def codes(issues):
    return [(issue.code, issue.path) for issue in issues]


def test_the_connector_declares_http_requests_run_by_the_engine():
    assert select_transport(components.load("linkedin_ads")) == "http"
    assert [operation.name for operation in CONNECTOR.operations] == ["http.request"]


def test_the_token_is_a_secret_reference_and_other_authentication_is_refused():
    assert check_connection(CONNECTOR, {"type": "bearer", "token": "{{ secrets.key }}"}) == []
    assert codes(check_connection(CONNECTOR, {"type": "bearer", "token": "written-in-the-file"})) == [
        ("literal-secret", "token")]
    assert codes(check_connection(CONNECTOR, {"type": "api_key", "name": "k", "value": "{{ secrets.v }}"})) == [
        ("bad-value", "type"), ("secret-placement", "value")]
