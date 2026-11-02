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

"""Section: connections, as their adapters (or connectors) accept them."""

from streamwright.core.jobs.check.findings import connections_of

__all__ = ["check_connections"]


def check_connections(job, rules, findings):
    for name, conn in connections_of(job).items():
        for code, message, is_warning in rules.check_connection(name, conn):
            findings.add(code, ("connections", name), message, is_warning)
