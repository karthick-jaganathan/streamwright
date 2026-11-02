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

"""The JSON schema of a job kind, generated from the same rule tables its checker enforces (SEM-04, PNP-03)."""

from streamwright.core.jobs.fields import json_schema_of

__all__ = ["DRAFT_7", "json_schema"]

DRAFT_7 = "http://json-schema.org/draft-07/schema#"


def json_schema(rules):
    """The draft 7 JSON schema of `rules.kind` (connection parts come from the installed adapters in `rules`)."""
    schema = {"$schema": DRAFT_7, "title": "StreamWright %s" % rules.kind,
              "description": "Generated from the %s rule tables; `streamwright validate` checks more (references, "
                             "names, write rules, cycles)." % rules.kind}
    schema.update(json_schema_of(rules.manifest_field()))
    return schema
