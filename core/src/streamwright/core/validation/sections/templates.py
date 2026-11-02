#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2025 Karthick Jaganathan
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

"""The `{{ reference | filter }}` template syntax of source documents."""

import re

TEMPLATE = re.compile(r"\{\{(.*?)\}\}", re.S)
REFERENCE_HEAD = re.compile(r"^([A-Za-z_]\w*)(?:\.([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*))?$")


def template_expressions(value):
    """The inside of each {{ reference }} in a value (recursively)."""
    if isinstance(value, dict):
        return [inner for item in value.values() for inner in template_expressions(item)]
    if isinstance(value, list):
        return [inner for item in value for inner in template_expressions(item)]
    return TEMPLATE.findall(value) if isinstance(value, str) else []
