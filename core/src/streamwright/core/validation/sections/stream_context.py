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

"""What the section checks know about a source's streams."""

import re

from streamwright.core.validation import vocabulary as spec

NAME = re.compile(spec.NAME_PATTERN)


def lower_name(value):
    return value.lower() if isinstance(value, str) else value


def transform_steps(stream):
    """The list of a stream's `transform.steps`, or None when there is none to read."""
    transform = stream.get("transform")
    steps = transform.get("steps") if isinstance(transform, dict) else None
    return steps if isinstance(steps, list) else None


class StreamContext:
    """What a stream makes available to references and cross-checks."""

    def __init__(self, name):
        self.name = name
        self.partitions = []
        self.incremental = False
        self.request_kind = None
        self.tables = {}          # lowercase request or step name -> (kind, index, name): the stream's tables
        self.steps_known = True   # False without steps to read (reported already): references to steps are not checked
