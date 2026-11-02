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

"""The checker's sections, in the order they run."""

from streamwright.core.jobs.check.connections import check_connections
from streamwright.core.jobs.check.dependencies import check_dependencies
from streamwright.core.jobs.check.findings import Findings
from streamwright.core.jobs.check.names import check_names
from streamwright.core.jobs.check.outputs import check_outputs
from streamwright.core.jobs.check.refs import check_references
from streamwright.core.jobs.check.runtime import check_engine_rules, check_runtime
from streamwright.core.jobs.check.secrets import check_secrets
from streamwright.core.jobs.check.spec import check_spec
from streamwright.core.jobs.check.steps import check_steps
from streamwright.core.jobs.check.structure import check_structure

__all__ = ["SECTIONS", "Findings"]

SECTIONS = (check_structure, check_names, check_spec, check_secrets, check_references, check_connections,
            check_steps, check_outputs, check_runtime, check_engine_rules, check_dependencies)
