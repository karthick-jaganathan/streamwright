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
Apply operations (connector contract, `OperationKind.APPLY`): the session protocol (ApplySession,
ReconcilingSession) and per-row outcome (RowResult) of a connector whose `source` adapter applies rows to a
vendor's objects. Declared in streamwright.core.runtime.apply, which documents the protocol.
"""

from streamwright.core.runtime.apply import (
    ACTION_COLUMN,
    APPLIED,
    APPLY_CAPABILITIES,
    FAILED,
    ApplySession,
    PartialApplyError,
    ReconcilingSession,
    RowResult,
)

__all__ = ["ACTION_COLUMN", "APPLIED", "APPLY_CAPABILITIES", "FAILED", "ApplySession", "PartialApplyError",
           "ReconcilingSession", "RowResult"]
