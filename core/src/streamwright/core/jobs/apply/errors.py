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
"""The error of apply outputs (no imports: the runner's outbox raises it through)."""

__all__ = ["ApplyError", "RowsRejectedError"]


class ApplyError(Exception):
    """Rows could not all be applied: rejected rows under on_row_error: fail, an unknown outcome held, a bad call."""


class RowsRejectedError(ApplyError):
    """Rows of batch `batch_id` were rejected under on_row_error: fail, so the batch fails (a retry resends them)."""

    def __init__(self, message, batch_id):
        super().__init__(message)
        self.batch_id = batch_id
