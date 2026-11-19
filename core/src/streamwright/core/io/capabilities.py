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
What each core engine supports, combined once from the io families' declarations: `SOURCE_CAPABILITIES` (streamwright
run) and `DUCKDB_CAPABILITIES` (kind: DuckDBJob). The engines' connector bindings, certification and describe use these.
"""

from typing import Final

from streamwright.core.io import database_rules, file_rules, message_rules
from streamwright.core.runtime.contract import combine_capabilities

__all__ = ["DUCKDB_CAPABILITIES", "ENGINE_CAPABILITIES", "SOURCE_CAPABILITIES"]

SOURCE_CAPABILITIES: Final = combine_capabilities(file_rules.FILE_SOURCE_CAPABILITIES,
                                                  database_rules.DATABASE_SOURCE_CAPABILITIES,
                                                  message_rules.MESSAGE_SOURCE_CAPABILITIES)
DUCKDB_CAPABILITIES: Final = combine_capabilities(file_rules.FILE_DUCKDB_CAPABILITIES,
                                                  database_rules.DATABASE_DUCKDB_CAPABILITIES,
                                                  message_rules.MESSAGE_DUCKDB_CAPABILITIES)
ENGINE_CAPABILITIES: Final = (SOURCE_CAPABILITIES, DUCKDB_CAPABILITIES)
